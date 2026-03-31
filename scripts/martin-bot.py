#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
BITGET 马丁策略机器人（最终版）
--------------------------------
策略逻辑：
1. 趋势判断：
   - 上涨趋势 -> 反向做空
   - 下跌趋势 -> 反向做多
   - 横盘 -> 等待

2. 首仓：
   - 做空：挂在最近阻力位附近
   - 做多：挂在最近支撑位附近

3. 加仓：
   - 不提前一次性算死 5 层
   - 每次准备补“下一层”时，重新计算最新支撑/阻力
   - 优先使用结构位
   - 没有合适结构位时，使用 ATR 距离
   - 再不行，用固定百分比偏移兜底

4. 止盈：
   - 浮盈 >= 5%：平 30%
   - 浮盈 >= 8%：再平 20%
   - 剩余仓位使用 ATR trailing + 回撤保护

5. 止损：
   - 收益率低于 -max_loss_pct 时全平

注意：
- 默认 sandbox=True，请实盘前改 config
- 本策略属于逆势均值回归 + 马丁，实盘风险很高
"""

import json
import time
import argparse
import traceback
import math
import sys
import os
import atexit
import threading
import contextlib
import io
import site
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, Tuple, List

ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = ROOT / 'data' / 'logs'
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
DEPS_DIR = ROOT / '.deps-local'
LEGACY_DEPS_DIR = ROOT / '.deps'
USER_SITE = Path(site.getusersitepackages())
for extra_path in (DEPS_DIR, LEGACY_DEPS_DIR, USER_SITE):
    if extra_path.exists() and str(extra_path) not in sys.path:
        sys.path.insert(0, str(extra_path))

from trading.exchanges import create_exchange_adapter
from trading.runtime_config import load_runtime_config

import pandas as pd


if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


_LOG_FILE_HANDLE = None
_ORIGINAL_STDOUT = sys.stdout
_ORIGINAL_STDERR = sys.stderr


class TeeStream:
    def __init__(self, primary, log_handle):
        self.primary = primary
        self.log_handle = log_handle
        self._martin_is_tee = True

    def write(self, data):
        if not isinstance(data, str):
            data = str(data)
        written = self.primary.write(data)
        self.primary.flush()
        self.log_handle.write(data)
        self.log_handle.flush()
        return written

    def flush(self):
        self.primary.flush()
        self.log_handle.flush()

    def isatty(self):
        return getattr(self.primary, "isatty", lambda: False)()

    @property
    def encoding(self):
        return getattr(self.primary, "encoding", "utf-8")


def _close_runtime_log():
    global _LOG_FILE_HANDLE
    if _LOG_FILE_HANDLE is not None:
        try:
            sys.stdout = _ORIGINAL_STDOUT
            sys.stderr = _ORIGINAL_STDERR
            _LOG_FILE_HANDLE.flush()
            _LOG_FILE_HANDLE.close()
        except Exception:
            pass
        _LOG_FILE_HANDLE = None


def configure_runtime_logging() -> Path:
    global _LOG_FILE_HANDLE

    if getattr(sys.stdout, "_martin_is_tee", False):
        return Path(os.environ.get("MARTIN_LOG_FILE", LOG_DIR / "martin.log"))

    log_file_value = os.environ.get("MARTIN_LOG_FILE")
    if log_file_value:
        log_file = Path(log_file_value)
    else:
        log_file = LOG_DIR / f"martin-{time.strftime('%Y%m%d-%H%M%S')}.log"

    log_file.parent.mkdir(parents=True, exist_ok=True)
    _LOG_FILE_HANDLE = open(log_file, "a", encoding="utf-8", buffering=1)
    sys.stdout = TeeStream(sys.stdout, _LOG_FILE_HANDLE)
    sys.stderr = TeeStream(sys.stderr, _LOG_FILE_HANDLE)
    atexit.register(_close_runtime_log)
    print(f"📝 运行日志已写入: {log_file}")
    return log_file


@dataclass
class RuntimeState:
    layer: int = 0
    pending_layer: int = 0
    phase: str = "PHASE1"
    last_phase: str = "PHASE1"
    phase2_start_layer: int = 0
    pending_entry_price: float = 0.0
    pending_entry_amount: float = 0.0
    last_fill_price: float = 0.0
    last_fill_time: str = ""
    protective_stop_active: bool = False
    protective_stop_order_id: str = ""
    protective_stop_client_oid: str = ""
    protective_stop_price: float = 0.0
    best_profit_pct: float = 0.0
    position_side: Optional[str] = None     # long / short
    last_known_contracts: float = 0.0
    bot_state: str = "IDLE"                 # IDLE / IN_STRATEGY
    partial_tp_1_done: bool = False
    partial_tp_2_done: bool = False
    activated: bool = False
    entry_price: float = 0.0
    last_update: str = ""


class MartinBot:
    def __init__(self, config_path=None):
        if config_path is None:
            config_path = ROOT / 'config' / 'config.json'

        self.config = load_runtime_config(config_path, default={})
        self.runtime_file = ROOT / 'data' / 'martin-runtime.json'
        self.live_snapshot_file = ROOT / 'data' / 'martin-live.json'

        # =========================
        # 策略参数
        # =========================
        self.symbol = self.config.get('symbol', 'ETH/USDT:USDT')
        self.leverage = self.config.get('leverage', 3)
        self.timeframe = self.config.get('timeframe', '5m')
        self.sr_timeframe = self.config.get('sr_timeframe', '1h')
        self.trend_lookback = self.config.get('trend_lookback', 120)

        self.fast_ema_period = self.config.get('fast_ema_period', 20)
        self.slow_ema_period = self.config.get('slow_ema_period', 50)
        self.rsi_period = self.config.get('rsi_period', 14)
        self.adx_period = self.config.get('adx_period', 14)
        self.atr_period = self.config.get('atr_period', 14)

        self.rsi_threshold = self.config.get('rsi_threshold', 50)
        self.adx_threshold = self.config.get('adx_threshold', 16.5)
        self.trend_follow_enabled = bool(self.config.get('trend_follow_enabled', True))
        self.trend_follow_adx_threshold = self._safe_float(
            self.config.get('trend_follow_adx_threshold', max(self.adx_threshold + 8.0, 28.0)),
            max(self.adx_threshold + 8.0, 28.0),
        )
        self.mean_reversion_adx_max = self._safe_float(
            self.config.get('mean_reversion_adx_max', max(self.adx_threshold, 20.0)),
            max(self.adx_threshold, 20.0),
        )
        self.mean_reversion_long_rsi = self._safe_float(
            self.config.get('mean_reversion_long_rsi', 35.0),
            35.0,
        )
        self.mean_reversion_short_rsi = self._safe_float(
            self.config.get('mean_reversion_short_rsi', 65.0),
            65.0,
        )
        self.mean_reversion_entry_atr_ratio = self._safe_float(
            self.config.get('mean_reversion_entry_atr_ratio', 0.35),
            0.35,
        )
        self.strong_trend_entry_enabled = bool(self.config.get('strong_trend_entry_enabled', True))
        self.strong_trend_entry_adx = self._safe_float(
            self.config.get(
                'strong_trend_entry_adx',
                max(self.trend_follow_adx_threshold + 4.0, self.adx_threshold + 10.0),
            ),
            max(self.trend_follow_adx_threshold + 4.0, self.adx_threshold + 10.0),
        )
        self.strong_trend_entry_mode = str(
            self.config.get('strong_trend_entry_mode', 'marketable_limit')
        ).lower()
        self.strong_trend_limit_offset_pct = max(
            self._safe_float(self.config.get('strong_trend_limit_offset_pct', 0.0003), 0.0003),
            0.0,
        )
        self.strong_trend_pullback_atr_ratio = max(
            self._safe_float(self.config.get('strong_trend_pullback_atr_ratio', 0.15), 0.15),
            0.0,
        )
        self.strong_trend_max_pullback_pct = max(
            self._safe_float(self.config.get('strong_trend_max_pullback_pct', 0.0015), 0.0015),
            0.0,
        )
        self.keep_pending_entry_on_stretched = bool(
            self.config.get('keep_pending_entry_on_stretched', True)
        )

        self.phase_switch_loss_pct = self._safe_float(self.config.get('phase_switch_loss_pct', 0.025), 0.025)
        self.phase_switch_layer = max(int(self.config.get('phase_switch_layer', 4)), 2)
        self.phase1_max_layers = max(int(self.config.get('phase1_max_layers', 3)), 1)
        self.phase1_first_order_ratio = self._safe_float(self.config.get('phase1_first_order_ratio', 0.025), 0.025)
        self.phase1_layer_multipliers = list(self.config.get('phase1_layer_multipliers', [1, 1.15, 1.35]))
        self.phase1_layer_min_gap_pct = self._safe_float(self.config.get('phase1_layer_min_gap_pct', 0.0), 0.0)
        self.phase1_layer_min_gap_atr_multiplier = self._safe_float(
            self.config.get('phase1_layer_min_gap_atr_multiplier', 0.35),
            0.35,
        )
        self.phase1_layer_trigger_base_pct = self._safe_float(self.config.get('phase1_layer_trigger_base_pct', 0.0), 0.0)
        self.phase1_layer_trigger_atr_multiplier = self._safe_float(
            self.config.get('phase1_layer_trigger_atr_multiplier', 0.35),
            0.35,
        )
        self.phase2_extra_layers = max(int(self.config.get('phase2_extra_layers', self.config.get('phase2_max_layers', 6))), 1)
        self.phase2_layer_multipliers = list(self.config.get('phase2_layer_multipliers', [1.7, 2.1, 2.6, 3.2, 3.9, 4.8]))
        self.phase2_layer_min_gap_pct = self._safe_float(self.config.get('phase2_layer_min_gap_pct', 0.010), 0.010)
        self.phase2_layer_min_gap_atr_multiplier = self._safe_float(
            self.config.get('phase2_layer_min_gap_atr_multiplier', 1.4),
            1.4,
        )
        self.phase2_layer_trigger_base_pct = self._safe_float(self.config.get('phase2_layer_trigger_base_pct', 0.012), 0.012)
        self.phase2_layer_trigger_atr_multiplier = self._safe_float(
            self.config.get('phase2_layer_trigger_atr_multiplier', 1.5),
            1.5,
        )
        if len(self.phase1_layer_multipliers) < self.phase1_max_layers:
            raise ValueError("phase1_layer_multipliers 长度不能小于 phase1_max_layers")
        if len(self.phase2_layer_multipliers) < self.phase2_extra_layers:
            raise ValueError("phase2_layer_multipliers 长度不能小于 phase2_extra_layers")

        self.max_layers = self.phase1_max_layers + self.phase2_extra_layers
        self.layer_multipliers = self.phase1_layer_multipliers + self.phase2_layer_multipliers
        self.first_order_ratio = self.phase1_first_order_ratio

        self.level_offset_pct = self.config.get('level_offset_pct', 0.001)              # 结构位偏移 0.1%
        self.add_layer_base_offset_pct = self.config.get('add_layer_base_offset_pct', 0.005)  # 固定兜底偏移 0.5% * 层数
        self.structure_refresh_threshold = self.config.get('structure_refresh_threshold', 0.008)  # 结构变化阈值 0.8%

        self.structure_min_gap_pct = self._safe_float(self.config.get('structure_min_gap_pct', 0.005), 0.005)
        self.structure_min_gap_atr_multiplier = self._safe_float(
            self.config.get('structure_min_gap_atr_multiplier', 0.8),
            0.8,
        )
        self.layer_min_gap_pct = self._safe_float(self.config.get('layer_min_gap_pct', self.phase2_layer_min_gap_pct), self.phase2_layer_min_gap_pct)
        self.layer_min_gap_atr_multiplier = self._safe_float(
            self.config.get('layer_min_gap_atr_multiplier', self.phase2_layer_min_gap_atr_multiplier),
            self.phase2_layer_min_gap_atr_multiplier,
        )
        self.layer_trigger_base_pct = self._safe_float(
            self.config.get('layer_trigger_base_pct', self.phase2_layer_trigger_base_pct),
            self.phase2_layer_trigger_base_pct,
        )
        self.layer_trigger_atr_multiplier = self._safe_float(
            self.config.get('layer_trigger_atr_multiplier', self.phase2_layer_trigger_atr_multiplier),
            self.phase2_layer_trigger_atr_multiplier,
        )
        self.layer_trigger_type = str(self.config.get('layer_trigger_type', 'mark_price')).lower()
        self.use_ema_structure = bool(self.config.get('use_ema_structure', False))

        self.fee_rate = self.config.get('fee_rate', 0.0005)         # taker 0.05%
        self.max_loss_pct = self.config.get('max_loss_pct', 0.50)   # -50%
        default_protective_floor = max(2 * self.fee_rate * max(float(self.leverage), 1.0) + 0.002, 0.005)
        self.protective_stop_enabled = bool(self.config.get('protective_stop_enabled', True))
        self.protective_stop_trigger_type = str(
            self.config.get('protective_stop_trigger_type', 'mark_price')
        ).lower()
        self.protective_stop_execute_price = self._safe_float(
            self.config.get('protective_stop_execute_price', 0.0),
            0.0,
        )
        self.protective_stop_profit_lock_ratio = self._safe_float(
            self.config.get('protective_stop_profit_lock_ratio', 0.25),
            0.25,
        )
        self.protective_stop_min_profit_pct = self._safe_float(
            self.config.get('protective_stop_min_profit_pct', default_protective_floor),
            default_protective_floor,
        )
        self.protective_stop_update_step_pct = self._safe_float(
            self.config.get('protective_stop_update_step_pct', 0.003),
            0.003,
        )
        self.trailing_min_close_profit_pct = self._safe_float(
            self.config.get('trailing_min_close_profit_pct', self.protective_stop_min_profit_pct),
            self.protective_stop_min_profit_pct,
        )

        self.loop_interval = self.config.get('loop_interval', 20)
        self.error_sleep = self.config.get('error_sleep', 60)
        self.min_balance = self.config.get('min_balance', 10.0)
        self.order_margin_safety_ratio = self.config.get('order_margin_safety_ratio', 0.95)
        self.entry_amount_refresh_tolerance = max(
            self._safe_float(self.config.get('entry_amount_refresh_tolerance', 0.01), 0.01),
            0.0,
        )
        self.insufficient_balance_shrink_ratio = self.config.get('insufficient_balance_shrink_ratio', 0.90)
        self.ws_risk_monitor_enabled = bool(
            self.config.get('ws_risk_monitor_enabled', self.config.get('wsEnabled', True))
        )
        self.ws_risk_check_interval = max(float(self.config.get('ws_risk_check_interval', 0.25)), 0.10)
        self.ws_risk_context_refresh_sec = max(float(self.config.get('ws_risk_context_refresh_sec', 5.0)), 1.0)
        self.ws_risk_log_step_pct = max(float(self.config.get('ws_risk_log_step_pct', 0.25)), 0.05) / 100
        self.local_snapshot_enabled = bool(self.config.get('local_snapshot_enabled', True))
        self.local_snapshot_interval = max(float(self.config.get('local_snapshot_interval', 1.0)), 0.25)
        self.local_snapshot_market_interval = max(
            float(self.config.get('local_snapshot_market_interval', 5.0)),
            self.local_snapshot_interval,
        )
        self.local_snapshot_trade_interval = max(float(self.config.get('local_snapshot_trade_interval', 6.0)), 1.0)
        self.local_snapshot_ledger_interval = max(float(self.config.get('local_snapshot_ledger_interval', 30.0)), 5.0)

        self.state_lock = threading.RLock()
        self.action_lock = threading.RLock()
        self.snapshot_lock = threading.Lock()
        self._exit_in_progress = threading.Event()
        self._risk_stop_event = threading.Event()
        self._risk_thread: Optional[threading.Thread] = None
        self._risk_context: Optional[Dict[str, Any]] = None
        self._risk_context_at = 0.0
        self._risk_context_key: Optional[Tuple[Any, ...]] = None
        self._last_risk_log_profit_pct = 0.0
        self._last_risk_rest_position_at = 0.0
        self._last_live_snapshot_at = 0.0
        self._last_live_market_at = 0.0
        self._last_live_trades_at = 0.0
        self._last_live_ledger_at = 0.0
        self._live_snapshot_cache: Dict[str, Any] = {}

        self.exchange = self._init_exchange()
        self.markets = None

        self.state = RuntimeState()
        self._load_runtime_state()
        self._last_risk_log_profit_pct = self.state.best_profit_pct

    # =========================================================
    # 基础工具
    # =========================================================
    def _load_json(self, path, default=None):
        path = Path(path)
        if path.exists():
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {} if default is None else default

    def _save_json(self, path, data):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = path.with_suffix(path.suffix + f'.{os.getpid()}.tmp')
        with open(temp_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(temp_path, path)

    def _now_str(self):
        return time.strftime("%Y-%m-%dT%H:%M:%S")

    def _format_timestamp_ms(self, timestamp_ms: Any) -> Optional[str]:
        if not timestamp_ms:
            return None
        try:
            return datetime.fromtimestamp(float(timestamp_ms) / 1000).isoformat(timespec="seconds")
        except (TypeError, ValueError, OSError):
            return None

    @contextlib.contextmanager
    def _silence_output(self):
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer), contextlib.redirect_stderr(buffer):
            yield

    def _quiet_call(self, fn, *args, **kwargs):
        with self._silence_output():
            return fn(*args, **kwargs)

    def _init_exchange(self):
        return create_exchange_adapter(self.config)

    def _ensure_markets(self):
        if self.markets is None:
            self.markets = self.exchange.load_markets()

    def _bootstrap_exchange(self):
        while True:
            try:
                self.setup_account()
                self._ensure_markets()
                self.sync_state_with_exchange()
                return
            except KeyboardInterrupt:
                raise
            except Exception as e:
                print(f"⚠️ 启动阶段连接交易所失败: {e}")
                print(f"⏳ {self.error_sleep} 秒后自动重试，不退出策略")
                time.sleep(self.error_sleep)

    def _market(self):
        self._ensure_markets()
        return self.exchange.market(self.symbol)

    def _safe_float(self, value, default=0.0) -> float:
        try:
            if value is None:
                return default
            return float(value)
        except Exception:
            return default

    def _amount_to_precision(self, amount: float) -> float:
        try:
            return float(self.exchange.amount_to_precision(self.symbol, amount))
        except Exception:
            return amount

    def _price_to_precision(self, price: float) -> float:
        try:
            return float(self.exchange.price_to_precision(self.symbol, price))
        except Exception:
            return price

    def _min_amount(self) -> float:
        market = self._market()
        return float(market.get('limits', {}).get('amount', {}).get('min') or 0)

    def _normalize_amount(self, amount: float) -> float:
        amount = self._amount_to_precision(amount)
        min_amount = self._min_amount()
        if min_amount > 0 and amount < min_amount:
            return 0.0
        return amount

    def _position_profit_pct(self, position: Dict[str, Any], current_price: Optional[float] = None) -> float:
        info = position.get('info') or {}
        entry_price = self._safe_float(position.get('entryPrice', info.get('openPriceAvg', 0)))
        live_price = self._safe_float(
            current_price,
            self._safe_float(position.get('markPrice', info.get('markPrice', 0))),
        )
        if entry_price > 0 and live_price > 0:
            side = str(position.get('side') or info.get('holdSide') or '').lower()
            leverage = self._safe_float(position.get('leverage', info.get('leverage', self.leverage)), self.leverage)
            if leverage <= 0:
                leverage = max(self.leverage, 1)
            direction = -1.0 if side in {'short', 'sell'} else 1.0
            return direction * ((live_price - entry_price) / entry_price) * leverage

        raw_percentage = position.get('percentage')
        if raw_percentage not in (None, ""):
            normalized = self._safe_float(raw_percentage, 0.0)
            if abs(normalized) > 1e-9:
                return normalized / 100

        unrealized = self._safe_float(position.get('unrealizedPnl', info.get('unrealizedPL', 0)))
        margin_size = self._safe_float(position.get('marginSize', info.get('marginSize', 0)))
        if margin_size > 0:
            return unrealized / margin_size

        mark_price = self._safe_float(position.get('markPrice', info.get('markPrice', 0)))
        if entry_price > 0 and mark_price > 0:
            side = str(position.get('side') or info.get('holdSide') or '').lower()
            leverage = self._safe_float(position.get('leverage', info.get('leverage', self.leverage)), self.leverage)
            if leverage <= 0:
                leverage = max(self.leverage, 1)
            direction = -1.0 if side in {'short', 'sell'} else 1.0
            return direction * ((mark_price - entry_price) / entry_price) * leverage

        return 0.0

    def _update_best_profit(self, current_profit_pct: float, source: str = "轮询", log_step_pct: Optional[float] = None) -> bool:
        should_log = False
        with self.state_lock:
            if current_profit_pct <= self.state.best_profit_pct + 1e-9:
                return False
            self.state.best_profit_pct = current_profit_pct
            if log_step_pct is None:
                should_log = True
                self._last_risk_log_profit_pct = current_profit_pct
            elif (
                self._last_risk_log_profit_pct <= 0
                or current_profit_pct >= self._last_risk_log_profit_pct + log_step_pct
            ):
                should_log = True
                self._last_risk_log_profit_pct = current_profit_pct
        self._save_runtime_state()
        if should_log:
            print(f"📈 [{source}] 最高浮盈更新: {current_profit_pct*100:.2f}%")
        return True

    def _set_runtime_flag(self, field_name: str, value: Any) -> bool:
        changed = False
        with self.state_lock:
            if getattr(self.state, field_name) != value:
                setattr(self.state, field_name, value)
                changed = True
        if changed:
            self._save_runtime_state()
        return changed

    def _position_side(self, position: Optional[Dict[str, Any]] = None) -> str:
        info = (position or {}).get('info') or {}
        side = str(
            (position or {}).get('side')
            or info.get('holdSide')
            or self.state.position_side
            or 'long'
        ).lower()
        return 'short' if side in {'short', 'sell'} else 'long'

    def _position_hold_side_for_plan(self, position: Optional[Dict[str, Any]] = None) -> str:
        info = (position or {}).get('info') or {}
        raw_side = str(
            (position or {}).get('side')
            or info.get('holdSide')
            or self.state.position_side
            or 'long'
        ).lower()
        pos_mode = str(info.get('posMode') or '').lower()
        if pos_mode == 'one_way_mode':
            return 'sell' if raw_side in {'short', 'sell'} else 'buy'
        if raw_side in {'buy', 'sell', 'long', 'short'}:
            return raw_side
        return 'short' if raw_side in {'short', 'sell'} else 'long'

    def _position_entry_price(self, position: Optional[Dict[str, Any]] = None) -> float:
        info = (position or {}).get('info') or {}
        return self._safe_float(
            (position or {}).get('entryPrice'),
            self._safe_float(info.get('openPriceAvg', 0), self._safe_float(self.state.entry_price, 0)),
        )

    def _protective_stop_target(
        self,
        position: Dict[str, Any],
        current_profit_pct: Optional[float] = None,
        best_profit_pct: Optional[float] = None,
    ) -> Optional[Dict[str, float]]:
        entry_price = self._position_entry_price(position)
        if entry_price <= 0:
            return None

        side = self._position_side(position)
        leverage = self._safe_float(
            position.get('leverage', ((position.get('info') or {}).get('leverage', self.leverage))),
            self.leverage,
        )
        if leverage <= 0:
            leverage = max(self.leverage, 1)

        best_profit = max(
            self._safe_float(best_profit_pct, self.state.best_profit_pct),
            0.0,
        )
        if best_profit <= 0:
            return None

        locked_profit_pct = max(
            self.protective_stop_min_profit_pct,
            best_profit * max(self.protective_stop_profit_lock_ratio, 0.0),
        )
        if current_profit_pct is not None:
            lockable_profit_pct = max(self._safe_float(current_profit_pct, 0.0) - 0.001, 0.0)
            if lockable_profit_pct < self.protective_stop_min_profit_pct:
                return None
            locked_profit_pct = min(locked_profit_pct, lockable_profit_pct)

        raw_move_ratio = locked_profit_pct / max(leverage, 1.0)
        if side == 'short':
            trigger_price = entry_price * (1 - raw_move_ratio)
        else:
            trigger_price = entry_price * (1 + raw_move_ratio)

        return {
            'entry_price': entry_price,
            'locked_profit_pct': locked_profit_pct,
            'trigger_price': trigger_price,
            'side': side,
            'hold_side': self._position_hold_side_for_plan(position),
            'leverage': leverage,
        }

    def _should_update_protective_stop(self, side: str, trigger_price: float) -> bool:
        current_price = self._safe_float(self.state.protective_stop_price, 0.0)
        if not self.state.protective_stop_active or current_price <= 0:
            return True

        min_step = max(self.protective_stop_update_step_pct, 0.0)
        if side == 'short':
            return trigger_price <= current_price * (1 - min_step)
        return trigger_price >= current_price * (1 + min_step)

    def _clear_protective_stop(self, remote: bool = True, force_all: bool = False) -> bool:
        remote_changed = False
        order_id = self.state.protective_stop_order_id
        client_oid = self.state.protective_stop_client_oid

        if remote and self.protective_stop_enabled and (force_all or order_id or client_oid or self.state.protective_stop_active):
            try:
                self.exchange.cancel_position_stop_loss(
                    self.symbol,
                    order_id=order_id or None,
                    client_oid=client_oid or None,
                )
                remote_changed = True
            except Exception as exc:
                print(f"⚠️ 取消保护止损失败: {exc}")

        changed = False
        with self.state_lock:
            if self.state.protective_stop_active:
                self.state.protective_stop_active = False
                changed = True
            if self.state.protective_stop_order_id:
                self.state.protective_stop_order_id = ""
                changed = True
            if self.state.protective_stop_client_oid:
                self.state.protective_stop_client_oid = ""
                changed = True
            if abs(self.state.protective_stop_price) > 1e-9:
                self.state.protective_stop_price = 0.0
                changed = True
        if changed:
            self._save_runtime_state()
        return changed or remote_changed

    def _arm_protective_stop(
        self,
        position: Dict[str, Any],
        current_profit_pct: Optional[float],
        reason: str,
        best_profit_pct: Optional[float] = None,
        force: bool = False,
    ) -> bool:
        if not self.protective_stop_enabled or self._exit_in_progress.is_set():
            return False

        target = self._protective_stop_target(position, current_profit_pct=current_profit_pct, best_profit_pct=best_profit_pct)
        if not target:
            return False
        if not force and not self._should_update_protective_stop(str(target['side']), target['trigger_price']):
            return False

        client_oid = self.state.protective_stop_client_oid or f"martin-pos-loss-{int(time.time() * 1000)}"
        order_id = self.state.protective_stop_order_id or None

        try:
            with self.action_lock:
                if self._exit_in_progress.is_set():
                    return False

                if order_id or self.state.protective_stop_client_oid:
                    try:
                        response = self.exchange.modify_tpsl_order(
                            self.symbol,
                            trigger_price=target['trigger_price'],
                            trigger_type=self.protective_stop_trigger_type,
                            execute_price=self.protective_stop_execute_price,
                            order_id=order_id,
                            client_oid=self.state.protective_stop_client_oid or None,
                            size="",
                        )
                    except Exception:
                        self.exchange.cancel_position_stop_loss(
                            self.symbol,
                            order_id=order_id,
                            client_oid=self.state.protective_stop_client_oid or None,
                        )
                        response = self.exchange.place_position_stop_loss(
                            self.symbol,
                            hold_side=str(target['hold_side']),
                            trigger_price=target['trigger_price'],
                            trigger_type=self.protective_stop_trigger_type,
                            execute_price=self.protective_stop_execute_price,
                            client_oid=client_oid,
                        )
                else:
                    response = self.exchange.place_position_stop_loss(
                        self.symbol,
                        hold_side=str(target['hold_side']),
                        trigger_price=target['trigger_price'],
                        trigger_type=self.protective_stop_trigger_type,
                        execute_price=self.protective_stop_execute_price,
                        client_oid=client_oid,
                    )
        except Exception as exc:
            print(f"⚠️ 挂保护止损失败: {exc}")
            return False

        response = response or {}
        new_order_id = str(response.get('id') or order_id or "")
        new_client_oid = str(response.get('clientOrderId') or client_oid or "")

        with self.state_lock:
            self.state.protective_stop_active = True
            self.state.protective_stop_order_id = new_order_id
            self.state.protective_stop_client_oid = new_client_oid
            self.state.protective_stop_price = self._safe_float(target['trigger_price'], 0.0)
        self._save_runtime_state()
        self._write_live_snapshot(force=True, include_market=False)
        print(
            f"🛡️ {reason}: 已挂保护止损 {target['side']} "
            f"@ {target['trigger_price']:.2f}，锁定收益下限 {target['locked_profit_pct']*100:.2f}%"
        )
        return True

    def _allow_trailing_close(
        self,
        position: Dict[str, Any],
        current_profit_pct: float,
        reason: str,
        best_profit_pct: Optional[float] = None,
    ) -> bool:
        profit_floor = max(self.trailing_min_close_profit_pct, self.protective_stop_min_profit_pct)
        if current_profit_pct >= profit_floor:
            return True

        self._arm_protective_stop(
            position,
            current_profit_pct=current_profit_pct,
            best_profit_pct=best_profit_pct,
            reason=f"{reason}，改挂保护止损",
            force=False,
        )
        print(
            f"🛡️ {reason}: 当前收益 {current_profit_pct*100:.2f}% "
            f"低于保底平仓线 {profit_floor*100:.2f}%，跳过亏损/低利润追踪平仓"
        )
        return False

    def _phase_config(self, phase: Optional[str] = None) -> Dict[str, Any]:
        normalized = str(phase or self.state.phase or 'PHASE1').upper()
        if normalized == 'PHASE2':
            return {
                'phase': 'PHASE2',
                'max_layers': self.max_layers,
                'first_order_ratio': self.phase1_first_order_ratio,
                'layer_multipliers': self.phase2_layer_multipliers,
                'layer_index_offset': self.phase1_max_layers,
                'layer_min_gap_pct': self.phase2_layer_min_gap_pct,
                'layer_min_gap_atr_multiplier': self.phase2_layer_min_gap_atr_multiplier,
                'layer_trigger_base_pct': self.phase2_layer_trigger_base_pct,
                'layer_trigger_atr_multiplier': self.phase2_layer_trigger_atr_multiplier,
            }
        return {
            'phase': 'PHASE1',
            'max_layers': self.phase1_max_layers,
            'first_order_ratio': self.phase1_first_order_ratio,
            'layer_multipliers': self.phase1_layer_multipliers,
            'layer_index_offset': 0,
            'layer_min_gap_pct': self.phase1_layer_min_gap_pct,
            'layer_min_gap_atr_multiplier': self.phase1_layer_min_gap_atr_multiplier,
            'layer_trigger_base_pct': self.phase1_layer_trigger_base_pct,
            'layer_trigger_atr_multiplier': self.phase1_layer_trigger_atr_multiplier,
        }

    def _infer_phase2_start_layer(self, layer: Optional[int] = None) -> int:
        layer_num = int(self.state.layer if layer is None else layer)
        if layer_num < self.phase_switch_layer:
            return max(layer_num, 1)
        return self.phase1_max_layers + 1

    def _repair_phase2_start_layer(self) -> bool:
        if str(self.state.phase or 'PHASE1').upper() != 'PHASE2':
            return False
        current = int(getattr(self.state, 'phase2_start_layer', 0) or 0)
        if current > 0:
            return False

        inferred = self._infer_phase2_start_layer()
        if inferred <= 0:
            return False

        self.state.phase2_start_layer = inferred
        print(
            f"🔧 修复历史 PHASE2 状态: layer={self.state.layer}, "
            f"推断 phase2_start_layer={inferred}"
        )
        return True

    def _resolve_phase_layer_index(self, phase_cfg: Dict[str, Any], layer_num: int) -> int:
        layer_multipliers = list(phase_cfg.get('layer_multipliers') or [])
        phase = str(phase_cfg.get('phase', 'PHASE1')).upper()
        offset = int(phase_cfg.get('layer_index_offset', 0))
        phase2_start_layer = 0
        force_fallback = False

        if phase == 'PHASE2':
            phase2_start_layer = int(getattr(self.state, 'phase2_start_layer', 0) or 0)
            if phase2_start_layer <= 0:
                phase2_start_layer = self._infer_phase2_start_layer()
            # PHASE2 允许因亏损提前切换。只要实际起点不是正常路径的起点，
            # 就优先按实际起始层号计算阶段内索引，避免 primary_index 在覆盖区抢先命中。
            force_fallback = (
                phase2_start_layer > 0 and
                phase2_start_layer != offset + 1
            )

        primary_index = layer_num - offset - 1
        if not force_fallback and 0 <= primary_index < len(layer_multipliers):
            return primary_index

        if phase == 'PHASE2':
            # PHASE2 允许因亏损提前切换，此时需要基于 PHASE2 实际起始层号
            # 计算阶段内索引，保证倍率在提前切换路径下也保持递增。
            if phase2_start_layer > 0:
                fallback_index = layer_num - phase2_start_layer
            else:
                fallback_index = layer_num - offset - 1
            if 0 <= fallback_index < len(layer_multipliers):
                return fallback_index

        return -1

    def _should_switch_to_phase2(
        self,
        position: Optional[Dict[str, Any]] = None,
        current_price: Optional[float] = None,
        current_profit_pct: Optional[float] = None,
    ) -> bool:
        if str(self.state.phase or 'PHASE1').upper() == 'PHASE2':
            return True
        if self.state.layer >= self.phase_switch_layer:
            return True
        if position is None:
            return False
        profit_pct = current_profit_pct
        if profit_pct is None:
            profit_pct = self._position_profit_pct(position, current_price)
        return profit_pct <= -abs(self.phase_switch_loss_pct)

    def _current_phase(
        self,
        position: Optional[Dict[str, Any]] = None,
        current_price: Optional[float] = None,
        current_profit_pct: Optional[float] = None,
    ) -> str:
        if self._should_switch_to_phase2(position, current_price=current_price, current_profit_pct=current_profit_pct):
            if str(self.state.phase or 'PHASE1').upper() != 'PHASE2':
                with self.state_lock:
                    self.state.phase = 'PHASE2'
                    self.state.phase2_start_layer = max(self.state.layer + 1, 1)
                self._save_runtime_state()
                print(
                    f"🧭 阶段切换: PHASE1 -> PHASE2 "
                    f"(layer={self.state.layer}, 阈值={self.phase_switch_layer}, "
                    f"亏损线={self.phase_switch_loss_pct*100:.2f}%)"
                )
            return 'PHASE2'
        if str(self.state.phase or 'PHASE1').upper() != 'PHASE1':
            with self.state_lock:
                self.state.phase = 'PHASE1'
                self.state.phase2_start_layer = 0
            self._save_runtime_state()
        return 'PHASE1'

    def _gap_ratio(
        self,
        current_price: float,
        atr_value: float,
        base_pct: Optional[float] = None,
        atr_multiplier: Optional[float] = None,
        depth_scale: float = 0.0,
        layer_num: int = 1,
        phase: Optional[str] = None,
        kind: str = 'spacing',
    ) -> float:
        if base_pct is None or atr_multiplier is None:
            phase_cfg = self._phase_config(phase)
            if kind == 'trigger':
                base_pct = phase_cfg['layer_trigger_base_pct']
                atr_multiplier = phase_cfg['layer_trigger_atr_multiplier']
            else:
                base_pct = phase_cfg['layer_min_gap_pct']
                atr_multiplier = phase_cfg['layer_min_gap_atr_multiplier']
        scale = 1.0 + max(layer_num - 2, 0) * depth_scale
        ratios = [max(base_pct, 0.0) * scale]
        if current_price > 0 and atr_value > 0 and atr_multiplier > 0:
            ratios.append((atr_value * atr_multiplier * scale) / current_price)
        return max(ratios)

    def _extract_latest_atr(self, timeframe: Optional[str] = None, limit: int = 80) -> Tuple[float, float]:
        df = self.fetch_ohlcv_df(timeframe=timeframe or self.timeframe, limit=max(limit, self.atr_period + 20))
        if df is None:
            return 0.0, 0.0
        df = self.add_indicators(df)
        row = df.iloc[-1]
        return self._safe_float(row.get('atr'), 0.0), self._safe_float(row.get('close'), 0.0)

    def _entry_bias_from_row(self, row) -> Tuple[str, str]:
        current_price = self._safe_float(row['close'])
        ema_fast = self._safe_float(row['ema_fast'])
        ema_slow = self._safe_float(row['ema_slow'])
        rsi = self._safe_float(row['rsi'])
        adx = self._safe_float(row['adx'])
        atr = self._safe_float(row['atr'], current_price * 0.01)
        atr_band = atr * self.mean_reversion_entry_atr_ratio

        is_bullish = ema_fast > ema_slow and rsi > self.rsi_threshold
        is_bearish = ema_fast < ema_slow and rsi < self.rsi_threshold

        if self.trend_follow_enabled and adx >= self.trend_follow_adx_threshold:
            if is_bullish:
                return "TREND_UP", "LONG"
            if is_bearish:
                return "TREND_DOWN", "SHORT"

        if adx <= self.mean_reversion_adx_max:
            if rsi <= self.mean_reversion_long_rsi and current_price <= (ema_fast - atr_band):
                return "RANGE_MEAN_REVERSION", "LONG"
            if rsi >= self.mean_reversion_short_rsi and current_price >= (ema_fast + atr_band):
                return "RANGE_MEAN_REVERSION", "SHORT"

        if is_bullish:
            return "BULLISH_BUT_STRETCHED", "WAIT"
        if is_bearish:
            return "BEARISH_BUT_STRETCHED", "WAIT"
        return "NEUTRAL", "WAIT"

    def _pending_entry_price_from_orders(self, orders: Optional[List[Dict[str, Any]]]) -> float:
        if not orders:
            return 0.0
        non_reduce_orders = [order for order in orders if not order.get('reduceOnly', False)]
        if not non_reduce_orders:
            return 0.0
        order = non_reduce_orders[0]
        price = self._safe_float(order.get('price', 0))
        return price if self._is_sane_pending_entry_price(price) else 0.0

    def _is_sane_pending_entry_price(self, price: float) -> bool:
        price = self._safe_float(price, 0.0)
        if price <= 0:
            return False
        references = [
            self._safe_float(self.state.pending_entry_price, 0.0),
            self._safe_float(self.state.last_fill_price, 0.0),
            self._safe_float(self.state.entry_price, 0.0),
        ]
        references = [value for value in references if value > 0]
        if not references:
            return True
        anchor = min(references) if str(self.state.position_side or '').lower() == 'long' else max(references)
        if anchor <= 0:
            return True
        ratio = price / anchor
        return 0.7 <= ratio <= 1.3

    def _layer_anchor_price(
        self,
        side: str,
        current_price: float,
        avg_price: float,
        pending_entry_price: Optional[float] = None,
    ) -> float:
        candidates = [
            self._safe_float(pending_entry_price, 0.0),
            self._safe_float(self.state.last_fill_price, 0.0),
            self._safe_float(avg_price, 0.0),
            self._safe_float(current_price, 0.0),
        ]
        candidates = [value for value in candidates if value > 0]
        if not candidates:
            return 0.0
        if side == 'long':
            return min(candidates)
        return max(candidates)

    def _enforce_layer_spacing(
        self,
        proposed_price: float,
        side: str,
        layer_num: int,
        current_price: float,
        avg_price: float,
        atr_value: float = 0.0,
        pending_entry_price: Optional[float] = None,
        phase: Optional[str] = None,
    ) -> float:
        spacing_ratio = self._gap_ratio(
            current_price=current_price,
            atr_value=atr_value,
            depth_scale=0.25,
            layer_num=layer_num,
            phase=phase,
            kind='spacing',
        )
        anchor_price = self._layer_anchor_price(
            side,
            current_price,
            avg_price,
            pending_entry_price=pending_entry_price,
        )
        if anchor_price <= 0:
            return proposed_price

        if side == 'long':
            return min(proposed_price, anchor_price * (1 - spacing_ratio))
        return max(proposed_price, anchor_price * (1 + spacing_ratio))

    def _next_layer_trigger_ready(
        self,
        position: Dict[str, Any],
        current_price: float,
        next_layer: int,
        pending_entry_price: Optional[float] = None,
        phase: Optional[str] = None,
    ) -> Tuple[bool, float, float]:
        avg_price = self._safe_float(position.get('entryPrice', self.state.entry_price or current_price), current_price)
        atr_value, _ = self._extract_latest_atr(limit=80)
        trigger_ratio = self._gap_ratio(
            current_price=current_price,
            atr_value=atr_value,
            depth_scale=0.50,
            layer_num=next_layer,
            phase=phase,
            kind='trigger',
        )
        anchor_price = self._layer_anchor_price(
            self.state.position_side or 'long',
            current_price,
            avg_price,
            pending_entry_price=pending_entry_price,
        )
        if anchor_price <= 0 or current_price <= 0:
            return False, 0.0, trigger_ratio

        if self.state.position_side == 'short':
            trigger_price = anchor_price * (1 + trigger_ratio)
            return current_price >= trigger_price, trigger_price, trigger_ratio

        trigger_price = anchor_price * (1 - trigger_ratio)
        return current_price <= trigger_price, trigger_price, trigger_ratio

    def _marketable_trigger_execute_price(self, side: str, entry_price: float, trigger_price: float) -> float:
        if entry_price <= 0:
            return trigger_price
        if trigger_price <= 0:
            return entry_price
        if side == 'short':
            return min(entry_price, trigger_price)
        return max(entry_price, trigger_price)

    def _build_add_order_plan(
        self,
        layer_num: int,
        current_price: float,
        position: Optional[Dict[str, Any]] = None,
        balance_snapshot: Optional[Dict[str, Any]] = None,
        anchor_pending_price: Optional[float] = None,
    ) -> Optional[Dict[str, Any]]:
        position = position or self.get_active_position()
        phase = self._current_phase(position, current_price=current_price)
        phase_cfg = self._phase_config(phase)
        if layer_num > phase_cfg['max_layers']:
            print(f"⚠️ 当前阶段 {phase} 最多执行到第{phase_cfg['max_layers']}层，跳过第{layer_num}层")
            return None

        if self._exit_in_progress.is_set():
            print("⚠️ 当前正在执行平仓流程，跳过本层加仓")
            return None

        balance_snapshot = balance_snapshot or self.get_balance_snapshot()
        if balance_snapshot is None:
            print("❌ 余额不足")
            return None
        equity = self._safe_float(balance_snapshot.get('equity', 0))
        if equity < self.min_balance:
            print("❌ 余额不足")
            return None

        if not self.state.position_side:
            print("⚠️ 未知持仓方向，无法加仓")
            return None

        avg_price = self._safe_float(position.get('entryPrice', current_price)) if position else current_price
        side = self.state.position_side

        structure_price, sr = self._select_structure_entry_price(
            side,
            layer_num,
            current_price,
            avg_price,
            pending_entry_price=anchor_pending_price,
            phase=phase,
        )
        if structure_price is not None:
            entry_price = structure_price
            source = "STRUCTURE"
        else:
            atr_price = self._select_atr_entry_price(side, layer_num, current_price, avg_price)
            if atr_price is not None:
                entry_price = atr_price
                source = "ATR"
            else:
                entry_price = self._select_fallback_entry_price(side, layer_num, current_price, avg_price)
                source = "FALLBACK"

        atr_value = self._safe_float((sr or {}).get('atr', 0.0), 0.0)
        if atr_value <= 0:
            atr_value, _ = self._extract_latest_atr(limit=80)
        if source != "STRUCTURE":
            entry_price = self._enforce_layer_spacing(
                proposed_price=entry_price,
                side=side,
                layer_num=layer_num,
                current_price=current_price,
                avg_price=avg_price,
                atr_value=atr_value,
                pending_entry_price=anchor_pending_price,
                phase=phase,
            )
        entry_price = self._price_to_precision(entry_price)

        layer_multipliers = phase_cfg['layer_multipliers']
        phase_layer_index = self._resolve_phase_layer_index(phase_cfg, layer_num)
        if phase_layer_index < 0 or phase_layer_index >= len(layer_multipliers):
            print(f"⚠️ 当前阶段 {phase} 未配置第{layer_num}层倍率，跳过加仓")
            return None
        desired_margin = equity * phase_cfg['first_order_ratio'] * layer_multipliers[phase_layer_index]
        layer_margin = self._calculate_order_margin(
            desired_margin,
            balance_snapshot,
            f"第{layer_num}层",
        )
        if layer_margin < self.min_balance:
            print("❌ 当前可新增保证金不足，跳过本层加仓")
            return None

        order_side = 'sell' if side == 'short' else 'buy'
        amount = (layer_margin * self.leverage) / entry_price
        amount = self._normalize_amount(amount)
        if amount <= 0:
            print("❌ 加仓数量低于交易所最小下单量")
            return None

        execute_price = self._price_to_precision(entry_price)
        return {
            'phase': phase,
            'layer_num': layer_num,
            'order_side': order_side,
            'amount': amount,
            'entry_price': entry_price,
            'execute_price': execute_price,
            'trigger_price': 0.0,
            'trigger_ratio': 0.0,
            'ready': False,
            'source': source,
            'avg_price': avg_price,
            'current_price': current_price,
            'layer_margin': layer_margin,
            'sr': sr,
            'position': position,
            'side': side,
            'phase_layer_index': phase_layer_index,
        }

    def _log_add_order_plan(self, plan: Dict[str, Any]) -> None:
        layer_num = int(plan['layer_num'])
        print(
            f"📋 [{plan['phase']}] 第{layer_num}层: {plan['order_side'].upper()} {plan['amount']} @ {plan['entry_price']} "
            f"(保证金 {plan['layer_margin']:.2f} USDT, 倍率 {self._phase_config(plan['phase'])['layer_multipliers'][plan['phase_layer_index']]})"
        )
        print(
            f"   来源: {plan['source']} | 持仓均价: {plan['avg_price']:.2f} "
            f"| 当前价: {plan['current_price']:.2f}"
        )
        sr = plan.get('sr')
        if sr:
            print(f"   最新阻力: {[f'{x:.2f}' for x in sr['resistance']]}")
            print(f"   最新支撑: {[f'{x:.2f}' for x in sr['support']]}")
            if sr.get('vp_levels'):
                print(f"   VP结构位: {[f'{x:.2f}' for x in sr['vp_levels']]}")
                print(f"   VP阻力: {[f'{x:.2f}' for x in sr.get('vp_resistance', [])]}")
                print(f"   VP支撑: {[f'{x:.2f}' for x in sr.get('vp_support', [])]}")
        print(f"   委托价: {plan['execute_price']:.2f}")

    def _amount_delta_ratio(self, left: float, right: float) -> float:
        left_value = abs(self._safe_float(left, 0.0))
        right_value = abs(self._safe_float(right, 0.0))
        return abs(left_value - right_value) / max(left_value, right_value, 1e-9)

    def _stabilize_plan_amount_with_existing_entry(
        self,
        plan: Dict[str, Any],
        orders: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        if len(orders) != 1:
            return plan
        existing_order = orders[0]
        if str(existing_order.get('side', '')).lower() != str(plan.get('order_side', '')).lower():
            return plan

        existing_amount = self._normalize_amount(self._safe_float(existing_order.get('amount', 0.0), 0.0))
        if existing_amount <= 0:
            return plan

        if self._amount_delta_ratio(existing_amount, plan.get('amount', 0.0)) > self.entry_amount_refresh_tolerance:
            return plan

        stabilized_plan = dict(plan)
        stabilized_plan['amount'] = existing_amount
        stabilized_plan['sticky_amount_reason'] = (
            f"沿用现有挂单数量 {existing_amount}，新候选数量 {plan['amount']} 偏差未超过 "
            f"{self.entry_amount_refresh_tolerance*100:.2f}%"
        )
        return stabilized_plan

    def _submit_add_order_plan(self, plan: Dict[str, Any]) -> bool:
        layer_num = int(plan['layer_num'])
        try:
            if plan['trigger_price'] <= 0:
                if not self._submit_entry_order(plan['order_side'], plan['amount'], plan['execute_price'], f"第{layer_num}层"):
                    return False
                print(f"✅ 第{layer_num}层加仓单已直接挂出")
            else:
                self.exchange.create_trigger_order(
                    self.symbol,
                    plan['order_side'],
                    plan['amount'],
                    plan['trigger_price'],
                    price=plan['execute_price'],
                    trigger_type=self.layer_trigger_type,
                    order_type='limit',
                )
                print(f"✅ 第{layer_num}层条件加仓单已挂出")
            self.state.phase = plan['phase']
            self.state.pending_layer = max(self.state.layer, min(layer_num, self.max_layers))
            self.state.pending_entry_price = plan['execute_price']
            self.state.pending_entry_amount = plan['amount']
            self._save_runtime_state()
            self._write_live_snapshot(force=True, include_market=True)
            return True
        except Exception as e:
            print(f"❌ 加仓失败: {e}")
            return False

    def _entry_orders_match_plan(self, orders: List[Dict[str, Any]], plan: Dict[str, Any]) -> bool:
        if len(orders) != 1:
            return False
        order = orders[0]
        if str(order.get('side', '')).lower() != str(plan['order_side']).lower():
            return False
        existing_amount = self._normalize_amount(self._safe_float(order.get('amount', 0.0), 0.0))
        if self._amount_delta_ratio(existing_amount, plan['amount']) > self.entry_amount_refresh_tolerance:
            return False

        existing_price = self._price_to_precision(self._safe_float(order.get('price', 0.0), 0.0))
        if existing_price != plan['execute_price']:
            return False

        if plan['trigger_price'] <= 0:
            return str(order.get('type', '')).lower() != 'trigger'

        if str(order.get('type', '')).lower() != 'trigger':
            return False
        existing_trigger = self._price_to_precision(self._safe_float(order.get('triggerPrice', 0.0), 0.0))
        return existing_trigger == plan['trigger_price']

    def _relative_price_delta(self, current_price: float, target_price: float) -> float:
        baseline = max(abs(current_price), abs(target_price), 1e-9)
        return abs(current_price - target_price) / baseline

    def _structure_side_distance(self, side: str, current_price: float, target_price: float) -> float:
        if current_price <= 0 or target_price <= 0:
            return float("inf")
        if side == 'long':
            return max(current_price - target_price, 0.0)
        return max(target_price - current_price, 0.0)

    def _is_structure_price_valid_for_side(self, side: str, current_price: float, avg_price: float, price: float) -> bool:
        if price <= 0 or current_price <= 0 or avg_price <= 0:
            return False
        if side == 'long':
            return price < current_price and price < avg_price
        return price > current_price and price > avg_price

    def _is_materially_better_structure_price(
        self,
        side: str,
        current_price: float,
        candidate_price: float,
        reference_price: float,
    ) -> bool:
        candidate_distance = self._structure_side_distance(side, current_price, candidate_price)
        reference_distance = self._structure_side_distance(side, current_price, reference_price)
        if not math.isfinite(candidate_distance) or not math.isfinite(reference_distance):
            return False
        if candidate_distance >= reference_distance:
            return False
        if reference_distance <= 1e-9:
            return True
        improvement_ratio = (reference_distance - candidate_distance) / reference_distance
        sticky_threshold = max(self._safe_float(self.structure_refresh_threshold, 0.0), 0.0)
        return improvement_ratio >= sticky_threshold

    def _select_sticky_structure_price(
        self,
        candidate_prices: List[float],
        pending_entry_price: Optional[float] = None,
    ) -> Optional[float]:
        anchor_price = self._safe_float(pending_entry_price, 0.0)
        if anchor_price <= 0 or not candidate_prices:
            return None
        nearest_price = min(candidate_prices, key=lambda price: abs(price - anchor_price))
        sticky_delta = self._relative_price_delta(nearest_price, anchor_price)
        sticky_threshold = max(
            self._safe_float(self.structure_refresh_threshold, 0.0),
            self._safe_float(self.level_offset_pct, 0.0) * 2.0,
            0.001,
        )
        if sticky_delta <= sticky_threshold:
            return nearest_price
        return None

    def _stabilize_plan_with_existing_entry(
        self,
        plan: Dict[str, Any],
        orders: List[Dict[str, Any]],
        current_price: float,
    ) -> Dict[str, Any]:
        if len(orders) != 1:
            return plan
        existing_order = orders[0]
        existing_price = self._price_to_precision(self._safe_float(existing_order.get('price', 0.0), 0.0))
        if existing_price <= 0:
            return plan
        if str(existing_order.get('side', '')).lower() != str(plan.get('order_side', '')).lower():
            return plan
        if self._normalize_amount(self._safe_float(existing_order.get('amount', 0.0), 0.0)) != plan.get('amount'):
            return plan
        if plan.get('trigger_price', 0.0) > 0:
            return plan
        side = str(plan.get('side') or self.state.position_side or '').lower()
        avg_price = self._safe_float(plan.get('avg_price', 0.0), 0.0)
        if side not in ('long', 'short'):
            return plan
        if not self._is_structure_price_valid_for_side(side, current_price, avg_price, existing_price):
            return plan
        if self._is_materially_better_structure_price(side, current_price, plan['execute_price'], existing_price):
            return plan

        stabilized_plan = dict(plan)
        stabilized_plan['entry_price'] = existing_price
        stabilized_plan['execute_price'] = existing_price
        stabilized_plan['sticky_existing_price'] = True
        stabilized_plan['sticky_reason'] = (
            f"沿用现有挂单价 {existing_price:.2f}，新候选价 {plan['execute_price']:.2f} 未明显更优"
        )
        return stabilized_plan

    def _entry_order_refresh_reason(self, orders: List[Dict[str, Any]], plan: Dict[str, Any]) -> str:
        if not orders:
            return "当前没有加仓挂单"
        if len(orders) != 1:
            return f"当前存在 {len(orders)} 个加仓挂单"

        order = orders[0]
        order_type = str(order.get('type', '')).lower()
        existing_side = str(order.get('side', '')).lower()
        if existing_side != str(plan['order_side']).lower():
            return f"方向不一致({existing_side} -> {plan['order_side']})"

        existing_amount = self._normalize_amount(self._safe_float(order.get('amount', 0.0), 0.0))
        if self._amount_delta_ratio(existing_amount, plan['amount']) > self.entry_amount_refresh_tolerance:
            return f"数量不一致({existing_amount} -> {plan['amount']})"

        existing_price = self._price_to_precision(self._safe_float(order.get('price', 0.0), 0.0))
        execute_delta = self._relative_price_delta(existing_price, plan['execute_price'])
        refresh_threshold = max(self._safe_float(self.structure_refresh_threshold, 0.0), 0.0)

        if plan['trigger_price'] <= 0:
            if order_type == 'trigger':
                return "当前挂单仍是条件单，需改为直接限价单"
            if execute_delta >= refresh_threshold:
                return (
                    f"委托价偏离过大({existing_price:.2f} -> {plan['execute_price']:.2f}, "
                    f"{execute_delta*100:.2f}%)"
                )
            return ""

        if order_type != 'trigger':
            return "当前挂单类型不是条件单"

        existing_trigger = self._price_to_precision(self._safe_float(order.get('triggerPrice', 0.0), 0.0))
        trigger_delta = self._relative_price_delta(existing_trigger, plan['trigger_price'])
        if trigger_delta >= refresh_threshold:
            return (
                f"触发价偏离过大({existing_trigger:.2f} -> {plan['trigger_price']:.2f}, "
                f"{trigger_delta*100:.2f}%)"
            )
        if execute_delta >= refresh_threshold:
            return (
                f"委托价偏离过大({existing_price:.2f} -> {plan['execute_price']:.2f}, "
                f"{execute_delta*100:.2f}%)"
            )
        return ""

    def _cancel_entry_orders(self, orders: List[Dict[str, Any]]) -> bool:
        targets = [order for order in orders if not order.get('reduceOnly', False)]
        if not targets:
            return True
        cancel_fn = getattr(self.exchange, 'cancel_orders', None)
        if not callable(cancel_fn):
            print("⚠️ 交易所适配器不支持定向撤单，跳过加仓单重建")
            return False
        with self.action_lock:
            try:
                cancel_fn(targets, self.symbol)
                print(f"✅ 已撤销 {len(targets)} 个旧加仓挂单")
                return True
            except Exception as e:
                print(f"⚠️ 定向撤销加仓挂单失败: {e}")
                return False

    def _reconcile_active_entry_orders(
        self,
        add_orders: List[Dict[str, Any]],
        next_layer: int,
        current_price: float,
        position: Dict[str, Any],
        reason_prefix: str = "",
    ) -> bool:
        anchor_pending_price = self._pending_entry_price_from_orders(add_orders)
        plan = self._build_add_order_plan(
            next_layer,
            current_price,
            position=position,
            anchor_pending_price=anchor_pending_price,
        )
        if plan is None:
            return False
        plan = self._stabilize_plan_amount_with_existing_entry(plan, add_orders)
        plan = self._stabilize_plan_with_existing_entry(plan, add_orders, current_price)
        if plan.get('sticky_amount_reason'):
            print(f"🧷 {reason_prefix}第{next_layer}层加仓数量保持不动: {plan['sticky_amount_reason']}")
        if plan.get('sticky_existing_price'):
            print(f"🧷 {reason_prefix}第{next_layer}层加仓挂单保持不动: {plan['sticky_reason']}")

        if self._entry_orders_match_plan(add_orders, plan):
            pending_price = self._pending_entry_price_from_orders(add_orders)
            if pending_price > 0 and abs(pending_price - self.state.pending_entry_price) > 1e-9:
                self._set_runtime_flag('pending_entry_price', pending_price)
            return True

        refresh_reason = self._entry_order_refresh_reason(add_orders, plan)
        if not refresh_reason:
            pending_price = self._pending_entry_price_from_orders(add_orders)
            if pending_price > 0 and abs(pending_price - self.state.pending_entry_price) > 1e-9:
                self._set_runtime_flag('pending_entry_price', pending_price)
            return True

        print(f"♻️ {reason_prefix}第{next_layer}层加仓挂单需要重建: {refresh_reason}")
        if not self._cancel_entry_orders(add_orders):
            return False
        return self._submit_add_order_plan(plan)

    def _reconcile_startup_entry_orders(self) -> None:
        position = self.get_active_position()
        if not position:
            return

        open_orders = self.fetch_open_orders()
        if open_orders is None:
            print("⚠️ 启动检查时无法获取挂单，跳过加仓单校验")
            return

        add_orders = [order for order in open_orders if not order.get('reduceOnly', False)]
        if not add_orders:
            return

        current_price = self._live_price_from_ws(position, allow_rest=True)
        if current_price <= 0:
            current_price = self._safe_float(position.get('markPrice', 0), self._safe_float(position.get('entryPrice', 0), 0.0))
        if current_price <= 0:
            print("⚠️ 启动检查时无法确定当前价格，跳过加仓单校验")
            return

        phase = self._current_phase(position, current_price=current_price)
        phase_cfg = self._phase_config(phase)
        next_layer = self.state.layer + 1
        if next_layer > phase_cfg['max_layers']:
            print(f"✅ 启动检查: 当前阶段 {phase} 无需保留第{next_layer}层加仓挂单")
            if add_orders:
                self._cancel_entry_orders(add_orders)
                self.sync_state_with_exchange()
            return

        if self._reconcile_active_entry_orders(
            add_orders,
            next_layer,
            current_price,
            position,
            reason_prefix=f"启动检查[{phase}] ",
        ):
            print(f"✅ 启动检查: 当前阶段 {phase} 的加仓挂单已完成校准")
        else:
            print("⚠️ 启动检查: 重挂加仓单失败，保留下一轮主循环补挂")
            return

        self.sync_state_with_exchange()

    def _dynamic_tp_values(self, adx: float, volatility_pct: float) -> Tuple[float, float]:
        leverage = max(float(self.leverage), 1.0)
        fee_floor = max(2 * self.fee_rate * leverage, 0.0)
        profit_floor = max(
            self.trailing_min_close_profit_pct,
            self.protective_stop_min_profit_pct,
            fee_floor + 0.001,
        )
        # Let the activation threshold follow current trading costs and protection settings
        # instead of pinning it to a fixed 3% floor.
        base_threshold = max(fee_floor * 3.0, profit_floor * 2.4)

        if adx > 30:
            adx_multiplier = 1.5
            trail_ratio = 0.4
        elif adx > 25:
            adx_multiplier = 1.2
            trail_ratio = 0.45
        elif adx > 20:
            adx_multiplier = 1.0
            trail_ratio = 0.5
        else:
            adx_multiplier = 0.7
            trail_ratio = 0.6

        if volatility_pct > 0.03:
            vol_multiplier = 1.3
        elif volatility_pct > 0.02:
            vol_multiplier = 1.1
        elif volatility_pct > 0.01:
            vol_multiplier = 1.0
        else:
            vol_multiplier = 0.8

        activate_pct = base_threshold * adx_multiplier * vol_multiplier
        activate_pct = max(activate_pct, max(profit_floor, fee_floor * 1.5))
        activate_pct = min(activate_pct, 0.15)
        return activate_pct, trail_ratio

    def _dynamic_partial_tp_targets(
        self,
        position: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, float]] = None,
        current_profit_pct: Optional[float] = None,
        best_profit_pct: Optional[float] = None,
    ) -> Dict[str, float]:
        side = self._position_side(position)
        layer = max(int(self.state.layer), 1)
        phase = self._current_phase(position, current_profit_pct=current_profit_pct)
        risk_context = context or self._build_risk_context(position_side=side, layer=layer) or {}

        adx = self._safe_float(risk_context.get('adx', 20.0), 20.0)
        volatility_pct = self._safe_float(risk_context.get('volatility_pct', 0.02), 0.02)

        if adx >= 30:
            trend_mult = 1.20
        elif adx >= 20:
            trend_mult = 1.00
        else:
            trend_mult = 0.85

        if volatility_pct > 0.03:
            vol_mult = 1.25
        elif volatility_pct > 0.015:
            vol_mult = 1.05
        else:
            vol_mult = 0.90

        if layer >= 4:
            layer_mult = 0.88
        elif layer == 3:
            layer_mult = 0.95
        else:
            layer_mult = 1.00

        phase_mult = 0.92 if phase == 'PHASE2' else 1.00
        total_mult = trend_mult * vol_mult * layer_mult * phase_mult

        leverage = max(float(self.leverage), 1.0)
        profit_floor = max(
            self.trailing_min_close_profit_pct,
            self.protective_stop_min_profit_pct,
            2 * self.fee_rate * leverage + 0.001,
        )

        tp1_threshold = max(profit_floor, 0.04 * total_mult)
        tp1_threshold = min(max(tp1_threshold, 0.015), 0.08)

        tp2_threshold = max(tp1_threshold + 0.015, 0.07 * total_mult)
        tp2_threshold = min(tp2_threshold, 0.14)

        return {
            'tp1_threshold': tp1_threshold,
            'tp2_threshold': tp2_threshold,
            'tp1_ratio': 0.30,
            'tp2_ratio': 0.20,
            'trend_mult': trend_mult,
            'vol_mult': vol_mult,
            'layer_mult': layer_mult,
            'phase_mult': phase_mult,
            'phase': phase,
            'adx': adx,
            'volatility_pct': volatility_pct,
            'best_profit_pct': self._safe_float(best_profit_pct, self.state.best_profit_pct),
        }

    def _build_risk_context(self, position_side: Optional[str] = None, layer: Optional[int] = None) -> Optional[Dict[str, float]]:
        df = self.fetch_ohlcv_df(limit=80)
        if df is None:
            return None

        df = self.add_indicators(df)
        row = df.iloc[-1]

        adx = self._safe_float(row['adx'], 20)
        current_price = self._safe_float(row['close'], 1)
        atr = self._safe_float(row['atr'], current_price * 0.02)
        volatility_pct = atr / current_price if current_price > 0 else 0.02
        activate_pct, trail_ratio = self._dynamic_tp_values(adx, volatility_pct)

        window = min(30, len(df))
        recent_high = self._safe_float(df['high'].tail(window).max(), current_price)
        recent_low = self._safe_float(df['low'].tail(window).min(), current_price)

        effective_side = (position_side or self.state.position_side or '').lower()
        effective_layer = int(layer if layer is not None else self.state.layer)
        atr_multiplier = 4.0 if effective_layer < 4 else 3.0

        if effective_side == 'short':
            trail_price = recent_low + atr_multiplier * atr
            trail_desc = f"做空ATR追踪: 最低{recent_low:.2f} + {atr_multiplier}*ATR({atr:.2f}) = {trail_price:.2f}"
        else:
            trail_price = recent_high - atr_multiplier * atr
            trail_desc = f"做多ATR追踪: 最高{recent_high:.2f} - {atr_multiplier}*ATR({atr:.2f}) = {trail_price:.2f}"

        return {
            'adx': adx,
            'atr': atr,
            'current_price': current_price,
            'volatility_pct': volatility_pct,
            'activate_pct': activate_pct,
            'trail_ratio': trail_ratio,
            'recent_high': recent_high,
            'recent_low': recent_low,
            'atr_multiplier': atr_multiplier,
            'trail_price': trail_price,
            'trail_desc': trail_desc,
        }

    def _live_price_from_ws(self, position: Optional[Dict[str, Any]] = None, allow_rest: bool = False) -> float:
        ws = getattr(self.exchange, 'ws', None)
        inst_id = getattr(self.exchange, 'inst_id', None)
        if ws is not None and inst_id:
            try:
                if ws.is_fresh("public"):
                    ticker = ws.get_ticker(inst_id)
                    price = self._safe_float((ticker or {}).get('lastPr', 0))
                    if price > 0:
                        return price
            except Exception:
                pass

        if position is not None:
            mark_price = self._safe_float(position.get('markPrice', 0))
            if mark_price > 0:
                return mark_price

        if allow_rest:
            try:
                ticker = self.exchange.fetch_ticker(self.symbol)
                return self._safe_float(ticker.get('last', 0))
            except Exception:
                return 0.0

        return 0.0

    def _ws_position_snapshot(self) -> Optional[Dict[str, Any]]:
        ws = getattr(self.exchange, 'ws', None)
        inst_id = getattr(self.exchange, 'inst_id', None)
        if ws is None or not getattr(ws, 'enabled', False) or not inst_id:
            return None
        try:
            if not ws.is_fresh("private"):
                return None
        except Exception:
            return None
        payload = ws.get_position(inst_id)
        if not payload:
            return None

        contracts = self._safe_float(payload.get('total', 0))
        if contracts <= 0:
            return None

        side = payload.get('holdSide')
        entry_price = self._safe_float(payload.get('openPriceAvg', 0))
        mark_price = self._safe_float(payload.get('markPrice', 0))
        unrealized_pnl = self._safe_float(payload.get('unrealizedPL', 0))

        return {
            'symbol': self.symbol,
            'contracts': contracts,
            'side': side,
            'entryPrice': entry_price,
            'markPrice': mark_price,
            'unrealizedPnl': unrealized_pnl,
            'percentage': 0.0,
            'liquidationPrice': self._safe_float(payload.get('liquidationPrice', 0)),
            'marginMode': payload.get('marginMode'),
            'marginSize': self._safe_float(payload.get('marginSize', 0)),
            'leverage': self._safe_float(payload.get('leverage', self.leverage), self.leverage),
            'info': payload,
        }

    def _realtime_position_snapshot(self) -> Optional[Dict[str, Any]]:
        position = self._ws_position_snapshot()
        if position:
            return position

        now = time.time()
        if (now - self._last_risk_rest_position_at) < self.ws_risk_context_refresh_sec:
            return None

        self._last_risk_rest_position_at = now
        return self.get_active_position()

    def _synthetic_runtime_position(self, current_price: float) -> Optional[Dict[str, Any]]:
        contracts = self._safe_float(self.state.last_known_contracts, 0.0)
        entry_price = self._safe_float(self.state.entry_price, 0.0)
        side = str(self.state.position_side or '').lower()
        if contracts <= 0 or entry_price <= 0 or side not in {'long', 'short'}:
            return None

        margin_size = (contracts * entry_price) / max(self.leverage, 1)
        return {
            'symbol': self.symbol,
            'contracts': contracts,
            'side': side,
            'entryPrice': entry_price,
            'markPrice': current_price,
            'unrealizedPnl': 0.0,
            'percentage': 0.0,
            'liquidationPrice': 0.0,
            'marginMode': None,
            'marginSize': margin_size,
            'leverage': float(max(self.leverage, 1)),
            'info': {},
        }

    def _refresh_realtime_risk_context(
        self,
        position: Dict[str, Any],
        force: bool = False,
    ) -> Optional[Dict[str, float]]:
        side = str(position.get('side', self.state.position_side or '')).lower()
        layer = int(self.state.layer)
        key = (side, layer)
        now = time.time()
        if (
            not force
            and self._risk_context is not None
            and self._risk_context_key == key
            and (now - self._risk_context_at) < self.ws_risk_context_refresh_sec
        ):
            return self._risk_context

        context = self._build_risk_context(position_side=side, layer=layer)
        if context is None:
            return self._risk_context

        self._risk_context = context
        self._risk_context_key = key
        self._risk_context_at = now
        return context

    # =========================================================
    # 状态管理
    # =========================================================
    def _load_runtime_state(self):
        if not self.runtime_file.exists():
            return
        try:
            raw = self._load_json(self.runtime_file, default={})
            self.state = RuntimeState(
                layer=raw.get('layer', 0),
                pending_layer=raw.get('pending_layer', raw.get('layer', 0)),
                phase=raw.get('phase', 'PHASE1'),
                last_phase=raw.get('last_phase', raw.get('phase', 'PHASE1')),
                phase2_start_layer=raw.get('phase2_start_layer', 0),
                pending_entry_price=raw.get('pending_entry_price', 0.0),
                pending_entry_amount=raw.get('pending_entry_amount', 0.0),
                last_fill_price=raw.get('last_fill_price', 0.0),
                last_fill_time=raw.get('last_fill_time', ""),
                protective_stop_active=raw.get('protective_stop_active', False),
                protective_stop_order_id=raw.get('protective_stop_order_id', ""),
                protective_stop_client_oid=raw.get('protective_stop_client_oid', ""),
                protective_stop_price=raw.get('protective_stop_price', 0.0),
                best_profit_pct=raw.get('best_profit_pct', 0.0),
                position_side=raw.get('position_side'),
                last_known_contracts=raw.get('last_known_contracts', 0.0),
                bot_state=raw.get('bot_state', 'IDLE'),
                partial_tp_1_done=raw.get('partial_tp_1_done', False),
                partial_tp_2_done=raw.get('partial_tp_2_done', False),
                activated=raw.get('activated', False),
                entry_price=raw.get('entry_price', 0.0),
                last_update=raw.get('last_update', ""),
            )
            repaired = self._repair_phase2_start_layer()
            if repaired:
                self._save_runtime_state()
            print(
                f"✅ 加载状态: layer={self.state.layer}, "
                f"best_profit={self.state.best_profit_pct*100:.2f}%, "
                f"side={self.state.position_side}, "
                f"phase={self.state.phase}"
            )
        except Exception as e:
            print(f"⚠️ 加载状态失败: {e}")

    def _save_runtime_state(self):
        with self.state_lock:
            self.state.last_update = self._now_str()
            payload = asdict(self.state)
        self._save_json(self.runtime_file, payload)

    def _reset_state(self):
        self._clear_protective_stop(remote=True, force_all=True)
        with self.state_lock:
            self.state = RuntimeState()
            self._last_risk_log_profit_pct = 0.0
            self._risk_context = None
            self._risk_context_at = 0.0
            self._risk_context_key = None
            self._last_risk_rest_position_at = 0.0
        self._save_runtime_state()

    def _snapshot_strategy(self, stream: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        payload = {
            'name': 'Bitget 马丁策略机器人',
            'exchange': getattr(self.exchange, 'name', 'Bitget'),
            'mode': 'sandbox' if self.config.get('sandbox', True) else 'live',
            'symbol': self.symbol,
            'timeframe': self.timeframe,
            'sr_timeframe': self.sr_timeframe,
            'max_layers': self.max_layers,
            'leverage': self.leverage,
            'phase_switch_loss_pct': self.phase_switch_loss_pct,
            'phase_switch_layer': self.phase_switch_layer,
            'trend_follow_enabled': self.trend_follow_enabled,
            'trend_follow_adx_threshold': self.trend_follow_adx_threshold,
            'mean_reversion_adx_max': self.mean_reversion_adx_max,
            'first_order_ratio': self.first_order_ratio,
            'layer_multipliers': self.layer_multipliers,
            'phase1_max_layers': self.phase1_max_layers,
            'phase1_first_order_ratio': self.phase1_first_order_ratio,
            'phase1_layer_multipliers': self.phase1_layer_multipliers,
            'phase1_layer_min_gap_pct': self.phase1_layer_min_gap_pct,
            'phase1_layer_trigger_base_pct': self.phase1_layer_trigger_base_pct,
            'phase2_extra_layers': self.phase2_extra_layers,
            'phase2_layer_multipliers': self.phase2_layer_multipliers,
            'phase2_layer_min_gap_pct': self.phase2_layer_min_gap_pct,
            'phase2_layer_trigger_base_pct': self.phase2_layer_trigger_base_pct,
            'level_offset_pct': self.level_offset_pct,
            'add_layer_base_offset_pct': self.add_layer_base_offset_pct,
            'structure_min_gap_pct': self.structure_min_gap_pct,
            'layer_min_gap_pct': self.layer_min_gap_pct,
            'layer_trigger_base_pct': self.layer_trigger_base_pct,
            'protective_stop_enabled': self.protective_stop_enabled,
            'protective_stop_trigger_type': self.protective_stop_trigger_type,
            'protective_stop_profit_lock_ratio': self.protective_stop_profit_lock_ratio,
            'protective_stop_min_profit_pct': self.protective_stop_min_profit_pct,
            'trailing_min_close_profit_pct': self.trailing_min_close_profit_pct,
            'transport': (stream or {}).get('transport', 'rest'),
        }
        try:
            market = self._market() or {}
        except Exception:
            market = {}

        precision = market.get('precision') or {}
        limits = market.get('limits') or {}
        if precision:
            payload['price_precision'] = precision.get('price')
            payload['amount_precision'] = precision.get('amount')
        if limits:
            payload['min_amount'] = ((limits.get('amount') or {}).get('min'))
            payload['min_cost'] = ((limits.get('cost') or {}).get('min'))
        return payload

    def _snapshot_account(self) -> Optional[Dict[str, Any]]:
        snapshot = self.get_balance_snapshot()
        if snapshot is None:
            raise RuntimeError('balance unavailable')
        total = self._safe_float(snapshot.get('equity', 0))
        used = self._safe_float(snapshot.get('used', 0))
        return {
            'currency': 'USDT',
            'free': self._safe_float(snapshot.get('free', 0)),
            'used': used,
            'total': total,
            'utilization_pct': (used / total * 100) if total > 0 else 0.0,
            'tradable_margin': self._safe_float(snapshot.get('tradable_margin', 0)),
            'safe_tradable_margin': self._safe_float(snapshot.get('safe_tradable_margin', 0)),
        }

    def _snapshot_position(self, raw: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        raw = raw or self.get_active_position()
        if not raw:
            return None

        side = str(raw.get('side', '')).lower() or None
        entry_price = self._safe_float(raw.get('entryPrice', 0))
        mark_price = self._safe_float(raw.get('markPrice', 0))
        contracts = self._safe_float(raw.get('contracts', 0))
        notional = contracts * (mark_price or entry_price)
        liquidation = self._safe_float(raw.get('liquidationPrice', 0))
        distance_to_liq_pct = 0.0
        if mark_price and liquidation:
            distance_to_liq_pct = abs(mark_price - liquidation) / mark_price * 100

        return {
            'side': side,
            'contracts': contracts,
            'entry_price': entry_price,
            'mark_price': mark_price,
            'notional': notional,
            'unrealized_pnl': self._safe_float(raw.get('unrealizedPnl', 0)),
            'percentage': self._safe_float(raw.get('percentage', 0)),
            'liquidation_price': liquidation,
            'margin_mode': raw.get('marginMode'),
            'distance_to_liquidation_pct': distance_to_liq_pct,
        }

    def _snapshot_open_orders(self, raw_orders: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
        rows = raw_orders if raw_orders is not None else self.fetch_open_orders()
        if rows is None:
            raise RuntimeError('open orders unavailable')
        if not rows:
            return []
        normalized = []
        for order in rows:
            normalized.append(
                {
                    'id': order.get('id'),
                    'side': order.get('side'),
                    'type': order.get('type'),
                    'price': self._safe_float(order.get('price', 0)),
                    'amount': self._safe_float(order.get('amount', 0)),
                    'filled': self._safe_float(order.get('filled', 0)),
                    'status': order.get('status'),
                    'reduceOnly': bool(order.get('reduceOnly', False)),
                    'timestamp': int(self._safe_float(order.get('timestamp', 0))),
                }
            )
        return normalized

    def _snapshot_recent_trades(self) -> List[Dict[str, Any]]:
        raw_trades = self.exchange.fetch_my_trades(self.symbol, limit=60)
        ordered = sorted(raw_trades, key=lambda item: self._safe_float(item.get('timestamp', 0)))
        trades = []
        for trade in ordered:
            fee = trade.get('fee') or {}
            info = trade.get('info') or {}
            trades.append(
                {
                    'id': trade.get('id'),
                    'order_id': trade.get('order'),
                    'timestamp_ms': self._safe_float(trade.get('timestamp', 0)),
                    'timestamp': self._format_timestamp_ms(trade.get('timestamp')),
                    'side': trade.get('side'),
                    'taker_or_maker': trade.get('takerOrMaker'),
                    'price': self._safe_float(trade.get('price', 0)),
                    'amount': self._safe_float(trade.get('amount', 0)),
                    'cost': self._safe_float(trade.get('cost', 0)),
                    'fee': self._safe_float(fee.get('cost', 0)),
                    'fee_currency': fee.get('currency'),
                    'symbol': trade.get('symbol'),
                    'reduce_only': bool(info.get('reduceOnly', False)),
                }
            )
        return trades

    def _snapshot_ledger(self) -> List[Dict[str, Any]]:
        entries = []
        for entry in self.exchange.fetch_ledger(limit=80):
            entries.append(
                {
                    'timestamp': self._format_timestamp_ms(entry.get('timestamp')),
                    'currency': entry.get('currency'),
                    'amount': self._safe_float(entry.get('amount', 0)),
                    'before': self._safe_float(entry.get('before', 0)),
                    'after': self._safe_float(entry.get('after', 0)),
                    'type': entry.get('type'),
                    'id': entry.get('id'),
                }
            )
        return entries

    def _snapshot_price_series(self, frame: pd.DataFrame) -> Dict[str, Any]:
        points = []
        for _, row in frame.iterrows():
            points.append(
                {
                    'timestamp': self._format_timestamp_ms(row['timestamp']),
                    'close': self._safe_float(row['close']),
                    'ema_fast': self._safe_float(row['ema_fast']),
                    'ema_slow': self._safe_float(row['ema_slow']),
                    'volume': self._safe_float(row['volume']),
                }
            )
        return {'points': points}

    def _snapshot_market(self) -> Optional[Dict[str, Any]]:
        ticker = self.exchange.fetch_ticker(self.symbol)
        candles = self.fetch_ohlcv_df(limit=max(self.trend_lookback, 140))
        if candles is None:
            raise RuntimeError('market candles unavailable')

        indicators_frame = self.add_indicators(candles)
        latest = indicators_frame.iloc[-1]
        trend, signal = self._infer_signal(latest)
        support_resistance = self._quiet_call(self.find_support_resistance)
        context = self._build_risk_context(position_side=self.state.position_side, layer=self.state.layer)
        activate_pct = context['activate_pct'] if context else 0.0
        trail_ratio = context['trail_ratio'] if context else 0.5
        live_position = self.get_active_position()
        partial_targets = self._dynamic_partial_tp_targets(position=live_position, context=context)

        return {
            'ticker': {
                'last': self._safe_float(ticker.get('last', 0)),
                'bid': self._safe_float(ticker.get('bid', 0)),
                'ask': self._safe_float(ticker.get('ask', 0)),
                'high': self._safe_float(ticker.get('high', 0)),
                'low': self._safe_float(ticker.get('low', 0)),
                'change_pct': self._safe_float(ticker.get('percentage', 0)),
                'base_volume': self._safe_float(ticker.get('baseVolume', 0)),
                'quote_volume': self._safe_float(ticker.get('quoteVolume', 0)),
            },
            'indicators': {
                'price': self._safe_float(latest['close']),
                'ema_fast': self._safe_float(latest['ema_fast']),
                'ema_slow': self._safe_float(latest['ema_slow']),
                'rsi': self._safe_float(latest['rsi']),
                'adx': self._safe_float(latest['adx']),
                'atr': self._safe_float(latest['atr']),
                'trend': trend,
                'signal': signal,
                'dynamic_tp_activate_pct': activate_pct * 100,
                'dynamic_tp_trail_ratio': trail_ratio * 100,
                'dynamic_partial_tp1_pct': partial_targets['tp1_threshold'] * 100,
                'dynamic_partial_tp2_pct': partial_targets['tp2_threshold'] * 100,
                'dynamic_partial_tp1_ratio': partial_targets['tp1_ratio'] * 100,
                'dynamic_partial_tp2_ratio': partial_targets['tp2_ratio'] * 100,
                'dynamic_partial_tp_phase': partial_targets['phase'],
            },
            'support_resistance': support_resistance
            or {'current_price': self._safe_float(latest['close']), 'support': [], 'resistance': []},
            'price_series': self._snapshot_price_series(indicators_frame.tail(72)),
        }

    def _get_live_section(
        self,
        key: str,
        builder,
        ttl: float,
        warnings: List[str],
        force: bool = False,
        default: Any = None,
    ) -> Any:
        now = time.time()
        section_key = f'{key}_section'
        ts_key = f'{key}_at'
        has_cache = section_key in self._live_snapshot_cache
        if has_cache and not force and (now - self._live_snapshot_cache.get(ts_key, 0.0)) < ttl:
            return self._live_snapshot_cache.get(section_key)

        try:
            value = builder()
        except Exception as exc:
            warnings.append(f'{key}: {exc}')
            return self._live_snapshot_cache.get(section_key, default)

        if value is None and default is not None:
            value = default
        self._live_snapshot_cache[section_key] = value
        self._live_snapshot_cache[ts_key] = now
        return value

    def _build_live_snapshot(self, force: bool = False, include_market: bool = False) -> Dict[str, Any]:
        warnings: List[str] = []
        stream = self._get_live_section(
            'stream',
            lambda: self.exchange.ws_status() if hasattr(self.exchange, 'ws_status') else {'enabled': False, 'transport': 'rest'},
            ttl=1.0,
            warnings=warnings,
            force=force,
            default={'enabled': False, 'transport': 'rest'},
        )
        strategy = self._snapshot_strategy(stream)
        account = self._get_live_section('account', self._snapshot_account, ttl=2.0, warnings=warnings, force=force, default={})
        position = self._get_live_section('position', self._snapshot_position, ttl=1.0, warnings=warnings, force=force, default=None)
        open_orders = self._get_live_section(
            'open_orders',
            self._snapshot_open_orders,
            ttl=2.0,
            warnings=warnings,
            force=force,
            default=[],
        )
        recent_trades = self._get_live_section(
            'recent_trades',
            self._snapshot_recent_trades,
            ttl=self.local_snapshot_trade_interval,
            warnings=warnings,
            force=False,
            default=[],
        )
        ledger = self._get_live_section(
            'ledger',
            self._snapshot_ledger,
            ttl=self.local_snapshot_ledger_interval,
            warnings=warnings,
            force=False,
            default=[],
        )
        market = self._get_live_section(
            'market',
            self._snapshot_market,
            ttl=self.local_snapshot_market_interval,
            warnings=warnings,
            force=force or include_market,
            default={},
        )

        with self.state_lock:
            runtime = asdict(self.state)

        return {
            'timestamp': self._now_str(),
            'source': 'martin-bot',
            'warnings': warnings,
            'strategy': strategy,
            'runtime': runtime,
            'stream': stream,
            'account': account,
            'position': position,
            'open_orders': open_orders,
            'recent_trades': recent_trades,
            'ledger': ledger,
            'market': market,
        }

    def _write_live_snapshot(self, force: bool = False, include_market: bool = False) -> bool:
        if not self.local_snapshot_enabled:
            return False

        now = time.time()
        if not force and (now - self._last_live_snapshot_at) < self.local_snapshot_interval:
            return False

        with self.snapshot_lock:
            now = time.time()
            if not force and (now - self._last_live_snapshot_at) < self.local_snapshot_interval:
                return False
            snapshot = self._build_live_snapshot(force=force, include_market=include_market)
            self._save_json(self.live_snapshot_file, snapshot)
            self._last_live_snapshot_at = now
            return True

    # =========================================================
    # 账户 / 仓位 / 订单
    # =========================================================
    def setup_account(self):
        try:
            print("--- 设置账户参数 ---")
            self.exchange.set_leverage(self.leverage, self.symbol)
            print(f"✅ 杠杆: {self.leverage}X")
        except Exception as e:
            if "leverage not change" not in str(e).lower():
                print(f"⚠️ 设置杠杆失败: {e}")

    def get_wallet_balance(self) -> Optional[float]:
        snapshot = self.get_balance_snapshot()
        if snapshot is None:
            return None
        return snapshot.get('equity')

    def get_balance_snapshot(self) -> Optional[Dict[str, float]]:
        try:
            balance = self.exchange.fetch_balance({'type': 'swap'})
            usdt = balance.get('USDT', {})
            info_rows = balance.get('info') or []
            info = {}
            if isinstance(info_rows, list):
                for row in info_rows:
                    if isinstance(row, dict) and str(row.get('marginCoin', '')).upper() == 'USDT':
                        info = row
                        break
                if not info and info_rows and isinstance(info_rows[0], dict):
                    info = info_rows[0]

            equity = self._safe_float(usdt.get('total', 0))
            free = self._safe_float(usdt.get('free', 0))
            used = self._safe_float(usdt.get('used', 0))

            candidates = [
                self._safe_float(info.get('crossedMaxAvailable', 0)),
                self._safe_float(info.get('isolatedMaxAvailable', 0)),
                self._safe_float(info.get('unionAvailable', 0)),
                free,
            ]
            positive_candidates = [value for value in candidates if value > 0]
            tradable_margin = min(positive_candidates) if positive_candidates else max(free, equity, 0.0)
            safe_tradable_margin = tradable_margin * self.order_margin_safety_ratio

            return {
                'equity': equity,
                'free': free,
                'used': used,
                'tradable_margin': tradable_margin,
                'safe_tradable_margin': safe_tradable_margin,
            }
        except Exception as e:
            print(f"❌ 获取余额失败: {e}")
            return None

    def _is_insufficient_balance_error(self, error: Exception) -> bool:
        message = str(error).lower()
        keywords = [
            "insufficient",
            "available balance",
            "余额不足",
            "超出账户余额",
            "订单金额超出账户余额",
        ]
        return any(token in message for token in keywords)

    def _calculate_order_margin(self, desired_margin: float, balance_snapshot: Dict[str, float], label: str) -> float:
        safe_margin = self._safe_float(balance_snapshot.get('safe_tradable_margin', 0))
        if safe_margin <= 0:
            return 0.0
        order_margin = min(desired_margin, safe_margin)
        if order_margin < desired_margin:
            print(
                f"⚠️ {label} 目标保证金 {desired_margin:.2f} USDT 超过当前可新增保证金 "
                f"{safe_margin:.2f} USDT，已自动缩到 {order_margin:.2f} USDT"
            )
        return order_margin

    def _calculate_retry_amount(self, current_amount: float, entry_price: float) -> float:
        balance_snapshot = self.get_balance_snapshot()
        if balance_snapshot is None or entry_price <= 0:
            return 0.0

        safe_margin = self._safe_float(balance_snapshot.get('safe_tradable_margin', 0))
        if safe_margin <= 0:
            return 0.0

        margin_limited_amount = (safe_margin * self.leverage) / entry_price
        shrink_amount = current_amount * self.insufficient_balance_shrink_ratio
        retry_amount = min(margin_limited_amount, shrink_amount)
        retry_amount = self._normalize_amount(retry_amount)
        if retry_amount >= current_amount:
            retry_amount = self._normalize_amount(current_amount * self.insufficient_balance_shrink_ratio)
        return retry_amount

    def _submit_entry_order(
        self,
        order_side: str,
        amount: float,
        entry_price: float,
        label: str,
        order_type: str = 'limit',
    ) -> bool:
        with self.action_lock:
            if self._exit_in_progress.is_set():
                print(f"⚠️ {label} 下单前检测到平仓流程进行中，跳过本次挂单")
                return False
            try:
                create_price = entry_price if order_type == 'limit' else None
                self.exchange.create_order(self.symbol, order_type, order_side, amount, create_price)
                return True
            except Exception as e:
                if order_type == 'limit' and self._is_insufficient_balance_error(e):
                    retry_amount = self._calculate_retry_amount(amount, entry_price)
                    if retry_amount > 0 and retry_amount < amount:
                        print(
                            f"⚠️ {label} 下单时可用保证金不足，自动缩量重试: "
                            f"{amount:.6f} -> {retry_amount:.6f}"
                        )
                        self.exchange.create_order(self.symbol, 'limit', order_side, retry_amount, entry_price)
                        return True
                raise

    def _has_pending_entry_order(self, side: str) -> bool:
        open_orders = self.fetch_open_orders()
        if open_orders is None:
            return True
        for order in open_orders:
            if order.get('reduceOnly', False):
                continue
            if str(order.get('side', '')).lower() == side.lower():
                return True
        return False

    def get_active_position(self) -> Optional[Dict[str, Any]]:
        try:
            positions = self.exchange.fetch_positions([self.symbol])
            for pos in positions:
                contracts = self._safe_float(pos.get('contracts', 0))
                if contracts > 0:
                    return pos
            return None
        except Exception as e:
            print(f"❌ 获取持仓失败: {e}")
            return None

    def fetch_open_orders(self) -> Optional[List[Dict[str, Any]]]:
        try:
            return self.exchange.fetch_open_orders(self.symbol)
        except Exception as e:
            print(f"⚠️ 获取挂单失败: {e}")
            return None

    def cancel_all_orders(self):
        with self.action_lock:
            try:
                self.exchange.cancel_all_orders(self.symbol)
                print("✅ 已取消所有挂单")
                return True
            except Exception as e:
                if "not exist" not in str(e).lower():
                    print(f"⚠️ 取消挂单失败: {e}")
                return False

    def close_position(self, position):
        with self.action_lock:
            try:
                contracts = self._safe_float(position.get('contracts', 0))
                if contracts <= 0:
                    return False

                contracts = self._normalize_amount(contracts)
                if contracts <= 0:
                    print("⚠️ 平仓数量低于最小下单量，跳过")
                    return False

                side = str(position.get('side', '')).lower()
                close_side = 'sell' if side == 'long' else 'buy'

                print(f"\n--- 市价平仓 {contracts} ---")
                response = self.exchange.create_order(
                    self.symbol,
                    'market',
                    close_side,
                    contracts,
                    None,
                    {'reduceOnly': True}
                )
                print("✅ 平仓完成")
                return response
            except Exception as e:
                print(f"❌ 平仓失败: {e}")
                return None

    def _wait_for_position_close(self, expected_contracts: float, timeout_sec: float = 3.0) -> bool:
        deadline = time.time() + max(timeout_sec, 0.5)
        target = self._safe_float(expected_contracts, 0.0)
        while time.time() < deadline:
            latest = self.get_active_position()
            if not latest:
                return True
            remaining = self._safe_float(latest.get('contracts', 0.0), 0.0)
            if remaining <= 0:
                return True
            if target > 0 and remaining < max(target * 0.05, 0.01):
                return True
            time.sleep(0.2)
        return False

    def _partial_close(self, position, ratio, reason):
        with self.action_lock:
            try:
                contracts = self._safe_float(position.get('contracts', 0))
                if contracts <= 0:
                    return False

                close_amount = self._normalize_amount(contracts * ratio)
                if close_amount <= 0:
                    print(f"⚠️ {reason}: 平仓量过小，跳过")
                    return False

                side = 'buy' if str(position.get('side')).lower() == 'short' else 'sell'

                self.exchange.create_order(
                    self.symbol,
                    'market',
                    side,
                    close_amount,
                    None,
                    {'reduceOnly': True}
                )
                print(f"✅ {reason}: 平仓 {close_amount} ({ratio*100:.0f}%)")
                return True
            except Exception as e:
                print(f"❌ {reason} 失败: {e}")
                return False

    def _execute_partial_take_profit(self, position: Dict[str, Any], ratio: float, reason: str, state_flag: str) -> bool:
        if self._exit_in_progress.is_set():
            return False
        live_position = self.get_active_position() or position
        if not live_position:
            return False
        success = self._partial_close(live_position, ratio, reason)
        if success:
            self._set_runtime_flag(state_flag, True)
            self._write_live_snapshot(force=True, include_market=False)
        return success

    def _execute_exit_pipeline(self, reason: str, position: Optional[Dict[str, Any]] = None) -> bool:
        if self._exit_in_progress.is_set():
            return False

        with self.action_lock:
            if self._exit_in_progress.is_set():
                return False

            self._exit_in_progress.set()
            try:
                live_position = position or self.get_active_position()
                if not live_position:
                    self.cancel_all_orders()
                    self.sync_state_with_exchange()
                    return False

                print(f"🎯 {reason}")
                self._clear_protective_stop(remote=True, force_all=True)
                open_orders = self.fetch_open_orders() or []
                entry_orders = [order for order in open_orders if not order.get('reduceOnly', False)]
                if entry_orders:
                    self._cancel_entry_orders(entry_orders)
                close_response = self.close_position(live_position)
                closed = self._wait_for_position_close(self._safe_float(live_position.get('contracts', 0.0), 0.0))
                if closed:
                    self.cancel_all_orders()
                elif close_response:
                    print("⚠️ 平仓单已提交，但短时间内未确认平仓，跳过全撤单以免撤掉减仓单")
                self.sync_state_with_exchange()
                self._write_live_snapshot(force=True, include_market=True)
                return closed
            finally:
                self._exit_in_progress.clear()

    # =========================================================
    # K线 / 指标
    # =========================================================
    def fetch_ohlcv_df(self, timeframe=None, limit=100) -> Optional[pd.DataFrame]:
        timeframe = timeframe or self.timeframe
        try:
            ohlcv = self.exchange.fetch_ohlcv(self.symbol, timeframe, limit=limit)
            if not ohlcv or len(ohlcv) < 20:
                return None
            df = pd.DataFrame(
                ohlcv,
                columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']
            )
            return df
        except Exception as e:
            print(f"❌ 获取K线失败: {e}")
            return None

    def add_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # EMA
        df['ema_fast'] = df['close'].ewm(span=self.fast_ema_period, adjust=False).mean()
        df['ema_slow'] = df['close'].ewm(span=self.slow_ema_period, adjust=False).mean()

        # RSI
        delta = df['close'].diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.rolling(self.rsi_period).mean()
        avg_loss = loss.rolling(self.rsi_period).mean()
        rs = avg_gain / avg_loss.replace(0, pd.NA)
        df['rsi'] = 100 - (100 / (1 + rs))

        # ATR
        prev_close = df['close'].shift(1)
        tr1 = df['high'] - df['low']
        tr2 = (df['high'] - prev_close).abs()
        tr3 = (df['low'] - prev_close).abs()
        df['tr'] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        df['atr'] = df['tr'].rolling(self.atr_period).mean()

        # ADX
        up_move = df['high'].diff()
        down_move = -df['low'].diff()

        plus_dm = pd.Series(0.0, index=df.index)
        minus_dm = pd.Series(0.0, index=df.index)

        plus_dm[(up_move > down_move) & (up_move > 0)] = up_move
        minus_dm[(down_move > up_move) & (down_move > 0)] = down_move

        atr = df['atr'].replace(0, pd.NA)
        plus_di = 100 * (plus_dm.rolling(self.adx_period).mean() / atr)
        minus_di = 100 * (minus_dm.rolling(self.adx_period).mean() / atr)

        dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, pd.NA)
        df['adx'] = dx.rolling(self.adx_period).mean()

        return df

    def _infer_signal(self, row) -> Tuple[str, str]:
        return self._entry_bias_from_row(row)

    def get_trend_context(self) -> Optional[Dict[str, Any]]:
        df = self.fetch_ohlcv_df(limit=self.trend_lookback)
        if df is None:
            return None

        df = self.add_indicators(df)
        row = df.iloc[-1]

        current_price = self._safe_float(row['close'])
        fast_ema_val = self._safe_float(row['ema_fast'])
        slow_ema_val = self._safe_float(row['ema_slow'])
        rsi_val = self._safe_float(row['rsi'])
        adx_val = self._safe_float(row['adx'])

        market_state, signal = self._infer_signal(row)

        print(
            f"📊 价格={current_price:.2f} "
            f"EMA{self.fast_ema_period}={fast_ema_val:.2f} "
            f"EMA{self.slow_ema_period}={slow_ema_val:.2f} "
            f"RSI={rsi_val:.1f} "
            f"ADX={adx_val:.1f}"
        )
        print(f"📈 市场状态: {market_state} | 信号: {signal}")
        return {
            'market_state': market_state,
            'signal': signal,
            'price': current_price,
            'ema_fast': fast_ema_val,
            'ema_slow': slow_ema_val,
            'rsi': rsi_val,
            'adx': adx_val,
            'atr': self._safe_float(row.get('atr'), 0.0),
        }

    def get_trend(self) -> Tuple[Optional[str], Optional[float]]:
        context = self.get_trend_context()
        if not context:
            return None, None
        return context['signal'], context['price']

    def _first_entry_should_use_aggressive_trend_plan(
        self,
        trade_side: str,
        trend_context: Optional[Dict[str, Any]],
    ) -> bool:
        if not self.strong_trend_entry_enabled or not trend_context:
            return False
        signal = str(trend_context.get('signal') or '').upper()
        adx = self._safe_float(trend_context.get('adx'), 0.0)
        desired_signal = 'LONG' if trade_side.lower() == 'long' else 'SHORT'
        return signal == desired_signal and adx >= self.strong_trend_entry_adx

    def _is_pending_entry_signal_compatible(
        self,
        trade_side: str,
        trend_context: Optional[Dict[str, Any]],
    ) -> bool:
        if not trend_context:
            return False
        signal = str(trend_context.get('signal') or '').upper()
        market_state = str(trend_context.get('market_state') or '').upper()
        side = trade_side.lower()
        if side == 'long':
            if signal == 'LONG':
                return True
            return self.keep_pending_entry_on_stretched and market_state == 'BULLISH_BUT_STRETCHED'
        if side == 'short':
            if signal == 'SHORT':
                return True
            return self.keep_pending_entry_on_stretched and market_state == 'BEARISH_BUT_STRETCHED'
        return False

    def _build_first_entry_plan(
        self,
        trade_side: str,
        current_price: float,
        sr: Optional[Dict[str, Any]],
        trend_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        side = trade_side.lower()
        order_side = 'sell' if side == 'short' else 'buy'
        plan = {
            'entry_type': 'limit',
            'entry_price': 0.0,
            'source': 'structure',
            'order_side': order_side,
        }

        if self._first_entry_should_use_aggressive_trend_plan(side, trend_context):
            if self.strong_trend_entry_mode == 'market':
                plan['entry_type'] = 'market'
                plan['entry_price'] = current_price
                plan['source'] = 'strong_trend_market'
                return plan

            offset = self.strong_trend_limit_offset_pct
            if side == 'long':
                plan['entry_price'] = current_price * (1 + offset)
            else:
                plan['entry_price'] = current_price * (1 - offset)
            plan['source'] = 'strong_trend_marketable_limit'
            return plan

        atr_value = self._safe_float((trend_context or {}).get('atr'), 0.0)
        atr_pullback_ratio = (atr_value / current_price) * self.strong_trend_pullback_atr_ratio if current_price > 0 else 0.0
        allowed_pullback_pct = max(
            self.level_offset_pct,
            min(self.strong_trend_max_pullback_pct, atr_pullback_ratio) if atr_pullback_ratio > 0 else self.level_offset_pct,
        )

        if sr:
            if side == 'short':
                levels = sr['resistance']
                if levels:
                    preferred_price = levels[0] * (1 - self.level_offset_pct)
                    if trend_context and str(trend_context.get('signal') or '').upper() == 'SHORT':
                        max_chase_price = current_price * (1 + allowed_pullback_pct)
                        plan['entry_price'] = min(preferred_price, max_chase_price)
                        plan['source'] = 'structure_clamped_short'
                    else:
                        min_entry_price = current_price * (1 + self.level_offset_pct)
                        plan['entry_price'] = max(preferred_price, min_entry_price)
                else:
                    plan['entry_price'] = current_price * (1 + self.level_offset_pct)
                    plan['source'] = 'fallback_short'
            else:
                levels = sr['support']
                if levels:
                    preferred_price = levels[0] * (1 + self.level_offset_pct)
                    if trend_context and str(trend_context.get('signal') or '').upper() == 'LONG':
                        min_chase_price = current_price * (1 - allowed_pullback_pct)
                        plan['entry_price'] = max(preferred_price, min_chase_price)
                        plan['source'] = 'structure_clamped_long'
                    else:
                        max_entry_price = current_price * (1 - self.level_offset_pct)
                        plan['entry_price'] = min(preferred_price, max_entry_price)
                else:
                    plan['entry_price'] = current_price * (1 - self.level_offset_pct)
                    plan['source'] = 'fallback_long'
        else:
            plan['entry_price'] = current_price * (
                (1 + self.level_offset_pct) if side == 'short' else (1 - self.level_offset_pct)
            )
            plan['source'] = 'no_structure_fallback'

        return plan

    # =========================================================
    # 支撑阻力
    # =========================================================
    def _dedupe_price_levels(self, levels: List[float], min_gap_ratio=0.002) -> List[float]:
        """
        去重 / 合并过近价位
        min_gap_ratio = 0.2%
        """
        if not levels:
            return []

        levels = sorted(levels)
        merged = [levels[0]]
        for lv in levels[1:]:
            if abs(lv - merged[-1]) / merged[-1] >= min_gap_ratio:
                merged.append(lv)
        return merged

    def _timeframe_seconds(self, timeframe: Optional[str]) -> float:
        value = str(timeframe or self.timeframe).strip().lower()
        try:
            if value.endswith("m"):
                return float(value[:-1]) * 60
            if value.endswith("h"):
                return float(value[:-1]) * 3600
            if value.endswith("d"):
                return float(value[:-1]) * 86400
            if value.endswith("w"):
                return float(value[:-1]) * 604800
        except ValueError:
            return float("inf")
        return float("inf")

    def _structure_timeframes_for_layer(self, layer_num: int) -> List[str]:
        candidates: List[str] = [str(self.sr_timeframe)]
        if layer_num >= 2:
            candidates.append(str(self.timeframe))

        unique: List[str] = []
        for timeframe in sorted(candidates, key=self._timeframe_seconds):
            if timeframe not in unique:
                unique.append(timeframe)
        return unique

    def _find_volume_profile_levels(self, timeframe=None, lookback=100) -> List[float]:
        timeframe = str(timeframe or self.sr_timeframe)
        lookback = max(int(lookback or 100), 20)
        fetch_limit = max(lookback, self.atr_period + 20)
        df = self.fetch_ohlcv_df(timeframe=timeframe, limit=fetch_limit)
        if df is None or df.empty:
            return []

        df = self.add_indicators(df)
        window = df.tail(lookback).copy()
        if window.empty:
            return []

        price_min = self._safe_float(window['low'].min(), 0.0)
        price_max = self._safe_float(window['high'].max(), 0.0)
        price_range = max(price_max - price_min, 0.0)
        if price_min <= 0 or price_max <= 0 or price_range <= 0:
            return []

        atr_value = self._safe_float(window['atr'].iloc[-1], 0.0)
        fallback_width = price_range / 50 if price_range > 0 else 0.0
        bin_width = max(atr_value * 0.5, fallback_width, price_min * 0.0005)
        if bin_width <= 0:
            return []

        bin_count = max(int(price_range / bin_width) + 1, 1)
        volume_bins = [0.0] * bin_count

        for row in window.itertuples(index=False):
            candle_low = self._safe_float(getattr(row, 'low', 0.0), 0.0)
            candle_high = self._safe_float(getattr(row, 'high', 0.0), 0.0)
            volume = max(self._safe_float(getattr(row, 'volume', 0.0), 0.0), 0.0)
            if volume <= 0:
                continue

            start_price = min(candle_low, candle_high)
            end_price = max(candle_low, candle_high)
            start_idx = min(max(int((start_price - price_min) / bin_width), 0), bin_count - 1)
            end_idx = min(max(int((end_price - price_min) / bin_width), 0), bin_count - 1)

            touched_bins = max(end_idx - start_idx + 1, 1)
            volume_per_bin = volume / touched_bins
            for idx in range(start_idx, end_idx + 1):
                volume_bins[idx] += volume_per_bin

        ranked_bins = sorted(
            [
                (idx, volume)
                for idx, volume in enumerate(volume_bins)
                if volume > 0
            ],
            key=lambda item: (-item[1], item[0]),
        )
        top_levels = [
            price_min + (idx + 0.5) * bin_width
            for idx, _ in ranked_bins[:5]
        ]
        return self._dedupe_price_levels(top_levels, min_gap_ratio=0.001)

    def find_support_resistance(self, lookback=30, timeframe: Optional[str] = None):
        """
        返回最近 5 个支撑 / 阻力候选
        来源：
        - recent high / low
        - pivot r1/r2/r3, s1/s2/s3
        - EMA20 上下偏移
        """
        timeframe = str(timeframe or self.sr_timeframe)
        df = self.fetch_ohlcv_df(timeframe=timeframe, limit=lookback + 40)
        if df is None:
            return None

        current_price = self._safe_float(df['close'].iloc[-1])
        high = df['high']
        low = df['low']
        close = df['close']
        prev_close = close.shift(1)

        recent_high = high.tail(lookback).max()
        recent_low = low.tail(lookback).min()

        tr1 = high - low
        tr2 = (high - prev_close).abs()
        tr3 = (low - prev_close).abs()
        atr = self._safe_float(pd.concat([tr1, tr2, tr3], axis=1).max(axis=1).rolling(self.atr_period).mean().iloc[-1], 0.0)
        structure_gap_ratio = self._gap_ratio(
            current_price=current_price,
            atr_value=atr,
            base_pct=self.structure_min_gap_pct,
            atr_multiplier=self.structure_min_gap_atr_multiplier,
        )

        last = df.iloc[-1]
        pivot = (last['high'] + last['low'] + last['close']) / 3

        r1 = 2 * pivot - last['low']
        s1 = 2 * pivot - last['high']
        r2 = pivot + (last['high'] - last['low'])
        s2 = pivot - (last['high'] - last['low'])
        r3 = last['high'] + 2 * (pivot - last['low'])
        s3 = last['low'] - 2 * (last['high'] - pivot)

        ema20 = close.ewm(span=20, adjust=False).mean().iloc[-1]

        raw_res = [r1, r2, r3, recent_high]
        raw_sup = [s1, s2, s3, recent_low]
        if self.use_ema_structure:
            raw_res.append(ema20 * 1.01)
            raw_sup.append(ema20 * 0.99)

        vp_levels = self._find_volume_profile_levels(timeframe=timeframe)
        vp_resistance = [x for x in vp_levels if x > current_price]
        vp_support = [x for x in vp_levels if x < current_price]
        raw_res.extend(vp_resistance)
        raw_sup.extend(vp_support)

        resistance_candidates = [x for x in raw_res if x > current_price]
        support_candidates = [x for x in raw_sup if x < current_price]

        resistance_candidates = self._dedupe_price_levels(
            sorted(set(resistance_candidates)),
            min_gap_ratio=structure_gap_ratio,
        )
        support_candidates = self._dedupe_price_levels(
            sorted(set(support_candidates), reverse=False),
            min_gap_ratio=structure_gap_ratio,
        )

        # support 要从“离当前最近”往下排，所以逆序
        support_candidates = sorted(support_candidates, reverse=True)

        result = {
            'timeframe': timeframe,
            'current_price': current_price,
            'gap_ratio': structure_gap_ratio,
            'atr': atr,
            'vp_levels': vp_levels,
            'vp_resistance': sorted(vp_resistance),
            'vp_support': sorted(vp_support, reverse=True),
            'resistance': resistance_candidates[:5],
            'support': support_candidates[:5]
        }

        print(f"📊 支撑阻力[{timeframe}]: 当前价={current_price:.2f}")
        print(f"  VP结构位: {[f'{x:.2f}' for x in vp_levels]}")
        print(f"  阻力: {[f'{x:.2f}' for x in result['resistance']]}")
        print(f"  支撑: {[f'{x:.2f}' for x in result['support']]}")

        return result

    def _select_structure_entry_price(
        self,
        side: str,
        layer_num: int,
        current_price: float,
        avg_price: float,
        pending_entry_price: Optional[float] = None,
        phase: Optional[str] = None,
    ):
        """
        根据最新支撑阻力，选择“下一层”挂单价
        规则：
        - 做多：优先找低于当前价、低于均价的支撑位
        - 做空：优先找高于当前价、高于均价的阻力位
        - 若已有实际成交价，仅检查候选结构位与 last_fill_price 的最小间距
        """
        last_sr = None
        last_fill_price = self._safe_float(self.state.last_fill_price, 0.0)
        pending_anchor_price = self._safe_float(pending_entry_price, 0.0)
        min_gap_ratio = max(self._phase_config(phase).get('layer_min_gap_pct', 0.0), 0.0)

        for timeframe in self._structure_timeframes_for_layer(layer_num):
            sr = self.find_support_resistance(timeframe=timeframe)
            if not sr:
                continue
            last_sr = sr

            if side == 'long':
                candidates = sorted(
                    [s for s in sr['support'] if s < current_price and s < avg_price],
                    reverse=True,
                )
                if not candidates:
                    continue

                max_entry_price = min(current_price, avg_price) * (1 - self.level_offset_pct)
                valid_prices: List[float] = []
                for candidate in candidates:
                    preferred_price = min(candidate * (1 + self.level_offset_pct), max_entry_price)
                    if last_fill_price > 0 and min_gap_ratio > 0:
                        gap_ratio = (last_fill_price - preferred_price) / last_fill_price
                        if gap_ratio < min_gap_ratio:
                            continue
                    valid_prices.append(preferred_price)
                sticky_price = self._select_sticky_structure_price(
                    valid_prices,
                    pending_entry_price=pending_anchor_price,
                )
                if sticky_price is not None:
                    return sticky_price, sr
                if valid_prices:
                    return valid_prices[0], sr
                continue

            candidates = sorted(
                [r for r in sr['resistance'] if r > current_price and r > avg_price]
            )
            if not candidates:
                continue

            min_entry_price = max(current_price, avg_price) * (1 + self.level_offset_pct)
            valid_prices = []
            for candidate in candidates:
                preferred_price = max(candidate * (1 - self.level_offset_pct), min_entry_price)
                if last_fill_price > 0 and min_gap_ratio > 0:
                    gap_ratio = (preferred_price - last_fill_price) / last_fill_price
                    if gap_ratio < min_gap_ratio:
                        continue
                valid_prices.append(preferred_price)
            sticky_price = self._select_sticky_structure_price(
                valid_prices,
                pending_entry_price=pending_anchor_price,
            )
            if sticky_price is not None:
                return sticky_price, sr
            if valid_prices:
                return valid_prices[0], sr

        return None, last_sr

    def _select_atr_entry_price(self, side: str, layer_num: int, current_price: float, avg_price: float):
        """
        当没有合适结构位时，用 ATR 作为第二优先级
        """
        df = self.fetch_ohlcv_df(limit=80)
        if df is None:
            return None

        df = self.add_indicators(df)
        row = df.iloc[-1]
        atr = self._safe_float(row['atr'], current_price * 0.02)

        # 层数越深，距离越远
        # 第2层: 1.0 ATR
        # 第3层: 1.5 ATR
        # 第4层: 2.0 ATR
        # 第5层: 2.5 ATR
        atr_mult = 1.0 + max(layer_num - 2, 0) * 0.5
        distance = atr * atr_mult

        if side == 'long':
            price = min(avg_price, current_price) - distance
        else:
            price = max(avg_price, current_price) + distance

        return price

    def _select_fallback_entry_price(self, side: str, layer_num: int, current_price: float, avg_price: float):
        """
        最后兜底：固定百分比偏移
        """
        offset_pct = self.add_layer_base_offset_pct * layer_num
        if side == 'long':
            return avg_price * (1 - offset_pct)
        else:
            return avg_price * (1 + offset_pct)

    # =========================================================
    # 止盈 / 止损
    # =========================================================
    def calculate_dynamic_tp(self) -> Tuple[float, float]:
        context = self._build_risk_context()
        if context is None:
            return 0.05, 0.5
        print(
            f"📊 动态止盈: ADX={context['adx']:.1f} 波动率={context['volatility_pct']*100:.2f}% "
            f"| 激活阈值={context['activate_pct']*100:.2f}% 回撤比例={context['trail_ratio']*100:.0f}%"
        )
        return context['activate_pct'], context['trail_ratio']

    def _start_ws_risk_monitor(self):
        ws = getattr(self.exchange, 'ws', None)
        if not self.ws_risk_monitor_enabled or ws is None or not getattr(ws, 'enabled', False):
            return
        if self._risk_thread and self._risk_thread.is_alive():
            return
        self._risk_stop_event.clear()
        self._risk_thread = threading.Thread(
            target=self._ws_risk_loop,
            name="martin-ws-risk",
            daemon=True,
        )
        self._risk_thread.start()
        print(f"⚡ WS 实时风控已启动: {self.ws_risk_check_interval:.2f}s")

    def _stop_ws_risk_monitor(self):
        self._risk_stop_event.set()
        if self._risk_thread and self._risk_thread.is_alive():
            self._risk_thread.join(timeout=3)
        self._risk_thread = None

    def _ws_risk_loop(self):
        while not self._risk_stop_event.is_set():
            try:
                self._ws_risk_step()
            except Exception as e:
                print(f"⚠️ WS 实时风控错误: {e}")
            self._risk_stop_event.wait(self.ws_risk_check_interval)

    def _ws_risk_step(self):
        if self._exit_in_progress.is_set():
            return
        if self.state.bot_state != "IN_STRATEGY" or not self.state.position_side:
            return

        live_price = self._live_price_from_ws(None, allow_rest=False)
        position = self._realtime_position_snapshot()
        if not position:
            if live_price <= 0:
                return
            position = self._synthetic_runtime_position(live_price)
            if not position:
                return

        if live_price <= 0:
            live_price = self._live_price_from_ws(position, allow_rest=False)
        current_profit_pct = self._position_profit_pct(position, live_price if live_price > 0 else None)
        self._update_best_profit(
            current_profit_pct,
            source="WS",
            log_step_pct=self.ws_risk_log_step_pct,
        )
        self._write_live_snapshot(force=False, include_market=False)

        if current_profit_pct < -self.max_loss_pct:
            print(
                f"🔴 WS止损触发: 亏损 {current_profit_pct*100:.2f}% "
                f"< -{self.max_loss_pct*100:.0f}%"
            )
            self._execute_exit_pipeline("WS 止损触发，执行总平仓", position)
            return

        context = self._refresh_realtime_risk_context(position)
        if context is None:
            return

        best_profit_pct = self.state.best_profit_pct
        partial_targets = self._dynamic_partial_tp_targets(
            position=position,
            context=context,
            current_profit_pct=current_profit_pct,
            best_profit_pct=best_profit_pct,
        )
        if best_profit_pct < context['activate_pct']:
            return

        if not self.state.activated and self._set_runtime_flag('activated', True):
            print(
                f"⚡ WS移动止盈已激活: "
                f"{best_profit_pct*100:.2f}% >= {context['activate_pct']*100:.2f}%"
            )
        self._arm_protective_stop(
            position,
            current_profit_pct=current_profit_pct,
            best_profit_pct=best_profit_pct,
            reason="WS移动止盈激活",
        )

        if best_profit_pct >= partial_targets['tp2_threshold'] and not self.state.partial_tp_2_done:
            print(
                f"🎯 WS分批止盈2: 浮盈 {best_profit_pct*100:.2f}% "
                f">= 动态阈值 {partial_targets['tp2_threshold']*100:.2f}%，平仓20%"
            )
            if self._execute_partial_take_profit(position, partial_targets['tp2_ratio'], "分批止盈2", "partial_tp_2_done"):
                return

        if best_profit_pct >= partial_targets['tp1_threshold'] and not self.state.partial_tp_1_done:
            print(
                f"🎯 WS分批止盈1: 浮盈 {best_profit_pct*100:.2f}% "
                f">= 动态阈值 {partial_targets['tp1_threshold']*100:.2f}%，平仓30%"
            )
            if self._execute_partial_take_profit(position, partial_targets['tp1_ratio'], "分批止盈1", "partial_tp_1_done"):
                return

        current_price = live_price or self._safe_float(position.get('markPrice', 0), context['current_price'])
        drawdown = best_profit_pct - current_profit_pct
        max_drawdown = context['trail_ratio'] * best_profit_pct
        if self.state.layer >= 4:
            max_drawdown = min(max_drawdown, 0.035 if best_profit_pct > 0.10 else 0.05)

        if best_profit_pct > 0 and drawdown >= max_drawdown:
            if not self._allow_trailing_close(position, current_profit_pct, "WS回撤保护", best_profit_pct):
                return
            print(
                f"🔴 WS回撤保护触发: 回撤 {drawdown*100:.2f}% "
                f">= {max_drawdown*100:.2f}%"
            )
            self._execute_exit_pipeline("WS 回撤保护触发，执行总平仓", position)
            return

        if self.state.position_side == 'short':
            should_close = current_price > context['trail_price']
        else:
            should_close = current_price < context['trail_price']

        if should_close:
            if not self._allow_trailing_close(position, current_profit_pct, "WS ATR止盈", best_profit_pct):
                return
            print(f"🎯 WS ATR止盈触发: {context['trail_desc']}, 当前价={current_price:.2f}")
            self._execute_exit_pipeline("WS ATR止盈触发，执行总平仓", position)

    def check_trailing_tp(self, position):
        try:
            contracts = self._safe_float(position.get('contracts', 0))
            if contracts <= 0:
                return False

            current_price = self._live_price_from_ws(position, allow_rest=False)
            if current_price <= 0:
                current_price = self._safe_float(position.get('markPrice', 0))

            current_profit_pct = self._position_profit_pct(position, current_price if current_price > 0 else None)

            # 硬止损
            if current_profit_pct < -self.max_loss_pct:
                print(
                    f"🔴 止损触发: 亏损 {current_profit_pct*100:.2f}% "
                    f"< -{self.max_loss_pct*100:.0f}%"
                )
                return True

            # 更新最高浮盈
            self._update_best_profit(current_profit_pct, source="轮询")

            context = self._refresh_realtime_risk_context(position, force=True)
            if context is None:
                return False

            activate_pct = context['activate_pct']
            trail_ratio = context['trail_ratio']
            print(
                f"📊 动态止盈: ADX={context['adx']:.1f} 波动率={context['volatility_pct']*100:.2f}% "
                f"| 激活阈值={activate_pct*100:.2f}% 回撤比例={trail_ratio*100:.0f}%"
            )
            partial_targets = self._dynamic_partial_tp_targets(
                position=position,
                context=context,
                current_profit_pct=current_profit_pct,
                best_profit_pct=self.state.best_profit_pct,
            )
            print(
                f"📌 动态分批止盈: TP1>={partial_targets['tp1_threshold']*100:.2f}% 平30% "
                f"| TP2>={partial_targets['tp2_threshold']*100:.2f}% 平20% "
                f"| 阶段={partial_targets['phase']}"
            )

            if self.state.best_profit_pct < activate_pct:
                print(f"⏳ 等待激活: {self.state.best_profit_pct*100:.2f}% < {activate_pct*100:.2f}%")
                return False

            self._set_runtime_flag('activated', True)
            self._arm_protective_stop(
                position,
                current_profit_pct=current_profit_pct,
                best_profit_pct=self.state.best_profit_pct,
                reason="轮询移动止盈激活",
            )

            # 分批止盈2
            if self.state.best_profit_pct >= partial_targets['tp2_threshold'] and not self.state.partial_tp_2_done:
                print(
                    f"🎯 分批止盈2: 浮盈 {self.state.best_profit_pct*100:.2f}% "
                    f">= 动态阈值 {partial_targets['tp2_threshold']*100:.2f}%，平仓20%"
                )
                self._execute_partial_take_profit(position, partial_targets['tp2_ratio'], "分批止盈2", "partial_tp_2_done")
                return False

            # 分批止盈1
            if self.state.best_profit_pct >= partial_targets['tp1_threshold'] and not self.state.partial_tp_1_done:
                print(
                    f"🎯 分批止盈1: 浮盈 {self.state.best_profit_pct*100:.2f}% "
                    f">= 动态阈值 {partial_targets['tp1_threshold']*100:.2f}%，平仓30%"
                )
                self._execute_partial_take_profit(position, partial_targets['tp1_ratio'], "分批止盈1", "partial_tp_1_done")
                return False

            should_close = (
                current_price > context['trail_price']
                if self.state.position_side == 'short'
                else current_price < context['trail_price']
            )

            print(f"📊 {context['trail_desc']}, 当前价={current_price:.2f}")

            # 回撤保护
            if self.state.best_profit_pct > 0:
                drawdown = self.state.best_profit_pct - current_profit_pct
                max_drawdown = trail_ratio * self.state.best_profit_pct

                if self.state.layer >= 4:
                    max_drawdown = min(max_drawdown, 0.035 if self.state.best_profit_pct > 0.10 else 0.05)

                if drawdown >= max_drawdown:
                    if not self._allow_trailing_close(position, current_profit_pct, "回撤保护", self.state.best_profit_pct):
                        return False
                    print(
                        f"🔴 回撤保护触发: 回撤 {drawdown*100:.2f}% "
                        f">= {max_drawdown*100:.2f}%"
                    )
                    return True

            if should_close:
                if not self._allow_trailing_close(position, current_profit_pct, "ATR止盈", self.state.best_profit_pct):
                    return False
                print(f"🎯 ATR止盈触发: {context['trail_desc']}")
                return True

            return False

        except Exception as e:
            print(f"❌ 检查止盈止损失败: {e}")
            return False

    # =========================================================
    # 下单
    # =========================================================
    def place_first_order(self, trade_side, current_price):
        """
        首仓：
        long -> 最近支撑位
        short -> 最近阻力位
        """
        print(f"\n--- 部署 [{trade_side}] 首仓 ---")

        if self._exit_in_progress.is_set():
            print("⚠️ 当前正在执行平仓流程，跳过首仓挂单")
            return False

        balance_snapshot = self.get_balance_snapshot()
        if balance_snapshot is None:
            print("❌ 余额不足")
            return False
        equity = self._safe_float(balance_snapshot.get('equity', 0))
        if equity < self.min_balance:
            print("❌ 余额不足")
            return False

        order_side = 'sell' if trade_side.lower() == 'short' else 'buy'

        open_orders = self.fetch_open_orders()
        if open_orders is None:
            print("⚠️ 当前无法确认挂单状态，跳过首仓，避免重复下单")
            return False
        if open_orders:
            print(f"⚠️ 已有 {len(open_orders)} 个挂单，不重复挂首仓")
            return False

        if self._has_pending_entry_order(order_side):
            print(f"⚠️ 已存在同方向未成交挂单 [{order_side.upper()}]，跳过重复首仓")
            return False

        desired_margin = equity * self.first_order_ratio
        base_margin = self._calculate_order_margin(desired_margin, balance_snapshot, "首仓")
        if base_margin < self.min_balance:
            print("❌ 当前可新增保证金不足，跳过首仓")
            return False

        trend_context = self.get_trend_context()
        sr = self.find_support_resistance()
        entry_plan = self._build_first_entry_plan(
            trade_side,
            current_price,
            sr,
            trend_context=trend_context,
        )
        entry_type = str(entry_plan.get('entry_type', 'limit')).lower()
        entry_price = self._safe_float(entry_plan.get('entry_price', current_price), current_price)
        if entry_type == 'limit':
            entry_price = self._price_to_precision(entry_price)
        else:
            entry_price = current_price
        amount = (base_margin * self.leverage) / entry_price
        amount = self._normalize_amount(amount)

        if amount <= 0:
            print("❌ 首仓数量低于交易所最小下单量")
            return False

        if entry_type == 'market':
            print(
                f"📋 首仓: {order_side.upper()} {amount} @ 市价参考 {entry_price:.2f} "
                f"(保证金 {base_margin:.2f} USDT, 来源 {entry_plan['source']})"
            )
        else:
            print(
                f"📋 首仓: {order_side.upper()} {amount} @ {entry_price} "
                f"(保证金 {base_margin:.2f} USDT, 来源 {entry_plan['source']})"
            )

        try:
            if not self._submit_entry_order(order_side, amount, entry_price, "首仓", order_type=entry_type):
                return False
            print("✅ 首仓挂单已挂出")

            self.state.bot_state = "IN_STRATEGY"
            self.state.position_side = trade_side.lower()
            self.state.layer = 1
            self.state.pending_layer = 1
            self.state.best_profit_pct = 0.0
            self.state.partial_tp_1_done = False
            self.state.partial_tp_2_done = False
            self.state.activated = False
            self.state.entry_price = entry_price
            self.state.pending_entry_price = 0.0 if entry_type == 'market' else entry_price
            self.state.pending_entry_amount = amount
            self.state.last_fill_price = 0.0
            self.state.last_fill_time = ""
            self.state.protective_stop_active = False
            self.state.protective_stop_order_id = ""
            self.state.protective_stop_client_oid = ""
            self.state.protective_stop_price = 0.0
            self.state.last_known_contracts = 0.0
            self._save_runtime_state()
            self._write_live_snapshot(force=True, include_market=True)
            return True
        except Exception as e:
            print(f"❌ 首仓下单失败: {e}")
            return False

    def place_add_order(self, layer_num, current_price):
        """
        每次补下一层时：
        1. 重新计算最新支撑/阻力
        2. 优先用结构位
        3. 没有合适结构位 -> ATR 距离
        4. 再不行 -> 固定偏移兜底
        """
        if layer_num > self.max_layers:
            print(f"已达最大层数 {self.max_layers}")
            return False

        print(f"\n--- 第 {layer_num} 层加仓 ---")

        open_orders = self.fetch_open_orders()
        if open_orders is None:
            print("⚠️ 当前无法确认挂单状态，跳过本层加仓，避免重复挂单")
            return False
        non_reduce_orders = [o for o in open_orders if not o.get('reduceOnly', False)]
        if non_reduce_orders:
            print(f"⚠️ 已存在 {len(non_reduce_orders)} 个非减仓挂单，跳过补挂")
            return False

        position = self.get_active_position()
        plan = self._build_add_order_plan(layer_num, current_price, position=position)
        if plan is None:
            return False
        self._log_add_order_plan(plan)
        return self._submit_add_order_plan(plan)

    # =========================================================
    # 状态同步
    # =========================================================
    def estimate_current_layer(self, contracts: float, price: float, balance: float) -> int:
        if balance <= 0 or price <= 0 or contracts <= 0:
            return 1

        base_margin = balance * self.first_order_ratio
        total_contracts = 0.0

        for layer in range(1, self.max_layers + 1):
            layer_margin = base_margin * self.layer_multipliers[layer - 1]
            layer_contracts = (layer_margin * self.leverage) / price
            total_contracts += layer_contracts
            if contracts <= total_contracts * 1.15:
                return layer

        return self.max_layers

    def sync_state_with_exchange(self):
        position = self.get_active_position()
        open_orders = self.fetch_open_orders()
        if open_orders is None:
            open_orders = []
        non_reduce_orders = [o for o in open_orders if not o.get('reduceOnly', False)]

        if position:
            contracts = self._safe_float(position.get('contracts', 0))
            side = str(position.get('side', '')).lower()
            entry_price = self._safe_float(position.get('entryPrice', 0))

            self.state.bot_state = "IN_STRATEGY"
            self.state.position_side = side
            self.state.last_known_contracts = contracts
            self.state.entry_price = entry_price

            price = entry_price or self._safe_float(position.get('markPrice', 0))
            if price <= 0:
                ticker = self.exchange.fetch_ticker(self.symbol)
                price = self._safe_float(ticker.get('last', 0))
            balance = self.get_wallet_balance() or 0

            self.state.layer = self.estimate_current_layer(contracts, price, balance)
            self.state.phase = self._current_phase(position, current_price=price)
            self._repair_phase2_start_layer()
            self.state.pending_layer = (
                min(self.max_layers, self.state.layer + 1)
                if non_reduce_orders else self.state.layer
            )
            self.state.pending_entry_price = self._pending_entry_price_from_orders(non_reduce_orders)
            self.state.pending_entry_amount = (
                self._normalize_amount(self._safe_float(non_reduce_orders[0].get('amount', 0.0), 0.0))
                if non_reduce_orders else 0.0
            )
            if self.state.last_fill_price <= 0:
                self.state.last_fill_price = entry_price
            self._save_runtime_state()
            self._write_live_snapshot(force=True, include_market=True)

            print(f"✅ 同步持仓成功: side={side}, contracts={contracts}, layer={self.state.layer}")
        else:
            open_orders = self.fetch_open_orders()
            if open_orders is None:
                print("⚠️ 挂单状态获取失败，保持当前状态，稍后重试")
                self._write_live_snapshot(force=True, include_market=False)
                return
            if open_orders:
                first_order = open_orders[0]
                self.state.bot_state = "IN_STRATEGY"
                self.state.position_side = 'long' if first_order['side'] == 'buy' else 'short'
                if self.state.layer == 0:
                    self.state.layer = 1
                self.state.pending_layer = max(self.state.pending_layer, self.state.layer)
                self.state.pending_entry_price = self._pending_entry_price_from_orders(open_orders)
                self.state.pending_entry_amount = self._normalize_amount(
                    self._safe_float(open_orders[0].get('amount', 0.0), 0.0)
                )
                self._save_runtime_state()
                self._write_live_snapshot(force=True, include_market=True)
                print(f"✅ 同步挂单状态: {len(open_orders)} 个挂单, side={self.state.position_side}")
            else:
                self._reset_state()
                self._write_live_snapshot(force=True, include_market=True)
                print("✅ 同步为空仓空单状态")

    # =========================================================
    # 主循环
    # =========================================================
    def run(self):
        print("\n" + "=" * 60)
        print("🤖 BITGET 马丁策略机器人（最终版）")
        print(f"  交易所: {getattr(self.exchange, 'name', 'Bitget')}")
        print(f"  交易对: {self.symbol}")
        print(f"  杠杆: {self.leverage}X")
        print(f"  Phase1 首仓: {self.phase1_first_order_ratio*100:.2f}%")
        print(f"  Phase1 层数/倍率: {self.phase1_max_layers} / {self.phase1_layer_multipliers}")
        print(f"  Phase2 额外层数/倍率: {self.phase2_extra_layers} / {self.phase2_layer_multipliers}")
        print(f"  总最大层数: {self.max_layers}")
        print(
            f"  阶段切换: 亏损达到 {self.phase_switch_loss_pct*100:.2f}% "
            f"或层数达到 {self.phase_switch_layer}"
        )
        print("=" * 60)

        self._bootstrap_exchange()
        startup_position = self.get_active_position()
        print(f"🧭 启动阶段: {self._current_phase(startup_position)}")
        self._reconcile_startup_entry_orders()
        self._set_runtime_flag('last_phase', self.state.phase)
        self._start_ws_risk_monitor()
        self._write_live_snapshot(force=True, include_market=True)

        try:
            while True:
                try:
                    position = self.get_active_position()

                    if self._exit_in_progress.is_set():
                        print("⏳ 平仓流程执行中，等待交易所状态同步")
                        time.sleep(min(self.loop_interval, 2))
                        continue

                    # ====================================
                    # IDLE
                    # ====================================
                    if self.state.bot_state == "IDLE":
                        print(f"\n--- [状态: IDLE] ---")

                        if position:
                            print("检测到持仓，切换为策略模式")
                            self.sync_state_with_exchange()
                            time.sleep(self.loop_interval)
                            continue

                        trend_context = self.get_trend_context()
                        signal = trend_context['signal'] if trend_context else None
                        price = trend_context['price'] if trend_context else None
                        if signal == "SHORT":
                            self.place_first_order('short', price)
                        elif signal == "LONG":
                            self.place_first_order('long', price)
                        else:
                            print("等待信号...")

                    # ====================================
                    # IN_STRATEGY
                    # ====================================
                    elif self.state.bot_state == "IN_STRATEGY":
                        side_text = (self.state.position_side or "UNKNOWN").upper()
                        current_phase = self._current_phase(position)
                        print(f"\n--- [状态: IN_STRATEGY] ({side_text}, {current_phase}) ---")

                        # 1) 有持仓
                        if position:
                            current_contracts = self._safe_float(position.get('contracts', 0))

                            # 止盈止损
                            if self.check_trailing_tp(position):
                                self._execute_exit_pipeline("轮询止盈/止损触发，执行总平仓", position)
                                time.sleep(self.loop_interval)
                                continue

                            # 检测加/减仓成交并同步已确认层级
                            previous_contracts = self.state.last_known_contracts
                            if abs(current_contracts - previous_contracts) > 1e-9:
                                if current_contracts > previous_contracts:
                                    added = current_contracts - previous_contracts
                                    if previous_contracts > 0:
                                        print(f"🎉 检测到加仓成交: +{added:.6f}")
                                    else:
                                        print(f"🎉 检测到新开仓: {current_contracts:.6f}")
                                else:
                                    reduced = previous_contracts - current_contracts
                                    print(f"🎉 检测到减仓成交: -{reduced:.6f}")

                                with self.state_lock:
                                    self.state.last_known_contracts = current_contracts
                                    if current_contracts > previous_contracts and previous_contracts <= 0 and self.state.last_fill_price <= 0:
                                        filled_price = self._safe_float(self.state.pending_entry_price, 0.0)
                                        if filled_price <= 0:
                                            filled_price = self._safe_float(position.get('entryPrice', current_price), current_price)
                                        self.state.last_fill_price = filled_price
                                        self.state.last_fill_time = self._now_str()
                                        self.state.pending_entry_price = 0.0
                                        self.state.pending_entry_amount = 0.0
                                    if current_contracts > previous_contracts and self.state.pending_layer > self.state.layer:
                                        self.state.layer = self.state.pending_layer
                                        filled_price = self._safe_float(self.state.pending_entry_price, 0.0)
                                        if filled_price <= 0:
                                            filled_price = self._safe_float(position.get('entryPrice', current_price), current_price)
                                        self.state.last_fill_price = filled_price
                                        self.state.last_fill_time = self._now_str()
                                        self.state.pending_entry_price = 0.0
                                        self.state.pending_entry_amount = 0.0
                                    if self.state.pending_layer < self.state.layer:
                                        self.state.pending_layer = self.state.layer
                                self._save_runtime_state()

                            if self._exit_in_progress.is_set():
                                time.sleep(min(self.loop_interval, 2))
                                continue

                            # 只补下一层，不一次性全挂
                            next_layer = self.state.layer + 1
                            phase_cfg = self._phase_config(current_phase)
                            if next_layer <= phase_cfg['max_layers']:
                                open_orders = self.fetch_open_orders()
                                if open_orders is None:
                                    print("⚠️ 当前无法确认挂单状态，先不补挂，下一轮再试")
                                    time.sleep(self.loop_interval)
                                    continue
                                add_orders = [o for o in open_orders if not o.get('reduceOnly', False)]
                                phase_changed = current_phase != str(self.state.last_phase or current_phase).upper()
                                if phase_changed:
                                    print(
                                        f"🧭 运行中阶段切换: {self.state.last_phase or 'UNKNOWN'} -> {current_phase}，"
                                        "检查并按新阶段参数重建加仓挂单"
                                    )
                                target_pending_layer = (
                                    min(self.max_layers, self.state.layer + 1)
                                    if add_orders else self.state.layer
                                )
                                if target_pending_layer != self.state.pending_layer:
                                    self._set_runtime_flag('pending_layer', target_pending_layer)
                                if len(add_orders) == 0:
                                    if phase_changed:
                                        self._set_runtime_flag('last_phase', current_phase)
                                    ticker = self.exchange.fetch_ticker(self.symbol)
                                    price = self._safe_float(ticker.get('last', 0))
                                    self.place_add_order(next_layer, price)
                                else:
                                    reconcile_price = self._safe_float(
                                        position.get('markPrice', 0),
                                        self._safe_float(position.get('entryPrice', 0), 0.0),
                                    )
                                    if reconcile_price <= 0:
                                        ticker = self.exchange.fetch_ticker(self.symbol)
                                        reconcile_price = self._safe_float(ticker.get('last', 0))
                                    if reconcile_price > 0:
                                        reason_prefix = ""
                                        if phase_changed:
                                            reason_prefix = f"阶段切换[{current_phase}] "
                                        self._reconcile_active_entry_orders(
                                            add_orders,
                                            next_layer,
                                            reconcile_price,
                                            position,
                                            reason_prefix=reason_prefix,
                                        )
                                        open_orders = self.fetch_open_orders() or []
                                        add_orders = [o for o in open_orders if not o.get('reduceOnly', False)]
                                    if phase_changed:
                                        self._set_runtime_flag('last_phase', current_phase)
                                    pending_price = self._pending_entry_price_from_orders(add_orders)
                                    if pending_price > 0 and abs(pending_price - self.state.pending_entry_price) > 1e-9:
                                        self._set_runtime_flag('pending_entry_price', pending_price)
                                    if add_orders:
                                        pending_amount = self._normalize_amount(self._safe_float(add_orders[0].get('amount', 0.0), 0.0))
                                        if abs(pending_amount - self.state.pending_entry_amount) > 1e-9:
                                            self._set_runtime_flag('pending_entry_amount', pending_amount)
                                    trigger_orders = [o for o in add_orders if o.get('type') == 'trigger']
                                    if trigger_orders:
                                        print(
                                            f"📋 当前已有 {len(add_orders)} 个加仓挂单: "
                                            f"委托价 {pending_price:.2f}"
                                        )
                                    else:
                                        print(
                                            f"📋 当前已有 {len(add_orders)} 个加仓挂单: "
                                            f"委托价 {pending_price:.2f}"
                                        )
                            else:
                                if self.state.pending_layer != self.state.layer:
                                    self._set_runtime_flag('pending_layer', self.state.layer)
                                if self.state.pending_entry_price != 0.0:
                                    self._set_runtime_flag('pending_entry_price', 0.0)
                                if self.state.pending_entry_amount != 0.0:
                                    self._set_runtime_flag('pending_entry_amount', 0.0)
                                if current_phase == 'PHASE1':
                                    print(
                                        f"⚠️ 已达 PHASE1 阶段上限 {phase_cfg['max_layers']}，"
                                        f"等待亏损达到 {self.phase_switch_loss_pct*100:.2f}% 或更深层级后再切到 PHASE2"
                                    )
                                else:
                                    print(f"⚠️ 已达最后一层 {phase_cfg['max_layers']}，不再补挂，只做风控和止盈")

                        # 2) 无持仓，但可能有挂单
                        else:
                            if self.state.last_known_contracts > 0:
                                print("✅ 检测到持仓已关闭，本轮结束")
                                self.cancel_all_orders()
                                self._reset_state()
                                time.sleep(self.loop_interval)
                                continue

                            open_orders = self.fetch_open_orders()
                            if open_orders is None:
                                print("⚠️ 当前无法确认挂单状态，保持原状态，下一轮再试")
                                time.sleep(self.loop_interval)
                                continue
                            if not open_orders:
                                print("📭 无持仓无挂单，回到 IDLE")
                                self._reset_state()
                                time.sleep(self.loop_interval)
                                continue

                            # 无持仓但有挂单 -> 检查趋势
                            trend_context = self.get_trend_context()
                            signal = trend_context['signal'] if trend_context else None
                            price = trend_context['price'] if trend_context else None

                            if self.state.position_side == 'long' and signal == 'SHORT':
                                print("⚠️ 原计划做多，但当前更适合反向做空，撤单重挂")
                                self.cancel_all_orders()
                                self._reset_state()
                                self.place_first_order('short', price)

                            elif self.state.position_side == 'short' and signal == 'LONG':
                                print("⚠️ 原计划做空，但当前更适合反向做多，撤单重挂")
                                self.cancel_all_orders()
                                self._reset_state()
                                self.place_first_order('long', price)

                            elif signal == "WAIT":
                                if self._is_pending_entry_signal_compatible(
                                    str(self.state.position_side or ''),
                                    trend_context,
                                ):
                                    print("📊 信号转为 stretched，但趋势方向仍兼容，保留首仓挂单继续等待")
                                else:
                                    print("📊 横盘/方向失效，取消挂单，等待更清晰信号")
                                    self.cancel_all_orders()
                                    self._reset_state()

                            else:
                                print("📊 趋势与挂单方向兼容，继续等待成交")

                    self._write_live_snapshot(force=False, include_market=True)
                    time.sleep(self.loop_interval)

                except KeyboardInterrupt:
                    print("\n用户中断，退出...")
                    break
                except Exception as e:
                    print(f"❌ 主循环错误: {e}")
                    traceback.print_exc()
                    time.sleep(self.error_sleep)
        finally:
            self._write_live_snapshot(force=True, include_market=True)
            self._stop_ws_risk_monitor()

    # =========================================================
    # CLI
    # =========================================================
    def main(self):
        parser = argparse.ArgumentParser(description='BITGET 马丁策略机器人（最终版）')
        parser.add_argument('--run', action='store_true', help='运行机器人')
        parser.add_argument('--balance', action='store_true', help='查询余额')
        parser.add_argument('--position', action='store_true', help='查询持仓')
        parser.add_argument('--trend', action='store_true', help='趋势分析')
        parser.add_argument('--sync', action='store_true', help='同步交易所状态到本地状态')
        args = parser.parse_args()

        if args.run:
            self.run()
        elif args.balance:
            self.setup_account()
            balance = self.get_wallet_balance()
            print(f"\n💰 账户余额: {balance} USDT")
        elif args.position:
            self.setup_account()
            pos = self.get_active_position()
            if pos:
                print(f"\n📊 当前持仓:")
                print(f"  方向: {pos.get('side')}")
                print(f"  数量: {pos.get('contracts')}")
                print(f"  均价: {pos.get('entryPrice')}")
                print(f"  盈亏: {pos.get('unrealizedPnl')}")
                print(f"  收益率: {pos.get('percentage')}")
            else:
                print("\n无持仓")
        elif args.trend:
            self.setup_account()
            self.get_trend()
        elif args.sync:
            self.setup_account()
            self.sync_state_with_exchange()
        else:
            parser.print_help()


if __name__ == '__main__':
    configure_runtime_logging()
    bot = MartinBot()
    bot.main()
