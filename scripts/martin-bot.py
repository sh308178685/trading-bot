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
   - 只使用移动止盈，不做 TP1 / TP2 分批减仓
   - 当前仓位层数越深，移动止盈越早激活、允许回撤越小
   - 激活后保持生效，直到整轮仓位确认退出

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
import uuid
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
    symbol: str = ""
    layer: int = 0
    pending_layer: int = 0
    phase: str = "PHASE1"
    last_phase: str = "PHASE1"
    phase2_start_layer: int = 0
    pending_entry_price: float = 0.0
    pending_entry_amount: float = 0.0
    pending_entry_order_id: str = ""
    pending_entry_client_oid: str = ""
    last_fill_price: float = 0.0
    last_fill_time: str = ""
    last_fill_order_id: str = ""
    last_fill_amount: float = 0.0
    last_fill_price_source: str = ""
    unresolved_fill_order_id: str = ""
    unresolved_fill_accounted_amount: float = 0.0
    zero_fill_canceled_entry_order_id: str = ""
    protective_stop_active: bool = False
    protective_stop_order_id: str = ""
    protective_stop_client_oid: str = ""
    protective_stop_price: float = 0.0
    best_profit_pct: float = 0.0
    active_trailing_drawdown_ratio: float = 0.0
    position_side: Optional[str] = None     # long / short
    last_known_contracts: float = 0.0
    bot_state: str = "IDLE"                 # IDLE / IN_STRATEGY
    activated: bool = False
    entry_price: float = 0.0
    initial_balance: float = 0.0            # 首仓时的余额基准，加仓仓位基于此计算
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

        # 保留原来的 ADX / ATR 波动率动态止盈，再用层数系数逐层收紧。
        # 列表不足 max_layers 时沿用最后一项。
        default_activation_layer_multipliers = [
            1.00, 0.90, 0.80, 0.70, 0.60, 0.52, 0.44, 0.37, 0.30,
        ]
        default_drawdown_layer_multipliers = [
            1.00, 0.92, 0.84, 0.76, 0.68, 0.60, 0.52, 0.44, 0.36,
        ]
        self.trailing_activation_layer_multipliers = self._normalize_layer_schedule(
            self.config.get('trailing_activation_layer_multipliers'),
            default_activation_layer_multipliers,
            minimum=0.05,
            maximum=1.0,
        )
        self.trailing_drawdown_layer_multipliers = self._normalize_layer_schedule(
            self.config.get('trailing_drawdown_layer_multipliers'),
            default_drawdown_layer_multipliers,
            minimum=0.05,
            maximum=1.0,
        )

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
            self.config.get('protective_stop_profit_lock_ratio', 0.75),
            0.75,
        )
        self.protective_stop_min_profit_pct = self._safe_float(
            self.config.get('protective_stop_min_profit_pct', default_protective_floor),
            default_protective_floor,
        )
        self.protective_stop_update_step_pct = self._safe_float(
            self.config.get('protective_stop_update_step_pct', 0.001),
            0.001,
        )
        self.trailing_activation_min_pct = max(
            self._safe_float(
                self.config.get('trailing_activation_min_pct', self.protective_stop_min_profit_pct),
                self.protective_stop_min_profit_pct,
            ),
            0.0001,
        )
        self.trailing_activation_max_pct = max(
            self._safe_float(self.config.get('trailing_activation_max_pct', 0.01), 0.01),
            self.trailing_activation_min_pct,
        )
        self.trailing_drawdown_min_ratio = min(
            max(self._safe_float(self.config.get('trailing_drawdown_min_ratio', 0.10), 0.10), 0.01),
            0.95,
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
        self.runtime_save_lock = threading.Lock()
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
        self._state_sync_required = False
        self._state_sync_reason = ""

        self.exchange = self._init_exchange()
        self.markets = None

        self.state = RuntimeState(symbol=self.symbol)
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
        temp_path = path.with_suffix(
            path.suffix
            + f'.{os.getpid()}.{threading.get_ident()}.{uuid.uuid4().hex}.tmp'
        )
        try:
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            # Windows readers (dashboard、Defender、索引器等) 可能会在极短时间内
            # 以不共享删除的方式占用目标文件，使原子替换报 WinError 5/32/33。
            # 始终重试同一个已完整写好的临时文件，既保留原子性，也不先删除旧状态。
            replace_retry_delays = (0.02, 0.05, 0.10, 0.20, 0.40)
            for attempt in range(len(replace_retry_delays) + 1):
                try:
                    os.replace(temp_path, path)
                    break
                except OSError as exc:
                    if getattr(exc, 'winerror', None) not in {5, 32, 33}:
                        raise
                    if attempt >= len(replace_retry_delays):
                        raise
                    time.sleep(replace_retry_delays[attempt])
        finally:
            try:
                temp_path.unlink(missing_ok=True)
            except OSError:
                pass

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

    def _normalize_layer_schedule(
        self,
        raw_values: Any,
        defaults: List[float],
        minimum: float,
        maximum: float,
    ) -> List[float]:
        configured = list(raw_values) if isinstance(raw_values, (list, tuple)) else []
        fallback_values = list(defaults) or [minimum]
        normalized: List[float] = []
        for index in range(max(int(self.max_layers), 1)):
            fallback = fallback_values[min(index, len(fallback_values) - 1)]
            raw_value = configured[index] if index < len(configured) else fallback
            value = min(max(self._safe_float(raw_value, fallback), minimum), maximum)
            if normalized:
                # 配置写反时也不能让深层仓位比浅层更晚止盈。
                value = min(value, normalized[-1])
            normalized.append(value)
        return normalized

    @staticmethod
    def _layer_schedule_value(values: List[float], layer: int, default: float) -> float:
        if not values:
            return default
        index = min(max(int(layer or 1), 1), len(values)) - 1
        return float(values[index])

    def _fallback_trailing_values_for_layer(self, layer: Optional[int] = None) -> Tuple[float, float]:
        effective_layer = max(int(layer if layer is not None else self.state.layer), 1)
        activation_floor = max(
            self._safe_float(getattr(self, 'trailing_activation_min_pct', 0.005), 0.005),
            0.0001,
        )
        drawdown_multiplier = self._layer_schedule_value(
            getattr(self, 'trailing_drawdown_layer_multipliers', [1.0]),
            effective_layer,
            1.0,
        )
        trail_ratio = max(
            self._safe_float(getattr(self, 'trailing_drawdown_min_ratio', 0.10), 0.10),
            0.25 * drawdown_multiplier,
        )
        return activation_floor, trail_ratio

    def _tighten_active_trail_ratio(self, candidate: float) -> float:
        normalized = min(max(self._safe_float(candidate, 0.0), 0.01), 0.95)
        changed = False
        with self.state_lock:
            current = self._safe_float(self.state.active_trailing_drawdown_ratio, 0.0)
            if current <= 0 or normalized < current - 1e-12:
                self.state.active_trailing_drawdown_ratio = normalized
                current = normalized
                changed = True
        if changed:
            self._save_runtime_state()
        return current

    def _mark_state_sync_required(self, reason: str) -> None:
        reason_text = str(reason or '').strip() or 'unknown'
        if not self._state_sync_required or reason_text != self._state_sync_reason:
            print(f"⚠️ 状态同步待重试: {reason_text}")
        self._state_sync_required = True
        self._state_sync_reason = reason_text

    def _clear_state_sync_required(self) -> None:
        self._state_sync_required = False
        self._state_sync_reason = ""

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

    def _layer_price_to_safe_precision(
        self,
        price: float,
        side: str,
        anchor_price: float,
        spacing_ratio: float,
    ) -> float:
        raw_price = self._safe_float(price, 0.0)
        try:
            step = self._safe_float((self._market().get('precision') or {}).get('price'), 0.0)
        except Exception:
            step = 0.0
        if step > 0 and raw_price > 0:
            units = raw_price / step
            if side == 'short':
                raw_price = math.ceil(units - 1e-12) * step
            else:
                raw_price = math.floor(units + 1e-12) * step
        quantized = self._price_to_precision(raw_price)
        required_price = anchor_price * (
            1 + spacing_ratio if side == 'short' else 1 - spacing_ratio
        )
        tolerance = max(abs(anchor_price) * 1e-12, 1e-15)
        if step > 0:
            if side == 'short' and quantized + tolerance < required_price:
                quantized = self._price_to_precision(quantized + step)
            elif side != 'short' and quantized > required_price + tolerance:
                quantized = self._price_to_precision(max(quantized - step, 0.0))
        return quantized

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
        trail_ratio: Optional[float] = None,
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

        resolved_trail_ratio = self._safe_float(trail_ratio, 0.0)
        if resolved_trail_ratio <= 0:
            resolved_trail_ratio = self._safe_float(
                self.state.active_trailing_drawdown_ratio,
                0.0,
            )
        if resolved_trail_ratio <= 0:
            _, resolved_trail_ratio = self._fallback_trailing_values_for_layer(self.state.layer)
        # 软件回撤线与交易所服务端保护单必须使用同一套收紧逻辑。
        # 例如允许回撤 25%，保护单至少锁住最高浮盈的 75%。
        lock_ratio = min(
            max(self.protective_stop_profit_lock_ratio, 1.0 - resolved_trail_ratio, 0.0),
            1.0,
        )
        locked_profit_pct = max(
            self.protective_stop_min_profit_pct,
            best_profit * max(lock_ratio, 0.0),
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
            'trail_ratio': resolved_trail_ratio,
            'lock_ratio': lock_ratio,
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
        with self.action_lock:
            remote_changed = False
            order_id = self.state.protective_stop_order_id
            client_oid = self.state.protective_stop_client_oid
            has_local_stop = bool(
                order_id or client_oid or self.state.protective_stop_active
            )

            if remote and (force_all or has_local_stop):
                try:
                    cancel_result = self.exchange.cancel_position_stop_loss(
                        self.symbol,
                        order_id=order_id or None,
                        client_oid=client_oid or None,
                    )
                    if isinstance(cancel_result, dict) and cancel_result.get('alreadyAbsent'):
                        print(
                            f"ℹ️ 远端保护止损已不存在，同步清理本地身份: "
                            f"orderId={order_id or '--'}"
                        )
                    remote_changed = True
                except Exception as exc:
                    # 远端取消没有得到成功确认时必须保留本地身份，供下一轮重试。
                    print(f"⚠️ 取消保护止损失败，保留远端订单身份: {exc}")
                    return False

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
        trail_ratio: Optional[float] = None,
        force: bool = False,
    ) -> bool:
        if not self.protective_stop_enabled or self._exit_in_progress.is_set():
            return False

        try:
            with self.action_lock:
                if (
                    self._exit_in_progress.is_set()
                    or self.state.bot_state != 'IN_STRATEGY'
                ):
                    return False
                live_position = self.get_active_position()
                if live_position is self._POSITION_API_ERROR or not live_position:
                    return False
                live_contracts = self._safe_float(live_position.get('contracts', 0.0), 0.0)
                live_side = str(live_position.get('side') or '').lower()
                state_side = str(self.state.position_side or '').lower()
                if (
                    live_contracts <= 0
                    or live_side not in {'long', 'short'}
                    or (state_side in {'long', 'short'} and state_side != live_side)
                ):
                    return False

                target = self._protective_stop_target(
                    live_position,
                    current_profit_pct=current_profit_pct,
                    best_profit_pct=best_profit_pct,
                    trail_ratio=trail_ratio,
                )
                if not target:
                    return False
                if not force and not self._should_update_protective_stop(
                    str(target['side']),
                    target['trigger_price'],
                ):
                    return False

                order_id = self.state.protective_stop_order_id or None
                current_client_oid = self.state.protective_stop_client_oid or None
                response_client_oid = current_client_oid
                if order_id or current_client_oid:
                    try:
                        response = self.exchange.modify_tpsl_order(
                            self.symbol,
                            trigger_price=target['trigger_price'],
                            trigger_type=self.protective_stop_trigger_type,
                            execute_price=self.protective_stop_execute_price,
                            order_id=order_id,
                            client_oid=current_client_oid,
                            size="",
                        )
                    except Exception:
                        # 取消必须先得到明确成功；失败会直接跳到外层并保留旧身份。
                        cancel_result = self.exchange.cancel_position_stop_loss(
                            self.symbol,
                            order_id=order_id,
                            client_oid=current_client_oid,
                        )
                        if isinstance(cancel_result, dict) and cancel_result.get('alreadyAbsent'):
                            print(
                                f"ℹ️ 旧保护止损已不存在，继续部署替代保护单: "
                                f"orderId={order_id or '--'}"
                            )
                        with self.state_lock:
                            self.state.protective_stop_active = False
                            self.state.protective_stop_order_id = ""
                            self.state.protective_stop_client_oid = ""
                            self.state.protective_stop_price = 0.0
                        self._save_runtime_state()
                        response_client_oid = self._new_client_order_id()
                        response = self.exchange.place_position_stop_loss(
                            self.symbol,
                            hold_side=str(target['hold_side']),
                            trigger_price=target['trigger_price'],
                            trigger_type=self.protective_stop_trigger_type,
                            execute_price=self.protective_stop_execute_price,
                            client_oid=response_client_oid,
                        )
                else:
                    response_client_oid = self._new_client_order_id()
                    response = self.exchange.place_position_stop_loss(
                        self.symbol,
                        hold_side=str(target['hold_side']),
                        trigger_price=target['trigger_price'],
                        trigger_type=self.protective_stop_trigger_type,
                        execute_price=self.protective_stop_execute_price,
                        client_oid=response_client_oid,
                    )

                response = response or {}
                new_order_id = str(response.get('id') or order_id or "")
                new_client_oid = str(
                    response.get('clientOrderId') or response_client_oid or ""
                )
                with self.state_lock:
                    self.state.protective_stop_active = True
                    self.state.protective_stop_order_id = new_order_id
                    self.state.protective_stop_client_oid = new_client_oid
                    self.state.protective_stop_price = self._safe_float(target['trigger_price'], 0.0)
                self._save_runtime_state()
        except Exception as exc:
            print(f"⚠️ 挂保护止损失败: {exc}")
            return False
        self._write_live_snapshot(force=True, include_market=False)
        print(
            f"🛡️ {reason}: 已挂保护止损 {target['side']} "
            f"@ {target['trigger_price']:.2f}，锁定收益下限 {target['locked_profit_pct']*100:.2f}%"
        )
        return True

    def _phase_config(self, phase: Optional[str] = None) -> Dict[str, Any]:
        normalized = str(phase or self.state.phase or 'PHASE1').upper()
        if normalized == 'PHASE2':
            return {
                'phase': 'PHASE2',
                'max_layers': self.max_layers,
                'first_order_ratio': self.phase1_first_order_ratio,
                # 提前切到 PHASE2 只改变风控/间距参数，倍率仍按全局层号映射：
                # layer 4 固定对应 phase2_layer_multipliers[0]，不能从提前切换层重新计数。
                'layer_multipliers': self.layer_multipliers,
                'layer_index_offset': 0,
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
        offset = int(phase_cfg.get('layer_index_offset', 0))

        primary_index = layer_num - offset - 1
        if 0 <= primary_index < len(layer_multipliers):
            return primary_index

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

    def _entry_order_identity(self, order: Optional[Dict[str, Any]]) -> Tuple[str, str]:
        row = order or {}
        info = row.get('info') or {}
        initial = info.get('initial') if isinstance(info.get('initial'), dict) else {}
        order_id = str(row.get('id') or row.get('orderId') or info.get('orderId') or '').strip()
        client_oid = str(
            row.get('clientOrderId')
            or row.get('clientOid')
            or info.get('clientOid')
            or info.get('clientOrderId')
            or info.get('text')
            or initial.get('text')
            or ''
        ).strip()
        return order_id, client_oid

    @staticmethod
    def _is_bot_entry_client_oid(client_oid: Optional[str]) -> bool:
        """Return whether a client identity belongs to this bot's entry flow."""
        value = str(client_oid or '').strip().lower()
        return bool(value) and value.startswith(('t-martin-', 'martin-'))

    def _is_owned_entry_order(
        self,
        order: Optional[Dict[str, Any]],
        *,
        allow_tracked_identity: bool = True,
    ) -> bool:
        """Identify bot entry orders before any adoption, cancellation, or rebuild."""
        order_id, client_oid = self._entry_order_identity(order)
        if self._is_bot_entry_client_oid(client_oid):
            return True
        if client_oid:
            # An explicit non-bot client identity is authoritative evidence of
            # foreign ownership; a matching server order ID cannot override it.
            return False

        # A persisted identity is useful for an order whose exchange response
        # omitted clientOrderId.  It is only a continuation of an already-owned
        # order; a new unlabelled/manual order is never adopted by side alone.
        if allow_tracked_identity and order_id:
            tracked_ids = {
                str(self.state.pending_entry_order_id or '').strip(),
                str(self.state.unresolved_fill_order_id or '').strip(),
                str(self.state.last_fill_order_id or '').strip(),
            }
            if order_id in tracked_ids - {''}:
                return True
        return False

    def _split_owned_entry_orders(
        self,
        orders: Optional[List[Dict[str, Any]]],
        position_side: Optional[str],
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        side = str(position_side or '').lower()
        expected_side = 'buy' if side == 'long' else 'sell' if side == 'short' else ''
        relevant = [
            order for order in (orders or [])
            if not order.get('reduceOnly', False)
        ]
        owned = [
            order for order in relevant
            if self._is_owned_entry_order(order)
            and (
                not expected_side
                or str(order.get('side', '')).lower() == expected_side
            )
        ]
        # Return every foreign entry order, including the opposite side.  A
        # no-position cleanup must never call cancel-all on an order we do not
        # own merely because its side is unexpected.
        foreign = [
            order for order in relevant
            if not self._is_owned_entry_order(order)
        ]
        return owned, foreign

    def _position_open_timestamp_ms(
        self,
        position: Optional[Dict[str, Any]],
    ) -> float:
        """Extract the exchange position's opening timestamp when available."""
        row = position or {}
        info = row.get('info') or {}
        raw = row.get('timestamp')
        if self._safe_float(raw, 0.0) <= 0:
            for key in (
                'open_time', 'openTime', 'first_open_time', 'firstOpenTime',
                'open_timestamp', 'openTimestamp', 'cTime', 'ctime',
            ):
                raw = info.get(key)
                if raw not in (None, ''):
                    break
        timestamp_ms = self._safe_float(raw, 0.0)
        if 0 < timestamp_ms < 1_000_000_000_000:
            timestamp_ms *= 1000.0
        return timestamp_ms

    def _last_fill_timestamp_ms(self) -> float:
        raw = str(self.state.last_fill_time or '').strip()
        if not raw:
            return 0.0
        try:
            return datetime.fromisoformat(raw).timestamp() * 1000.0
        except (TypeError, ValueError, OSError, OverflowError):
            return self._safe_float(raw, 0.0)

    def _guard_verified_anchor_for_position(
        self,
        position: Optional[Dict[str, Any]],
    ) -> bool:
        """Invalidate a verified anchor that predates the current position cycle."""
        if not self._has_verified_last_fill_price():
            return False
        position_open_ms = self._position_open_timestamp_ms(position)
        if position_open_ms <= 0:
            # Exchanges without an opening timestamp retain the existing strict
            # order identity and trade-history checks.
            return True
        anchor_timestamp_ms = self._last_fill_timestamp_ms()
        if anchor_timestamp_ms > 0 and anchor_timestamp_ms + 5000.0 >= position_open_ms:
            return True

        anchor_order_id = str(self.state.last_fill_order_id or '').strip()
        accounted_amount = self._safe_float(self.state.last_fill_amount, 0.0)
        with self.state_lock:
            self.state.last_fill_price = 0.0
            self.state.last_fill_time = ''
            self.state.last_fill_order_id = ''
            self.state.last_fill_amount = 0.0
            self.state.last_fill_price_source = ''
            if anchor_order_id:
                self.state.unresolved_fill_order_id = anchor_order_id
                self.state.unresolved_fill_accounted_amount = max(accounted_amount, 0.0)
        self._mark_state_sync_required("末次成交锚点早于当前持仓周期")
        self._save_runtime_state()
        print("🛑 已隔离早于当前持仓周期的末次成交锚点，禁止生成加仓计划")
        return False

    @staticmethod
    def _entry_execution_order_id(order: Optional[Dict[str, Any]]) -> str:
        """Return Gate's triggered child order ID, never its unrelated me_order_id."""
        row = order or {}
        info = row.get('info') or {}
        execution_order_id = str(
            row.get('triggerExecutionOrderId')
            or row.get('executionOrderId')
            or info.get('trade_id_string')
            or info.get('trade_id')
            or ''
        ).strip()
        return '' if execution_order_id in {'', '0'} else execution_order_id

    def _promote_trigger_execution_order_id(
        self,
        trigger_order_id: str,
        execution_order_id: str,
    ) -> None:
        parent_id = str(trigger_order_id or '').strip()
        child_id = str(execution_order_id or '').strip()
        if not parent_id or not child_id or parent_id == child_id:
            return
        changed = False
        with self.state_lock:
            if str(self.state.pending_entry_order_id or '').strip() == parent_id:
                self.state.pending_entry_order_id = child_id
                changed = True
            if str(self.state.unresolved_fill_order_id or '').strip() == parent_id:
                self.state.unresolved_fill_order_id = child_id
                changed = True
            if str(self.state.last_fill_order_id or '').strip() == parent_id:
                self.state.last_fill_order_id = child_id
                changed = True
            if str(self.state.zero_fill_canceled_entry_order_id or '').strip() == parent_id:
                # A trigger-created child proves the parent was not a zero-fill cancel.
                self.state.zero_fill_canceled_entry_order_id = ''
                changed = True
        if changed:
            self._save_runtime_state()
            print(
                f"🔗 Gate 条件单已触发，成交身份切换: {parent_id} -> {child_id}"
            )

    def _has_verified_last_fill_price(self) -> bool:
        price = self._safe_float(self.state.last_fill_price, 0.0)
        source = str(self.state.last_fill_price_source or '').strip().lower()
        order_id = str(self.state.last_fill_order_id or '').strip()
        unresolved_order_id = str(self.state.unresolved_fill_order_id or '').strip()
        return (
            price > 0
            and source == 'trade_history'
            and bool(order_id)
            and not unresolved_order_id
        )

    def _fetch_authoritative_my_trades(self, limit: int) -> List[Dict[str, Any]]:
        """Fetch a complete REST-backed fill window when the adapter supports it."""
        authoritative_fetch = getattr(
            self.exchange,
            'fetch_my_trades_authoritative',
            None,
        )
        if callable(authoritative_fetch):
            return authoritative_fetch(self.symbol, limit=limit) or []
        # Gate and ordinary CCXT adapters already use their REST trade history here.
        return self.exchange.fetch_my_trades(self.symbol, limit=limit) or []

    def _fetch_authoritative_order(self, order_id: str) -> Dict[str, Any]:
        """Fetch an order including exchange-specific completed-order fallbacks."""
        fetch_order = getattr(self.exchange, 'fetch_order_authoritative', None)
        if not callable(fetch_order):
            fetch_order = getattr(self.exchange, 'fetch_order', None)
        if not callable(fetch_order):
            raise RuntimeError("exchange does not support authoritative order lookup")
        return fetch_order(order_id, self.symbol) or {}

    def _resolve_order_id_from_client_oid(self, client_oid: str) -> Optional[str]:
        resolver = getattr(
            self.exchange,
            'fetch_order_by_client_id_authoritative',
            None,
        )
        if not callable(resolver):
            return None
        expected_client_oid = str(client_oid or '').strip()
        if not expected_client_oid:
            return None
        order = resolver(expected_client_oid, self.symbol) or {}
        order_id, recovered_client_oid = self._entry_order_identity(order)
        execution_order_id = self._entry_execution_order_id(order)
        if not order_id:
            raise RuntimeError("client order lookup returned no server order id")
        if recovered_client_oid and recovered_client_oid != expected_client_oid:
            raise RuntimeError("client order lookup returned a different client id")
        return execution_order_id or order_id

    def _authoritative_entry_order_ownership(
        self,
        order: Optional[Dict[str, Any]],
        expected_order_id: str,
    ) -> Optional[bool]:
        """Check ownership when an authoritative order view exposes identity.

        ``None`` means the exchange omitted client identity; callers retain the
        existing strict trade/position checks for that legacy response.  An
        explicit foreign identity is never accepted.  Trigger children inherit
        ownership only through the verified parent and its ``trade_id`` link.
        """
        fetched_order_id, client_oid = self._entry_order_identity(order)
        if fetched_order_id and fetched_order_id != str(expected_order_id).strip():
            return False
        info = (order or {}).get('info') or {}
        initial = info.get('initial') if isinstance(info.get('initial'), dict) else {}
        explicit_client_oid = str(
            client_oid
            or initial.get('text')
            or info.get('text')
            or ''
        ).strip()
        if explicit_client_oid:
            return self._is_bot_entry_client_oid(explicit_client_oid)
        if str((order or {}).get('type') or '').strip().lower() == 'trigger':
            # No parent identity means there is no safe way to attribute a
            # trigger-created child, even if me_order_id is present.
            return False
        return None

    def _query_latest_entry_fill(
        self,
        position: Optional[Dict[str, Any]],
        preferred_order_id: Optional[str] = None,
        preferred_client_oid: Optional[str] = None,
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """Return (query_is_complete, recovered_fill)."""
        position_info = (position or {}).get('info') or {}
        raw_position_side = str(
            (position or {}).get('side') or position_info.get('holdSide') or ''
        ).lower()
        if raw_position_side not in {'long', 'short'}:
            print("⚠️ 真实持仓方向不明确，无法从成交记录恢复末次加仓价")
            return False, None
        side = raw_position_side
        expected_trade_side = 'sell' if side == 'short' else 'buy'
        position_open_ms = self._position_open_timestamp_ms(position)
        cycle_tolerance_ms = 5000.0
        preferred_id = str(preferred_order_id or '').strip()
        preferred_client_id = str(preferred_client_oid or '').strip()
        trade_limit = 60
        try:
            raw_trades = self._fetch_authoritative_my_trades(trade_limit)
        except Exception as exc:
            print(f"⚠️ 获取最近成交记录失败，无法确认末次加仓价: {exc}")
            return False, None
        raw_trades = sorted(
            raw_trades,
            key=lambda trade: self._safe_float(
                trade.get('timestamp', (trade.get('info') or {}).get('cTime', 0)),
                0.0,
            ),
        )

        candidates: List[Dict[str, Any]] = []
        seen_trade_ids = set()
        for sequence, trade in enumerate(raw_trades):
            info = trade.get('info') or {}
            trade_id = str(trade.get('id') or info.get('tradeId') or '').strip()
            if trade_id and trade_id in seen_trade_ids:
                continue
            if trade_id:
                seen_trade_ids.add(trade_id)
            trade_side = str(trade.get('side') or info.get('side') or '').lower()
            if trade_side != expected_trade_side:
                continue

            reduce_only_value = trade.get('reduceOnly', info.get('reduceOnly', False))
            if reduce_only_value is True or str(reduce_only_value).strip().lower() in {'1', 'true', 'yes'}:
                continue
            trade_side_detail = str(info.get('tradeSide') or '').lower()
            if any(token in trade_side_detail for token in ('close', 'reduce', 'burst', 'delivery', 'adl')):
                continue
            trade_position_side = str(
                trade.get('positionSide')
                or trade.get('holdSide')
                or info.get('positionSide')
                or info.get('holdSide')
                or ''
            ).lower()
            if trade_position_side in {'long', 'short'} and trade_position_side != side:
                continue

            price = self._safe_float(trade.get('price', info.get('priceAvg', info.get('price', 0))), 0.0)
            amount = self._safe_float(
                trade.get('amount', info.get('size', info.get('baseVolume', 0))),
                0.0,
            )
            if price <= 0 or amount <= 0:
                continue

            order_id = str(
                trade.get('order')
                or trade.get('orderId')
                or info.get('orderId')
                or ''
            ).strip()
            trade_client_oid = str(
                trade.get('clientOrderId')
                or trade.get('clientOid')
                or info.get('clientOid')
                or info.get('clientOrderId')
                or info.get('text')
                or ''
            ).strip()
            if (
                not order_id
                or (preferred_id and order_id != preferred_id)
                or (preferred_client_id and trade_client_oid != preferred_client_id)
            ):
                continue

            timestamp_ms = self._safe_float(
                trade.get('timestamp', info.get('cTime', info.get('uTime', 0))),
                0.0,
            )
            if timestamp_ms <= 0:
                continue
            if (
                position_open_ms > 0
                and timestamp_ms + cycle_tolerance_ms < position_open_ms
            ):
                continue
            candidates.append(
                {
                    'price': price,
                    'amount': max(amount, 0.0),
                    'order_id': order_id,
                    'timestamp_ms': timestamp_ms,
                    'sequence': sequence,
                }
            )

        if not candidates:
            if len(raw_trades) >= trade_limit:
                print("⚠️ 成交查询窗口已满，无法证明目标订单从未成交")
                return False, None
            return True, None

        latest = max(candidates, key=lambda row: (row['timestamp_ms'], row['sequence']))
        latest_timestamp = latest['timestamp_ms']
        latest_order_ids = {
            row['order_id'] for row in candidates if row['timestamp_ms'] == latest_timestamp
        }
        if not preferred_id and not preferred_client_id and len(latest_order_ids) > 1:
            print("⚠️ 最近时刻存在多个同方向开仓订单，无法唯一确定末次加仓")
            return False, None
        latest_order_id = latest['order_id']
        same_order = (
            [row for row in candidates if row['order_id'] == latest_order_id]
            if latest_order_id
            else [latest]
        )
        if len(raw_trades) >= trade_limit and min(row['sequence'] for row in same_order) == 0:
            print("⚠️ 末次成交订单超出成交查询窗口，无法完整聚合部分成交")
            return False, None
        total_amount = sum(row['amount'] for row in same_order)
        fill_price = sum(row['price'] * row['amount'] for row in same_order) / total_amount
        latest_timestamp_ms = max(row['timestamp_ms'] for row in same_order)
        return True, {
            'price': fill_price,
            'amount': total_amount,
            'order_id': latest_order_id,
            'timestamp_ms': latest_timestamp_ms,
        }

    def _fetch_latest_entry_fill(
        self,
        position: Optional[Dict[str, Any]],
        preferred_order_id: Optional[str] = None,
        preferred_client_oid: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        _, recovered = self._query_latest_entry_fill(
            position,
            preferred_order_id=preferred_order_id,
            preferred_client_oid=preferred_client_oid,
        )
        return recovered

    def _entry_order_fill_barrier(self, order_ids: List[str]) -> Optional[str]:
        """Return a filled order id, empty string for no fill, or None if unknown."""
        expected_ids = {str(order_id).strip() for order_id in order_ids if str(order_id).strip()}
        if not expected_ids:
            return None
        trade_limit = 60
        try:
            trades = self._fetch_authoritative_my_trades(trade_limit)
        except Exception as exc:
            print(f"⚠️ 撤单后无法核对成交记录，禁止立即重挂: {exc}")
            return None
        for trade in trades:
            info = trade.get('info') or {}
            order_id = str(
                trade.get('order')
                or trade.get('orderId')
                or info.get('orderId')
                or ''
            ).strip()
            if order_id in expected_ids and self._safe_float(
                trade.get('amount', info.get('size', info.get('baseVolume', 0.0))),
                0.0,
            ) > 0:
                return order_id
        if len(trades) >= trade_limit:
            print("⚠️ 成交查询窗口已满，无法证明被撤订单没有边界成交，禁止立即重挂")
            return None

        zero_fill_canceled_order_id = str(
            self.state.zero_fill_canceled_entry_order_id or ''
        ).strip()
        order_ids_requiring_lookup = set(expected_ids)
        if not callable(
            getattr(self.exchange, 'fetch_order_authoritative', None)
            or getattr(self.exchange, 'fetch_order', None)
        ):
            print("⚠️ 交易所不支持按订单核对撤单终态，禁止立即重挂")
            return None
        for order_id in sorted(order_ids_requiring_lookup):
            try:
                order = self._fetch_authoritative_order(order_id)
            except Exception as exc:
                if order_id == zero_fill_canceled_order_id:
                    # The persisted proof includes an explicit zero-fill cancel
                    # response, disappearance from open orders and a complete trade
                    # window. Gate may remove that terminal order from both views.
                    continue
                print(f"⚠️ 无法确认被撤订单 {order_id} 的最终状态，禁止立即重挂: {exc}")
                return None
            execution_order_id = self._entry_execution_order_id(order)
            if execution_order_id:
                for trade in trades:
                    trade_info = trade.get('info') or {}
                    trade_order_id = str(
                        trade.get('order')
                        or trade.get('orderId')
                        or trade_info.get('orderId')
                        or ''
                    ).strip()
                    if trade_order_id == execution_order_id and self._safe_float(
                        trade.get(
                            'amount',
                            trade_info.get('size', trade_info.get('baseVolume', 0.0)),
                        ),
                        0.0,
                    ) > 0:
                        return execution_order_id
                try:
                    execution_order = self._fetch_authoritative_order(
                        execution_order_id
                    )
                except Exception as exc:
                    print(
                        f"⚠️ Gate 条件单 {order_id} 已生成子订单 "
                        f"{execution_order_id}，但无法确认子订单终态: {exc}"
                    )
                    return None
                execution_status = str(
                    execution_order.get('status') or ''
                ).strip().lower()
                execution_filled = self._safe_float(
                    execution_order.get('filled', 0.0),
                    0.0,
                )
                if execution_filled > 0 or execution_status in {'closed', 'filled'}:
                    return execution_order_id
                if execution_status not in {
                    'canceled', 'cancelled', 'expired', 'rejected'
                }:
                    return None
                continue
            status = str(order.get('status') or '').strip().lower()
            filled = self._safe_float(order.get('filled', 0.0), 0.0)
            if str(order.get('type') or '').strip().lower() == 'trigger':
                info = order.get('info') or {}
                raw_outcome = str(
                    info.get('finish_as')
                    or info.get('reason')
                    or info.get('status')
                    or ''
                ).strip().lower()
                if (
                    status in {'canceled', 'cancelled', 'expired', 'rejected'}
                    or raw_outcome in {
                        'canceled', 'cancelled', 'expired', 'failed', 'rejected'
                    }
                ):
                    continue
                # A closed auto parent without trade_id only describes the trigger
                # lifecycle; it does not prove either a fill or a zero-fill cancel.
                print(
                    f"⚠️ Gate 条件单 {order_id} 终态缺少子订单 ID，禁止立即重挂"
                )
                return None
            if filled > 0 or status in {'closed', 'filled'}:
                return order_id
            if status not in {'canceled', 'cancelled', 'expired', 'rejected'}:
                print(
                    f"⚠️ 被撤订单 {order_id} 终态不明确(status={status or 'unknown'})，"
                    "禁止立即重挂"
                )
                return None
        return ''

    def _ensure_verified_last_fill_price(
        self,
        position: Optional[Dict[str, Any]],
        *,
        force_refresh: bool = False,
        preferred_order_id: Optional[str] = None,
        preferred_client_oid: Optional[str] = None,
        expected_contract_delta: Optional[float] = None,
        previously_accounted_fill_amount: float = 0.0,
    ) -> bool:
        if not force_refresh and self._has_verified_last_fill_price():
            return True

        # 一旦掌握订单 ID，就只能接受该订单的成交。退回到“最近同方向成交”
        # 会把手工交易或其他策略订单误认成上一层成交，重新引入过近加仓风险。
        resolved_preferred_order_id = str(preferred_order_id or '').strip()
        preferred_client_id = str(preferred_client_oid or '').strip()
        if not resolved_preferred_order_id and preferred_client_id:
            try:
                resolved_preferred_order_id = (
                    self._resolve_order_id_from_client_oid(preferred_client_id) or ''
                )
            except Exception as exc:
                print(f"⚠️ 无法用 clientOid 反查成交订单 ID: {exc}")
        recovered = self._fetch_latest_entry_fill(
            position,
            preferred_order_id=resolved_preferred_order_id or None,
            preferred_client_oid=(
                preferred_client_id if not resolved_preferred_order_id else None
            ),
        )
        if recovered is None and resolved_preferred_order_id:
            # Gate records a triggered auto order under a parent ID, while fills
            # belong to the ordinary child order in info.trade_id. Resolve that
            # exact link before declaring the fill missing.
            try:
                trigger_order = self._fetch_authoritative_order(
                    resolved_preferred_order_id
                )
                ownership = self._authoritative_entry_order_ownership(
                    trigger_order,
                    resolved_preferred_order_id,
                )
                if ownership is False:
                    print("⚠️ 权威订单身份不属于机器人，拒绝采用其成交锚点")
                    trigger_order = None
                execution_order_id = self._entry_execution_order_id(trigger_order)
            except Exception:
                execution_order_id = ''
            if execution_order_id:
                parent_order_id = resolved_preferred_order_id
                self._promote_trigger_execution_order_id(
                    parent_order_id,
                    execution_order_id,
                )
                resolved_preferred_order_id = execution_order_id
                recovered = self._fetch_latest_entry_fill(
                    position,
                    preferred_order_id=execution_order_id,
                )
        if recovered is not None and resolved_preferred_order_id:
            try:
                authoritative_order = self._fetch_authoritative_order(
                    resolved_preferred_order_id
                )
            except Exception:
                # Some legacy adapters cannot retrieve completed orders after
                # the trade history has settled; retain the existing strict
                # order-id/side/amount evidence in that case.
                authoritative_order = None
            if authoritative_order is not None:
                ownership = self._authoritative_entry_order_ownership(
                    authoritative_order,
                    resolved_preferred_order_id,
                )
                if ownership is False:
                    print("⚠️ 权威订单身份不属于机器人，拒绝采用其成交锚点")
                    recovered = None
        if recovered is not None and expected_contract_delta is not None:
            expected_delta = self._safe_float(expected_contract_delta, 0.0)
            recovered_total = self._safe_float(recovered.get('amount'), 0.0)
            accounted_amount = max(
                self._safe_float(previously_accounted_fill_amount, 0.0),
                0.0,
            )
            recovered_delta = recovered_total - accounted_amount
            delta_tolerance = max(
                abs(expected_delta) * 1e-8,
                abs(recovered_total) * 1e-8,
                1e-9,
            )
            if (
                expected_delta < -delta_tolerance
                or recovered_delta < -delta_tolerance
                or abs(recovered_delta - expected_delta) > delta_tolerance
            ):
                print(
                    "⚠️ 仓位增量与目标订单真实成交量不一致，"
                    f"expected={expected_delta:.8f}, fillDelta={recovered_delta:.8f}；"
                    "拒绝晋层并等待人工/交易所状态核对"
                )
                recovered = None
        if recovered is not None:
            return self._store_verified_last_fill(recovered)

        invalidation_accounted_amount = 0.0
        if resolved_preferred_order_id:
            if (
                str(self.state.unresolved_fill_order_id or '').strip()
                == resolved_preferred_order_id
            ):
                invalidation_accounted_amount = self._safe_float(
                    self.state.unresolved_fill_accounted_amount,
                    0.0,
                )
            elif (
                str(self.state.last_fill_order_id or '').strip()
                == resolved_preferred_order_id
                and str(self.state.last_fill_price_source or '').strip().lower()
                == 'trade_history'
            ):
                invalidation_accounted_amount = self._safe_float(
                    self.state.last_fill_amount,
                    0.0,
                )
        with self.state_lock:
            self.state.last_fill_price = 0.0
            self.state.last_fill_time = ''
            self.state.last_fill_order_id = ''
            self.state.last_fill_amount = 0.0
            self.state.last_fill_price_source = ''
            self.state.unresolved_fill_order_id = resolved_preferred_order_id
            self.state.unresolved_fill_accounted_amount = max(
                invalidation_accounted_amount,
                0.0,
            )
        self._save_runtime_state()
        return False

    def _store_verified_last_fill(self, recovered: Dict[str, Any]) -> bool:
        price = self._safe_float(recovered.get('price'), 0.0)
        amount = self._safe_float(recovered.get('amount'), 0.0)
        order_id = str(recovered.get('order_id') or '').strip()
        if price <= 0 or amount <= 0 or not order_id:
            return False
        with self.state_lock:
            self.state.last_fill_price = price
            self.state.last_fill_time = (
                self._format_timestamp_ms(recovered.get('timestamp_ms')) or self._now_str()
            )
            self.state.last_fill_order_id = order_id
            self.state.last_fill_amount = amount
            self.state.last_fill_price_source = 'trade_history'
            self.state.unresolved_fill_order_id = ''
            self.state.unresolved_fill_accounted_amount = 0.0
            if self.state.zero_fill_canceled_entry_order_id == order_id:
                self.state.zero_fill_canceled_entry_order_id = ''
        self._save_runtime_state()
        print(
            f"✅ 已从成交记录确认末次加仓价: {price:.8f}"
            f" (orderId={order_id})"
        )
        return True

    def _recover_single_layer_position_fill(
        self,
        position: Optional[Dict[str, Any]],
    ) -> bool:
        """Strictly recover a lost first-layer fill without using position cost as an anchor.

        This path is intentionally limited to layer 1.  A deeper position contains
        several fills, so matching only the aggregate position cannot prove which
        order was the most recent layer.  The recovered price always comes from
        authoritative trade history; position entryPrice is only a consistency check.
        """
        if int(self.state.layer or 0) != 1 or not position:
            return False

        query_complete, recovered = self._query_latest_entry_fill(position)
        if not query_complete or recovered is None:
            return False

        position_info = position.get('info') or {}
        contracts = self._safe_float(
            position.get('contracts', position_info.get('contracts', position_info.get('size', 0.0))),
            0.0,
        )
        entry_price = self._safe_float(
            position.get('entryPrice', position_info.get('openPriceAvg', 0.0)),
            0.0,
        )
        fill_amount = self._safe_float(recovered.get('amount'), 0.0)
        fill_price = self._safe_float(recovered.get('price'), 0.0)
        fill_timestamp_ms = self._safe_float(recovered.get('timestamp_ms'), 0.0)
        order_id = str(recovered.get('order_id') or '').strip()
        position_open_ms = self._position_open_timestamp_ms(position)
        amount_tolerance = max(abs(contracts) * 1e-8, 1e-9)
        if (
            contracts <= 0
            or entry_price <= 0
            or fill_amount <= 0
            or fill_price <= 0
            or not order_id
            or abs(fill_amount - contracts) > amount_tolerance
        ):
            print("⚠️ 最近成交量无法与当前第1层仓位一一对应，拒绝自动恢复加仓锚点")
            return False
        if position_open_ms > 0 and fill_timestamp_ms + 5000.0 < position_open_ms:
            print("⚠️ 最近机器人成交早于当前仓位周期，拒绝把旧周期成交恢复为加仓锚点")
            return False

        try:
            step = self._safe_float((self._market().get('precision') or {}).get('price'), 0.0)
        except Exception:
            step = 0.0
        if step >= entry_price:
            step = 0.0
        price_tolerance = max(abs(entry_price) * 1e-7, step * 0.51, 1e-12)
        if abs(fill_price - entry_price) > price_tolerance:
            print("⚠️ 最近成交价与当前第1层仓位不一致，拒绝自动恢复加仓锚点")
            return False

        if not callable(
            getattr(self.exchange, 'fetch_order_authoritative', None)
            or getattr(self.exchange, 'fetch_order', None)
        ):
            print("⚠️ 交易所不支持按订单复核首仓来源，拒绝自动恢复加仓锚点")
            return False
        try:
            order = self._fetch_authoritative_order(order_id)
        except Exception as exc:
            print(f"⚠️ 无法复核首仓订单 {order_id}，拒绝自动恢复加仓锚点: {exc}")
            return False

        fetched_order_id, client_oid = self._entry_order_identity(order)
        order_info = order.get('info') or {}
        status = str(order.get('status') or order_info.get('status') or '').strip().lower()
        order_side = str(order.get('side') or order_info.get('side') or '').strip().lower()
        expected_side = 'sell' if str(position.get('side') or '').lower() == 'short' else 'buy'
        order_filled = self._safe_float(
            order.get('filled', order_info.get('filled', order_info.get('size', 0.0))),
            0.0,
        )
        reduce_only_value = order.get('reduceOnly', order_info.get('reduceOnly', False))
        is_reduce_only = (
            reduce_only_value is True
            or str(reduce_only_value).strip().lower() in {'1', 'true', 'yes'}
        )
        if (
            fetched_order_id != order_id
            or status not in {'closed', 'filled'}
            or order_side != expected_side
            or is_reduce_only
            or abs(order_filled - fill_amount) > amount_tolerance
            or not self._is_bot_entry_client_oid(client_oid)
        ):
            print("⚠️ 最近成交未通过机器人首仓订单身份校验，拒绝自动恢复加仓锚点")
            return False

        print("🔎 已通过成交、订单身份及第1层仓位三重校验，恢复真实首仓锚点")
        return self._store_verified_last_fill(recovered)

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
        # 层间距只能锚定到已确认的上一层实际成交价。持仓均价、现价和待成交价
        # 都不具备这个语义，尤其在价格反弹时会允许下一层越过上一层成交价。
        if not self._has_verified_last_fill_price():
            return 0.0
        return self._safe_float(self.state.last_fill_price, 0.0)

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
            return 0.0

        if side == 'long':
            return min(proposed_price, anchor_price * (1 - spacing_ratio))
        return max(proposed_price, anchor_price * (1 + spacing_ratio))

    def _is_add_order_plan_spacing_valid(self, plan: Dict[str, Any]) -> bool:
        if not self._has_verified_last_fill_price():
            return False
        anchor_price = self._safe_float(self.state.last_fill_price, 0.0)
        entry_price = self._safe_float(plan.get('execute_price', plan.get('entry_price', 0.0)), 0.0)
        spacing_ratio = max(self._safe_float(plan.get('spacing_ratio', 0.0), 0.0), 0.0)
        if anchor_price <= 0 or entry_price <= 0:
            return False
        tolerance = max(abs(anchor_price) * 1e-12, 1e-15)
        if str(plan.get('side') or self.state.position_side or '').lower() == 'short':
            return entry_price + tolerance >= anchor_price * (1 + spacing_ratio)
        return entry_price <= anchor_price * (1 - spacing_ratio) + tolerance

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
        if not position or position is self._POSITION_API_ERROR:
            print(f"⚠️ 无法确认真实持仓，跳过第{layer_num}层加仓计划")
            return None
        if layer_num > 1 and self._has_verified_last_fill_price():
            if not self._guard_verified_anchor_for_position(position):
                print(
                    f"🛑 第{max(layer_num - 1, 1)}层成交锚点不属于当前持仓周期，"
                    f"禁止生成第{layer_num}层加仓计划"
                )
                return None
        if layer_num > 1 and not self._has_verified_last_fill_price():
            exact_anchor_order_id = str(
                self.state.unresolved_fill_order_id
                or self.state.last_fill_order_id
                or ''
            ).strip()
            exact_anchor_client_oid = ''
            if (
                not exact_anchor_order_id
                and self.state.pending_layer <= self.state.layer
            ):
                exact_anchor_client_oid = str(
                    self.state.pending_entry_client_oid or ''
                ).strip()
            # 深层仓位无法从聚合持仓反推出“上一层”的唯一成交。没有精确订单
            # 身份时必须失败关闭，不能退回最近同向成交（可能是手工/其他策略）。
            if not exact_anchor_order_id and not exact_anchor_client_oid:
                print(
                    f"🛑 第{max(layer_num - 1, 1)}层缺少精确成交订单身份，"
                    f"禁止生成第{layer_num}层加仓计划"
                )
                return None
            if not self._ensure_verified_last_fill_price(
                position,
                preferred_order_id=exact_anchor_order_id or None,
                preferred_client_oid=exact_anchor_client_oid or None,
            ):
                print(
                    f"🛑 无法确认第{max(layer_num - 1, 1)}层的真实成交价，"
                    f"禁止生成第{layer_num}层加仓计划"
                )
                return None
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

        # 使用首仓时的余额基准计算仓位，避免中途充值导致加仓过大
        base_equity = self.state.initial_balance if self.state.initial_balance > 0 else equity

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
        spacing_ratio = self._gap_ratio(
            current_price=current_price,
            atr_value=atr_value,
            depth_scale=0.25,
            layer_num=layer_num,
            phase=phase,
            kind='spacing',
        )
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
        if entry_price <= 0:
            print(f"🛑 第{layer_num}层缺少可靠成交锚点，禁止下单")
            return None
        spacing_anchor_price = self._safe_float(self.state.last_fill_price, 0.0)
        entry_price = self._layer_price_to_safe_precision(
            entry_price,
            side,
            spacing_anchor_price,
            spacing_ratio,
        )

        layer_multipliers = phase_cfg['layer_multipliers']
        phase_layer_index = self._resolve_phase_layer_index(phase_cfg, layer_num)
        if phase_layer_index < 0 or phase_layer_index >= len(layer_multipliers):
            print(f"⚠️ 当前阶段 {phase} 未配置第{layer_num}层倍率，跳过加仓")
            return None
        desired_margin = base_equity * phase_cfg['first_order_ratio'] * layer_multipliers[phase_layer_index]
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
            'spacing_ratio': spacing_ratio,
            'spacing_anchor_price': spacing_anchor_price,
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

        existing_price = self._price_to_precision(self._safe_float(existing_order.get('price', 0.0), 0.0))
        if existing_price > 0 and existing_price != plan.get('execute_price'):
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
            # 与平仓流程共用同一把锁，确保“最终复核 -> 下单 -> 状态落盘”不会被
            # 全平流程穿插。action_lock 是 RLock，普通限价单可安全复入
            # _submit_entry_order()。
            with self.action_lock:
                if self._exit_in_progress.is_set():
                    print(f"⚠️ 第{layer_num}层加仓下单前检测到平仓流程进行中，跳过本次挂单")
                    return False
                if self.state.bot_state != "IN_STRATEGY" or self.state.layer <= 0:
                    print(
                        f"⚠️ 当前运行态不允许部署第{layer_num}层加仓单: "
                        f"bot_state={self.state.bot_state}, layer={self.state.layer}"
                    )
                    return False
                replaced_order_ids = [
                    str(order_id).strip()
                    for order_id in plan.get('replaced_entry_order_ids', [])
                    if str(order_id).strip()
                ]
                if replaced_order_ids:
                    boundary_fill_order_id: Optional[str] = ''
                    for attempt in range(3):
                        boundary_fill_order_id = self._entry_order_fill_barrier(
                            replaced_order_ids
                        )
                        if boundary_fill_order_id is None or boundary_fill_order_id:
                            break
                        if attempt < 2:
                            time.sleep(0.15)
                    if boundary_fill_order_id is None:
                        print("🛑 无法排除旧加仓单在撤单边界成交，拒绝立即重挂")
                        return False
                    if boundary_fill_order_id:
                        with self.state_lock:
                            self.state.last_fill_price = 0.0
                            self.state.last_fill_time = ''
                            self.state.last_fill_order_id = ''
                            self.state.last_fill_amount = 0.0
                            self.state.last_fill_price_source = ''
                            self.state.unresolved_fill_order_id = boundary_fill_order_id
                            self.state.unresolved_fill_accounted_amount = 0.0
                        self._save_runtime_state()
                        print(
                            f"🛑 旧加仓单 {boundary_fill_order_id} 在撤单边界已有成交，"
                            "已阻断重挂并等待仓位同步"
                        )
                        return False
                if not self._is_add_order_plan_spacing_valid(plan):
                    print(
                        f"🛑 第{layer_num}层最终委托价未满足相对真实末次成交价的层间距，拒绝下单"
                    )
                    return False
                active_position = self.get_active_position()
                if not active_position or active_position is self._POSITION_API_ERROR:
                    print(f"⚠️ 无法确认当前持仓，跳过部署第{layer_num}层加仓单")
                    return False
                expected_position = plan.get('position') or {}
                expected_contracts = self._safe_float(expected_position.get('contracts', 0.0), 0.0)
                active_contracts = self._safe_float(active_position.get('contracts', 0.0), 0.0)
                contract_tolerance = max(abs(expected_contracts) * 1e-8, 1e-9)
                expected_side = str(plan.get('side') or expected_position.get('side') or '').lower()
                active_side = str(active_position.get('side') or '').lower()
                if (
                    expected_contracts <= 0
                    or abs(active_contracts - expected_contracts) > contract_tolerance
                    or active_side != expected_side
                ):
                    print(
                        f"🛑 第{layer_num}层下单前持仓已变化 "
                        f"({expected_side} {expected_contracts} -> {active_side} {active_contracts})，"
                        "中止下单并等待重新同步成交"
                    )
                    return False
                latest_open_orders = self.fetch_open_orders()
                if latest_open_orders is None:
                    print(f"🛑 第{layer_num}层下单前无法复核挂单列表，拒绝下单")
                    return False
                active_entry_orders = [
                    order
                    for order in latest_open_orders
                    if not order.get('reduceOnly', False)
                ]
                if active_entry_orders:
                    print(
                        f"🛑 第{layer_num}层下单前又检测到 {len(active_entry_orders)} 个开仓挂单，"
                        "拒绝重复提交"
                    )
                    return False
                if plan['trigger_price'] <= 0:
                    order_response = self._submit_entry_order(
                        plan['order_side'],
                        plan['amount'],
                        plan['execute_price'],
                        f"第{layer_num}层",
                    )
                    if order_response is None:
                        return False
                    print(f"✅ 第{layer_num}层加仓单已直接挂出")
                else:
                    order_response = self._create_trigger_order_idempotent(
                        plan['order_side'],
                        plan['amount'],
                        plan['trigger_price'],
                        price=plan['execute_price'],
                        trigger_type=self.layer_trigger_type,
                        order_type='limit',
                    )
                    print(f"✅ 第{layer_num}层条件加仓单已挂出")
                order_id, client_oid = self._entry_order_identity(order_response)
                submitted_amount = self._safe_float(order_response.get('amount'), plan['amount'])
                if submitted_amount <= 0:
                    submitted_amount = plan['amount']
                with self.state_lock:
                    self.state.phase = plan['phase']
                    self.state.pending_layer = max(self.state.layer, min(layer_num, self.max_layers))
                    self.state.pending_entry_price = plan['execute_price']
                    self.state.pending_entry_amount = submitted_amount
                    self.state.pending_entry_order_id = order_id
                    self.state.pending_entry_client_oid = client_oid
                    self.state.zero_fill_canceled_entry_order_id = ''
                self._save_runtime_state()
            # 快照可能触发多次网络读取，不能占用交易锁阻塞紧急平仓。
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

    def _preserve_existing_entry_price_for_amount_rebuild(
        self,
        plan: Dict[str, Any],
        orders: List[Dict[str, Any]],
        current_price: float,
    ) -> Dict[str, Any]:
        if len(orders) != 1:
            return plan

        existing_order = orders[0]
        if str(existing_order.get('side', '')).lower() != str(plan.get('order_side', '')).lower():
            return plan

        existing_amount = self._normalize_amount(self._safe_float(existing_order.get('amount', 0.0), 0.0))
        if self._amount_delta_ratio(existing_amount, plan.get('amount', 0.0)) <= self.entry_amount_refresh_tolerance:
            return plan

        existing_price = self._price_to_precision(self._safe_float(existing_order.get('price', 0.0), 0.0))
        if existing_price <= 0:
            return plan

        side = str(plan.get('side') or self.state.position_side or '').lower()
        avg_price = self._safe_float(plan.get('avg_price', 0.0), 0.0)
        if side not in ('long', 'short'):
            return plan
        if not self._is_sane_pending_entry_price(existing_price):
            return plan
        if not self._is_structure_price_valid_for_side(side, current_price, avg_price, existing_price):
            return plan

        stabilized_plan = dict(plan)
        stabilized_plan['entry_price'] = existing_price
        stabilized_plan['execute_price'] = existing_price
        adjusted_amount = self._normalize_amount(
            (self._safe_float(plan.get('layer_margin', 0.0), 0.0) * self.leverage) / existing_price
        )
        if adjusted_amount <= 0:
            return plan
        stabilized_plan['amount'] = adjusted_amount
        stabilized_plan['sticky_existing_price_for_rebuild'] = True
        stabilized_plan['sticky_rebuild_reason'] = (
            f"数量需要重建，沿用旧挂单价 {existing_price:.2f}，"
            f"按旧价重算数量 {existing_amount} -> {adjusted_amount}"
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
        existing_plan = dict(plan)
        existing_plan['entry_price'] = existing_price
        existing_plan['execute_price'] = existing_price
        if not self._is_add_order_plan_spacing_valid(existing_plan):
            return (
                f"现有委托价 {existing_price:.2f} 未满足相对真实末次成交价的最小层间距"
            )
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

    def _cancel_response_confirms_zero_fill(
        self,
        target: Dict[str, Any],
        response: Dict[str, Any],
    ) -> bool:
        if not isinstance(response, dict):
            return False
        target_order_id, _ = self._entry_order_identity(target)
        response_order_id, _ = self._entry_order_identity(response)
        if not target_order_id or response_order_id != target_order_id:
            return False

        info = response.get('info') or {}
        status = str(response.get('status') or info.get('status') or '').strip().lower()
        finish_as = str(
            info.get('finish_as') or info.get('finishAs') or ''
        ).strip().lower()
        if status not in {'canceled', 'cancelled'} and finish_as not in {
            'canceled',
            'cancelled',
        }:
            return False

        if str(target.get('type') or '').strip().lower() == 'trigger':
            # A Gate auto order can create an ordinary child at the cancellation
            # boundary.  Only an explicit trade_id=0 response proves that no child
            # exists; absence of the parsed field is not enough.
            has_execution_field = (
                'trade_id' in info or 'trade_id_string' in info
            )
            if (
                not has_execution_field
                or self._entry_execution_order_id(response)
            ):
                return False

        target_amount = self._safe_float(target.get('amount', 0.0), 0.0)
        target_filled = self._safe_float(target.get('filled', 0.0), 0.0)
        tolerance = max(abs(target_amount) * 1e-8, 1e-9)
        if target_filled > tolerance:
            return False

        filled_is_known = response.get('filled') is not None
        response_filled = self._safe_float(response.get('filled'), 0.0)
        if not filled_is_known and info.get('filled') is not None:
            filled_is_known = True
            response_filled = self._safe_float(info.get('filled'), 0.0)

        raw_size = abs(self._safe_float(info.get('size'), 0.0))
        raw_left = abs(self._safe_float(info.get('left'), 0.0))
        if not filled_is_known and raw_size > 0 and info.get('left') is not None:
            filled_is_known = True
            response_filled = max(raw_size - raw_left, 0.0)
        if not filled_is_known or response_filled > tolerance:
            return False

        if response.get('remaining') is not None and target_amount > 0:
            response_remaining = self._safe_float(response.get('remaining'), 0.0)
            if response_remaining + tolerance < target_amount:
                return False
        elif raw_size > 0 and raw_left + tolerance < raw_size:
            return False
        return True

    def _cancel_entry_orders(self, orders: List[Dict[str, Any]]) -> bool:
        targets = [order for order in orders if not order.get('reduceOnly', False)]
        if not targets:
            return True
        target_order_ids = {
            self._entry_order_identity(order)[0]
            for order in targets
            if self._entry_order_identity(order)[0]
        }
        if len(target_order_ids) != len(targets):
            print("🛑 加仓挂单缺少唯一 orderId，无法安全确认定向撤单终态")
            return False
        cancel_fn = getattr(self.exchange, 'cancel_orders', None)
        if not callable(cancel_fn):
            print("⚠️ 交易所适配器不支持定向撤单，跳过加仓单重建")
            return False
        with self.action_lock:
            try:
                cancel_result = cancel_fn(targets, self.symbol)
            except Exception as e:
                print(f"⚠️ 定向撤销加仓挂单失败: {e}")
                return False
            cancel_responses = (
                list(cancel_result)
                if isinstance(cancel_result, (list, tuple))
                else [cancel_result]
                if isinstance(cancel_result, dict)
                else []
            )
            response_by_order_id = {
                self._entry_order_identity(response)[0]: response
                for response in cancel_responses
                if isinstance(response, dict) and self._entry_order_identity(response)[0]
            }
            zero_fill_response_ids = {
                order_id
                for order_id, target in (
                    (self._entry_order_identity(target)[0], target) for target in targets
                )
                if order_id
                and order_id in response_by_order_id
                and self._cancel_response_confirms_zero_fill(
                    target,
                    response_by_order_id[order_id],
                )
            }
            for attempt in range(1, 5):
                if attempt > 1:
                    time.sleep(0.15)
                open_orders = self.fetch_open_orders()
                if open_orders is None:
                    continue
                remaining_ids = {
                    self._entry_order_identity(order)[0]
                    for order in open_orders
                    if not order.get('reduceOnly', False)
                    and self._entry_order_identity(order)[0] in target_order_ids
                }
                if not remaining_ids:
                    verified_zero_fill_ids = set()
                    for target in targets:
                        order_id, _ = self._entry_order_identity(target)
                        if order_id not in zero_fill_response_ids:
                            continue
                        target_side = str(target.get('side') or '').strip().lower()
                        position_side = 'long' if target_side == 'buy' else 'short'
                        query_complete, recovered = self._query_latest_entry_fill(
                            {'side': position_side},
                            preferred_order_id=order_id,
                        )
                        if query_complete and recovered is None:
                            verified_zero_fill_ids.add(order_id)
                    tracked_order_id = str(
                        self.state.pending_entry_order_id
                        or self.state.unresolved_fill_order_id
                        or ''
                    ).strip()
                    proof_order_id = (
                        tracked_order_id
                        if tracked_order_id in verified_zero_fill_ids
                        else next(iter(verified_zero_fill_ids))
                        if len(verified_zero_fill_ids) == 1
                        else ''
                    )
                    if proof_order_id:
                        with self.state_lock:
                            self.state.zero_fill_canceled_entry_order_id = proof_order_id
                        self._save_runtime_state()
                        print(
                            f"🔒 已记录零成交撤单终态: orderId={proof_order_id}"
                        )
                    print(f"✅ 已确认撤销 {len(targets)} 个旧加仓挂单")
                    return True
            print(
                "🛑 撤单接口已返回，但旧加仓单仍未确认消失，禁止提交替代订单: "
                f"{sorted(target_order_ids)}"
            )
            return False

    def _retain_residual_fill_order(
        self,
        add_orders: List[Dict[str, Any]],
        current_phase: str,
        position: Optional[Dict[str, Any]],
    ) -> bool:
        foreign_orders = [
            order for order in add_orders
            if not order.get('reduceOnly', False)
            and not self._is_owned_entry_order(order)
        ]
        if foreign_orders:
            print("🛑 残余成交校准发现外来开仓单，禁止采用、撤销或重建")
            return False
        if self._has_verified_last_fill_price() and not self._guard_verified_anchor_for_position(
            position
        ):
            print("🛑 残余订单锚点早于当前持仓周期，执行自有订单定向撤销并隔离")
            if not self._cancel_entry_orders(add_orders):
                print("⚠️ 旧残余订单尚未确认撤销，保持隔离并禁止重建")
            return False
        if not self._has_verified_last_fill_price():
            return False
        last_fill_order_id = str(self.state.last_fill_order_id or '').strip()
        if not last_fill_order_id:
            return False
        residual_fill_orders = [
            order
            for order in add_orders
            if self._entry_order_identity(order)[0] == last_fill_order_id
        ]
        if not residual_fill_orders:
            return False

        extra_orders = [
            order
            for order in add_orders
            if self._entry_order_identity(order)[0] != last_fill_order_id
        ]
        if extra_orders:
            print(
                f"🛑 检测到残余成交单之外的 {len(extra_orders)} 个额外加仓单，"
                "立即定向撤销并暂停部署下一层"
            )
            if not self._cancel_entry_orders(extra_orders):
                print("⚠️ 额外加仓单尚未确认撤销，将继续阻断新加仓")

        residual_order = residual_fill_orders[0]
        residual_order_id, residual_client_oid = self._entry_order_identity(residual_order)
        residual_layer = max(self.state.layer, self.state.pending_layer)
        residual_price = self._pending_entry_price_from_orders(residual_fill_orders)
        residual_amount = self._normalize_amount(
            self._safe_float(residual_order.get('amount', 0.0), 0.0)
        )
        with self.state_lock:
            self.state.pending_layer = residual_layer
            self.state.pending_entry_price = residual_price
            self.state.pending_entry_amount = residual_amount
            self.state.pending_entry_order_id = residual_order_id
            self.state.pending_entry_client_oid = residual_client_oid
            self.state.last_phase = current_phase
        self._save_runtime_state()
        print(
            f"⏳ 第{residual_layer}层订单仍有未成交部分，"
            "继续等待同一订单完成，不提前部署下一层"
        )
        return True

    def _reconcile_active_entry_orders(
        self,
        add_orders: List[Dict[str, Any]],
        next_layer: int,
        current_price: float,
        position: Dict[str, Any],
        reason_prefix: str = "",
    ) -> bool:
        _, foreign_orders = self._split_owned_entry_orders(
            add_orders,
            position.get('side'),
        )
        if foreign_orders:
            print(
                f"🛑 {reason_prefix}检测到外来同向开仓单，禁止撤销或重建"
            )
            return False
        anchor_pending_price = self._pending_entry_price_from_orders(add_orders)
        plan = self._build_add_order_plan(
            next_layer,
            current_price,
            position=position,
            anchor_pending_price=anchor_pending_price,
        )
        if plan is None:
            print(f"🛑 {reason_prefix}无法确认安全层间距，撤销现有加仓挂单")
            self._cancel_entry_orders(add_orders)
            return False
        safe_plan = dict(plan)
        plan = self._stabilize_plan_amount_with_existing_entry(plan, add_orders)
        plan = self._stabilize_plan_with_existing_entry(plan, add_orders, current_price)
        plan = self._preserve_existing_entry_price_for_amount_rebuild(plan, add_orders, current_price)
        if not self._is_add_order_plan_spacing_valid(plan):
            print(
                f"⚠️ {reason_prefix}旧挂单价格不满足真实成交间距，忽略价格粘性并采用安全新价"
            )
            plan = safe_plan
        if not self._is_add_order_plan_spacing_valid(plan):
            print(f"🛑 {reason_prefix}第{next_layer}层计划未通过最终间距校验")
            self._cancel_entry_orders(add_orders)
            return False
        if plan.get('sticky_amount_reason'):
            print(f"🧷 {reason_prefix}第{next_layer}层加仓数量保持不动: {plan['sticky_amount_reason']}")
        if plan.get('sticky_existing_price'):
            print(f"🧷 {reason_prefix}第{next_layer}层加仓挂单保持不动: {plan['sticky_reason']}")
        if plan.get('sticky_existing_price_for_rebuild'):
            print(f"🧷 {reason_prefix}第{next_layer}层加仓重建沿用旧价: {plan['sticky_rebuild_reason']}")

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
        replaced_order_ids = [
            self._entry_order_identity(order)[0]
            for order in add_orders
            if self._entry_order_identity(order)[0]
        ]
        if not self._cancel_entry_orders(add_orders):
            return False
        replacement_plan = dict(plan)
        replacement_plan['replaced_entry_order_ids'] = replaced_order_ids
        return self._submit_add_order_plan(replacement_plan)

    def _reconcile_startup_entry_orders(self) -> None:
        ok, position = self.enforce_exchange_position_sync(reason="启动校准")
        if not ok or not position:
            return

        open_orders = self.fetch_open_orders()
        if open_orders is None:
            print("⚠️ 启动检查时无法获取挂单，跳过加仓单校验")
            return

        position_side = str(position.get('side', '')).lower()
        add_orders, foreign_orders = self._split_owned_entry_orders(
            open_orders,
            position_side,
        )
        if foreign_orders:
            self._mark_state_sync_required(
                "启动校准检测到外来同向开仓单"
            )
            print(
                "🛑 启动校准发现外来同向开仓单，保留其原状并暂停重建: "
                f"{[self._entry_order_identity(order)[0] or self._entry_order_identity(order)[1] for order in foreign_orders]}"
            )
            return
        if not add_orders:
            if self._has_verified_last_fill_price() and not self._guard_verified_anchor_for_position(
                position
            ):
                print("🛑 启动校准拒绝沿用早于当前持仓周期的成交锚点")
            return
        if self._retain_residual_fill_order(
            add_orders,
            self._current_phase(position),
            position,
        ):
            print("⏳ 启动检查: 最近成交订单仍有未成交部分，等待该层完成")
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

    def _dynamic_tp_values(
        self,
        adx: float = 0.0,
        volatility_pct: float = 0.0,
        layer: Optional[int] = None,
    ) -> Tuple[float, float]:
        """Return market-dynamic trailing values, tightened by confirmed layer."""
        effective_layer = max(int(layer if layer is not None else self.state.layer), 1)
        leverage = max(self._safe_float(getattr(self, 'leverage', 1.0), 1.0), 1.0)
        fee_rate = max(self._safe_float(getattr(self, 'fee_rate', 0.0005), 0.0005), 0.0)
        fee_floor = 2.0 * fee_rate * leverage
        activation_floor = max(
            self._safe_float(getattr(self, 'trailing_activation_min_pct', 0.005), 0.005),
            self._safe_float(getattr(self, 'protective_stop_min_profit_pct', 0.005), 0.005),
            fee_floor + 0.001,
        )
        activation_cap = max(
            self._safe_float(getattr(self, 'trailing_activation_max_pct', 0.01), 0.01),
            activation_floor,
        )
        base_threshold = max(fee_floor * 1.5, activation_floor)

        if adx > 30:
            adx_multiplier = 1.5
            market_drawdown_ratio = 0.25
        elif adx > 25:
            adx_multiplier = 1.2
            market_drawdown_ratio = 0.28
        elif adx > 20:
            adx_multiplier = 1.0
            market_drawdown_ratio = 0.31
        else:
            adx_multiplier = 0.7
            market_drawdown_ratio = 0.35

        if volatility_pct > 0.03:
            volatility_multiplier = 1.3
        elif volatility_pct > 0.02:
            volatility_multiplier = 1.1
        elif volatility_pct > 0.01:
            volatility_multiplier = 1.0
        else:
            volatility_multiplier = 0.8

        market_activation = min(
            max(base_threshold * adx_multiplier * volatility_multiplier, activation_floor),
            activation_cap,
        )
        activation_layer_multiplier = self._layer_schedule_value(
            getattr(self, 'trailing_activation_layer_multipliers', [1.0]),
            effective_layer,
            1.0,
        )
        # 只压缩高于交易成本地板的动态部分，避免深层目标低到连成本都覆盖不了。
        activate_pct = activation_floor + (
            (market_activation - activation_floor) * activation_layer_multiplier
        )

        drawdown_layer_multiplier = self._layer_schedule_value(
            getattr(self, 'trailing_drawdown_layer_multipliers', [1.0]),
            effective_layer,
            1.0,
        )
        trail_ratio = max(
            self._safe_float(getattr(self, 'trailing_drawdown_min_ratio', 0.10), 0.10),
            market_drawdown_ratio * drawdown_layer_multiplier,
        )
        return activate_pct, trail_ratio

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
        effective_side = (position_side or self.state.position_side or '').lower()
        effective_layer = max(int(layer if layer is not None else self.state.layer), 1)
        activate_pct, trail_ratio = self._dynamic_tp_values(
            adx,
            volatility_pct,
            layer=effective_layer,
        )

        window = min(30, len(df))
        recent_high = self._safe_float(df['high'].tail(window).max(), current_price)
        recent_low = self._safe_float(df['low'].tail(window).min(), current_price)

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
            'layer': effective_layer,
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

        try:
            context = self._build_risk_context(position_side=side, layer=layer)
        except Exception as exc:
            print(f"⚠️ 刷新移动止盈指标失败，继续使用分层回撤保护: {exc}")
            context = None
        if context is None:
            # 旧层级/旧方向的 ATR 线不能带到新仓位；调用方会用当前层级的
            # 激活阈值和回撤比例继续保护，只暂时跳过 ATR 条件。
            return self._risk_context if self._risk_context_key == key else None

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
            runtime_symbol = str(raw.get('symbol') or '').strip()
            if runtime_symbol and runtime_symbol != self.symbol:
                print(
                    f"⚠️ 运行态交易对与当前配置不一致: runtime={runtime_symbol}, config={self.symbol}。"
                    "将忽略旧运行态，等待按当前交易对重新同步。"
                )
                self.state = RuntimeState(symbol=self.symbol)
                self._save_runtime_state()
                return
            self.state = RuntimeState(
                symbol=runtime_symbol or self.symbol,
                layer=raw.get('layer', 0),
                pending_layer=raw.get('pending_layer', raw.get('layer', 0)),
                phase=raw.get('phase', 'PHASE1'),
                last_phase=raw.get('last_phase', raw.get('phase', 'PHASE1')),
                phase2_start_layer=raw.get('phase2_start_layer', 0),
                pending_entry_price=raw.get('pending_entry_price', 0.0),
                pending_entry_amount=raw.get('pending_entry_amount', 0.0),
                pending_entry_order_id=raw.get('pending_entry_order_id', ""),
                pending_entry_client_oid=raw.get('pending_entry_client_oid', ""),
                last_fill_price=raw.get('last_fill_price', 0.0),
                last_fill_time=raw.get('last_fill_time', ""),
                last_fill_order_id=raw.get('last_fill_order_id', ""),
                last_fill_amount=raw.get('last_fill_amount', 0.0),
                last_fill_price_source=raw.get('last_fill_price_source', ""),
                unresolved_fill_order_id=raw.get('unresolved_fill_order_id', ""),
                unresolved_fill_accounted_amount=raw.get(
                    'unresolved_fill_accounted_amount',
                    0.0,
                ),
                zero_fill_canceled_entry_order_id=raw.get(
                    'zero_fill_canceled_entry_order_id',
                    "",
                ),
                protective_stop_active=raw.get('protective_stop_active', False),
                protective_stop_order_id=raw.get('protective_stop_order_id', ""),
                protective_stop_client_oid=raw.get('protective_stop_client_oid', ""),
                protective_stop_price=raw.get('protective_stop_price', 0.0),
                best_profit_pct=raw.get('best_profit_pct', 0.0),
                active_trailing_drawdown_ratio=raw.get('active_trailing_drawdown_ratio', 0.0),
                position_side=raw.get('position_side'),
                last_known_contracts=raw.get('last_known_contracts', 0.0),
                bot_state=raw.get('bot_state', 'IDLE'),
                activated=raw.get('activated', False),
                entry_price=raw.get('entry_price', 0.0),
                initial_balance=raw.get('initial_balance', 0.0),
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
        save_lock = getattr(self, 'runtime_save_lock', None)
        if save_lock is None:
            # 兼容测试或旧式 __new__ 构造；正式实例会在 __init__ 中预先创建。
            save_lock = threading.Lock()
            self.runtime_save_lock = save_lock
        with save_lock:
            with self.state_lock:
                self.state.last_update = self._now_str()
                payload = asdict(self.state)
                payload['symbol'] = self.symbol
            self._save_json(self.runtime_file, payload)

    def _reset_state(self):
        with self.action_lock:
            if not self._clear_protective_stop(remote=True, force_all=True):
                if (
                    self.state.protective_stop_active
                    or self.state.protective_stop_order_id
                    or self.state.protective_stop_client_oid
                ):
                    print("🛑 保护止损尚未确认撤销，保留运行态并拒绝重置为 IDLE")
                    return False
            with self.state_lock:
                self.state = RuntimeState(symbol=self.symbol)
                self._last_risk_log_profit_pct = 0.0
                self._risk_context = None
                self._risk_context_at = 0.0
                self._risk_context_key = None
                self._last_risk_rest_position_at = 0.0
            self._save_runtime_state()
            return True

    def _has_orphan_entry_orders(self, open_orders: List[Dict[str, Any]]) -> bool:
        non_reduce_orders = [o for o in open_orders if not o.get('reduceOnly', False)]
        if not non_reduce_orders:
            return bool(open_orders)
        return (
            self.state.last_known_contracts > 0
            or self.state.layer > 1
            or self.state.pending_layer > 1
        )

    def _is_owned_exit_order(self, order: Optional[Dict[str, Any]]) -> bool:
        """Return whether an open exit/entry order may be canceled by this bot."""
        if self._is_owned_entry_order(order):
            return True
        order_id, client_oid = self._entry_order_identity(order)
        if (
            order_id
            and order_id == str(self.state.protective_stop_order_id or '').strip()
        ):
            return True
        if (
            client_oid
            and client_oid == str(self.state.protective_stop_client_oid or '').strip()
        ):
            return True
        return False

    def _cancel_owned_open_orders(
        self,
        orders: List[Dict[str, Any]],
    ) -> bool:
        """Cancel only explicitly owned orders and verify their disappearance."""
        targets = [order for order in orders if self._is_owned_exit_order(order)]
        if not targets:
            return True
        cancel_fn = getattr(self.exchange, 'cancel_orders', None)
        if not callable(cancel_fn):
            cancel_one = getattr(self.exchange, 'cancel_order', None)
            if not callable(cancel_one):
                return False
            cancel_fn = lambda rows, symbol: [
                cancel_one(
                    self._entry_order_identity(row)[0],
                    symbol,
                    {'trigger': str(row.get('type') or '').lower() == 'trigger'},
                )
                for row in rows
            ]
        try:
            cancel_fn(targets, self.symbol)
        except Exception as exc:
            print(f"⚠️ 取消机器人自有挂单失败，进入隔离: {exc}")
            return False
        target_ids = {
            self._entry_order_identity(order)[0]
            for order in targets
            if self._entry_order_identity(order)[0]
        }
        target_clients = {
            self._entry_order_identity(order)[1]
            for order in targets
            if self._entry_order_identity(order)[1]
        }
        if not target_ids and not target_clients:
            return False
        for _ in range(4):
            latest = self.fetch_open_orders()
            if latest is None:
                return False
            remaining = [
                order for order in latest
                if (
                    self._entry_order_identity(order)[0] in target_ids
                    or self._entry_order_identity(order)[1] in target_clients
                )
            ]
            if not remaining:
                return True
            time.sleep(0.15)
        return False

    def _finalize_full_exit(self, reason: str = "") -> bool:
        prefix = f"{reason} " if reason else ""
        orders_cleared = False
        for attempt in range(1, 4):
            open_orders = self.fetch_open_orders()
            if open_orders is None:
                print(f"⚠️ {prefix}全平后无法确认挂单状态，第 {attempt} 次清理未完成")
                continue
            if not open_orders:
                orders_cleared = True
                break
            foreign_orders = [
                order for order in open_orders
                if not self._is_owned_exit_order(order)
            ]
            owned_orders = [
                order for order in open_orders
                if self._is_owned_exit_order(order)
            ]
            if owned_orders and not self._cancel_owned_open_orders(owned_orders):
                print(f"🛑 {prefix}机器人自有挂单未确认撤销，保留运行态")
                self._write_live_snapshot(force=True, include_market=True)
                return False
            if foreign_orders:
                print(
                    f"🛑 {prefix}发现 {len(foreign_orders)} 个外来挂单，"
                    "不执行全撤并保留运行态"
                )
                self._write_live_snapshot(force=True, include_market=True)
                return False
            time.sleep(0.2)
            print(f"⚠️ {prefix}全平后仍有 {len(open_orders)} 个挂单残留，第 {attempt} 次重试撤单")

        if not orders_cleared:
            print(f"🛑 {prefix}挂单未确认清空，保留运行态与订单身份，禁止重置为 IDLE")
            self._write_live_snapshot(force=True, include_market=True)
            return False

        if not self._wait_for_position_close(0.0, timeout_sec=1.0):
            print(f"🛑 {prefix}未连续确认仓位为零，保留运行态等待重试")
            self._write_live_snapshot(force=True, include_market=True)
            return False

        if self._reset_state() is False:
            self._write_live_snapshot(force=True, include_market=True)
            return False
        self._write_live_snapshot(force=True, include_market=True)
        return True

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
            'trailing_activation_min_pct': self.trailing_activation_min_pct,
            'trailing_activation_max_pct': self.trailing_activation_max_pct,
            'trailing_drawdown_min_ratio': self.trailing_drawdown_min_ratio,
            'trailing_activation_layer_multipliers': self.trailing_activation_layer_multipliers,
            'trailing_drawdown_layer_multipliers': self.trailing_drawdown_layer_multipliers,
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
        fallback_activate_pct, fallback_trail_ratio = self._fallback_trailing_values_for_layer(self.state.layer)
        activate_pct = context['activate_pct'] if context else fallback_activate_pct
        trail_ratio = context['trail_ratio'] if context else fallback_trail_ratio
        active_trail_ratio = self._safe_float(self.state.active_trailing_drawdown_ratio, 0.0)
        if self.state.activated and active_trail_ratio > 0:
            trail_ratio = min(trail_ratio, active_trail_ratio)
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
                'dynamic_tp_layer': max(int(self.state.layer), 1),
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
                raise RuntimeError(f"Leverage setup failed; trading is blocked: {e}") from e
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

    def _new_client_order_id(self) -> str:
        # Gate clientOrderId/text 最多 28 个字符；t- 前缀同时符合 Gate 的自定义
        # text 约定。总长度 27，Bitget clientOid 也可直接接受。
        return f"t-martin-{uuid.uuid4().hex[:18]}"

    def _create_order_idempotent(
        self,
        order_type: str,
        side: str,
        amount: float,
        price: Optional[float] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        order_params = dict(params or {})
        client_oid = str(
            order_params.get('clientOrderId')
            or order_params.get('clientOid')
            or self._new_client_order_id()
        )
        if not order_params.get('clientOrderId') and not order_params.get('clientOid'):
            order_params['clientOrderId'] = client_oid
        response = self.exchange.create_order(
            self.symbol,
            order_type,
            side,
            amount,
            price,
            order_params,
        )
        normalized = dict(response) if isinstance(response, dict) else {'raw': response}
        normalized.setdefault('clientOrderId', client_oid)
        normalized.setdefault('amount', amount)
        return normalized

    def _create_trigger_order_idempotent(
        self,
        side: str,
        amount: float,
        trigger_price: float,
        *,
        price: Optional[float],
        trigger_type: str,
        order_type: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        order_params = dict(params or {})
        client_oid = str(
            order_params.get('clientOrderId')
            or order_params.get('clientOid')
            or self._new_client_order_id()
        )
        if not order_params.get('clientOrderId') and not order_params.get('clientOid'):
            order_params['clientOrderId'] = client_oid
        response = self.exchange.create_trigger_order(
            self.symbol,
            side,
            amount,
            trigger_price,
            price=price,
            trigger_type=trigger_type,
            order_type=order_type,
            params=order_params,
        )
        normalized = dict(response) if isinstance(response, dict) else {'raw': response}
        normalized.setdefault('clientOrderId', client_oid)
        normalized.setdefault('amount', amount)
        return normalized

    def _submit_entry_order(
        self,
        order_side: str,
        amount: float,
        entry_price: float,
        label: str,
        order_type: str = 'limit',
    ) -> Optional[Dict[str, Any]]:
        with self.action_lock:
            if self._exit_in_progress.is_set():
                print(f"⚠️ {label} 下单前检测到平仓流程进行中，跳过本次挂单")
                return None
            try:
                create_price = entry_price if order_type == 'limit' else None
                return self._create_order_idempotent(
                    order_type,
                    order_side,
                    amount,
                    create_price,
                )
            except Exception as e:
                if order_type == 'limit' and self._is_insufficient_balance_error(e):
                    retry_amount = self._calculate_retry_amount(amount, entry_price)
                    if retry_amount > 0 and retry_amount < amount:
                        print(
                            f"⚠️ {label} 下单时可用保证金不足，自动缩量重试: "
                            f"{amount:.6f} -> {retry_amount:.6f}"
                        )
                        return self._create_order_idempotent(
                            'limit',
                            order_side,
                            retry_amount,
                            entry_price,
                        )
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

    def _fetch_active_position(self, suppress_error: bool = False) -> Tuple[bool, Optional[Dict[str, Any]]]:
        try:
            positions = self.exchange.fetch_positions([self.symbol])
            self._clear_state_sync_required()
            for pos in positions:
                contracts = self._safe_float(pos.get('contracts', 0))
                if contracts > 0:
                    return True, pos
            return True, None
        except Exception as e:
            self._mark_state_sync_required(f"持仓查询失败: {e}")
            if not suppress_error:
                print(f"❌ 获取持仓失败: {e}")
            return False, None

    # Sentinel value to distinguish API errors from genuine "no position"
    _POSITION_API_ERROR = {"__api_error__": True}

    def get_active_position(self) -> Optional[Dict[str, Any]]:
        """Return position dict if active, None if no position, _POSITION_API_ERROR if API call failed."""
        ok, position = self._fetch_active_position()
        if not ok:
            return self._POSITION_API_ERROR
        return position

    def fetch_open_orders(self) -> Optional[List[Dict[str, Any]]]:
        try:
            return self.exchange.fetch_open_orders(self.symbol)
        except Exception as e:
            self._mark_state_sync_required(f"挂单查询失败: {e}")
            print(f"⚠️ 获取挂单失败: {e}")
            return None

    def _wrong_direction_entry_orders(
        self,
        position_side: Optional[str],
        open_orders: Optional[List[Dict[str, Any]]] = None,
    ) -> List[Dict[str, Any]]:
        side = str(position_side or '').lower()
        if side not in {'long', 'short'}:
            return []
        rows = open_orders if open_orders is not None else self.fetch_open_orders()
        if not rows:
            return []
        wrong_side = 'sell' if side == 'long' else 'buy'
        return [
            order for order in rows
            if not order.get('reduceOnly', False)
            and str(order.get('side', '')).lower() == wrong_side
            and self._is_owned_entry_order(order)
        ]

    def _cancel_wrong_direction_entry_orders(
        self,
        position_side: Optional[str],
        open_orders: Optional[List[Dict[str, Any]]] = None,
        reason: str = "",
    ) -> bool:
        wrong_orders = self._wrong_direction_entry_orders(position_side, open_orders=open_orders)
        if not wrong_orders:
            return True
        reason_prefix = f"{reason}: " if reason else ""
        print(
            f"⚠️ {reason_prefix}检测到 {len(wrong_orders)} 个与真实持仓方向冲突的加仓挂单，准备撤销"
        )
        if not self._cancel_entry_orders(wrong_orders):
            print(f"⚠️ {reason_prefix}撤销错误方向挂单失败")
            return False
        print(f"✅ {reason_prefix}已撤销错误方向挂单")
        return True

    def _prepare_position_side_change(
        self,
        previous_side: str,
        exchange_side: str,
        open_orders: List[Dict[str, Any]],
        reason: str = "",
    ) -> bool:
        """Quarantine a direction change until every old entry order is terminal.

        The old runtime identity must remain intact while cancellation is uncertain.
        Otherwise a still-live long add order can later reduce or reverse a newly
        observed short position (and vice versa) without the bot being able to track it.
        """
        reason_prefix = f"{reason}: " if reason else ""
        if not self._cancel_wrong_direction_entry_orders(
            exchange_side,
            open_orders=open_orders,
            reason=reason or "方向切换",
        ):
            self._mark_state_sync_required("方向切换时旧加仓挂单未确认撤销")
            print(
                f"🛑 {reason_prefix}旧方向加仓挂单尚未确认撤销，"
                "保留原订单身份并暂停策略同步"
            )
            return False

        tracked_order_id = str(
            self.state.pending_entry_order_id
            or self.state.unresolved_fill_order_id
            or ''
        ).strip()
        tracked_client_oid = str(self.state.pending_entry_client_oid or '').strip()
        if not tracked_order_id and not tracked_client_oid:
            return True

        resolution = self._resolve_missing_pending_entry_order(
            {'side': previous_side}
        )
        if resolution in {'canceled_zero_fill', 'known_terminal'}:
            self._clear_pending_entry_tracking(
                reset_pending_layer=False,
                clear_unresolved_order_id=tracked_order_id,
            )
            return True

        self._mark_state_sync_required(
            f"方向切换时旧订单终态不明确: {resolution}"
        )
        print(
            f"🛑 {reason_prefix}检测到持仓方向 {previous_side} -> {exchange_side}，"
            f"但旧订单 {tracked_order_id or tracked_client_oid} 的终态为 "
            f"{resolution}；保留身份并暂停，禁止丢单后继续加仓"
        )
        return False

    def _force_sync_position_side(
        self,
        position: Optional[Dict[str, Any]],
        reason: str = "",
        sync_contracts: bool = True,
    ) -> bool:
        if not position:
            return True

        exchange_side = str(position.get('side', '')).lower()
        if exchange_side not in {'long', 'short'}:
            return True

        changed = False
        with self.state_lock:
            if self.state.position_side != exchange_side:
                print(
                    f"⚠️ {reason or '持仓同步'}: runtime.position_side={self.state.position_side}, "
                    f"exchange.position_side={exchange_side}，已以交易所为准修正"
                )
                self.state.position_side = exchange_side
                # 方向变化代表旧策略周期身份已失效。旧 layer/pending_layer 绝不能
                # 带入新方向，否则会用旧方向的待成交层级错误晋层。
                self.state.layer = 0
                self.state.pending_layer = 0
                self.state.phase = 'PHASE1'
                self.state.last_phase = 'PHASE1'
                self.state.phase2_start_layer = 0
                self.state.initial_balance = 0.0
                self.state.best_profit_pct = 0.0
                self.state.active_trailing_drawdown_ratio = 0.0
                self.state.activated = False
                self.state.last_known_contracts = 0.0
                self.state.last_fill_price = 0.0
                self.state.last_fill_time = ''
                self.state.last_fill_order_id = ''
                self.state.last_fill_amount = 0.0
                self.state.last_fill_price_source = ''
                self.state.unresolved_fill_order_id = ''
                self.state.unresolved_fill_accounted_amount = 0.0
                self.state.pending_entry_price = 0.0
                self.state.pending_entry_amount = 0.0
                self.state.pending_entry_order_id = ''
                self.state.pending_entry_client_oid = ''
                self.state.zero_fill_canceled_entry_order_id = ''
                changed = True
            contracts = self._safe_float(position.get('contracts', 0))
            entry_price = self._safe_float(position.get('entryPrice', 0))
            if sync_contracts and abs(self.state.last_known_contracts - contracts) > 1e-9:
                self.state.last_known_contracts = contracts
                changed = True
            if entry_price > 0 and abs(self.state.entry_price - entry_price) > 1e-9:
                self.state.entry_price = entry_price
                changed = True
            if self.state.bot_state != "IN_STRATEGY":
                self.state.bot_state = "IN_STRATEGY"
                changed = True

        if changed:
            self._save_runtime_state()
        return changed

    def enforce_exchange_position_sync(
        self,
        reason: str = "",
        sync_contracts: bool = True,
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        ok, position = self._fetch_active_position()
        if not ok:
            print("⚠️ 无法确认真实持仓，保持当前运行态，下一轮再同步")
            self._write_live_snapshot(force=True, include_market=False)
            return False, None

        if position:
            exchange_side = str(position.get('side', '')).lower()
            previous_side = str(self.state.position_side or '').lower()
            side_changed = (
                previous_side in {'long', 'short'}
                and exchange_side in {'long', 'short'}
                and previous_side != exchange_side
            )
            open_orders = self.fetch_open_orders()
            if open_orders is None:
                if side_changed:
                    self._mark_state_sync_required(
                        "方向切换时无法确认旧加仓挂单列表"
                    )
                    print(
                        "🛑 检测到持仓方向变化，但挂单查询失败；"
                        "保留旧订单身份并暂停本轮"
                    )
                    return False, position
            elif side_changed:
                if not self._prepare_position_side_change(
                    previous_side,
                    exchange_side,
                    open_orders=open_orders,
                    reason=reason or "方向校准",
                ):
                    return False, position
            elif not self._cancel_wrong_direction_entry_orders(
                exchange_side,
                open_orders=open_orders,
                reason=reason or "方向校准",
            ):
                self._mark_state_sync_required("错误方向挂单未确认撤销")
                return False, position
        self._force_sync_position_side(
            position,
            reason=reason or "主循环校准",
            sync_contracts=sync_contracts,
        )
        return True, position

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
        """限价平仓，防止插针滑点。做空用ask+滑点买入平仓，做多用bid-滑点卖出平仓。"""
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

                # 获取当前价格
                current_price = self._live_price_from_ws(position, allow_rest=True)
                if current_price <= 0:
                    print("⚠️ 无法获取当前价格，回退到市价平仓")
                    response = self._create_order_idempotent(
                        'market', close_side, contracts, None,
                        {'reduceOnly': True}
                    )
                    print("✅ 市价平仓完成（回退）")
                    return response

                # 滑点保护：做空买入平仓用 ask 方向偏移，做多卖出平仓用 bid 方向偏移
                slippage_pct = self._safe_float(
                    self.config.get('close_slippage_pct', 0.001), 0.001
                )
                ticker = self.exchange.fetch_ticker(self.symbol)
                if close_side == 'buy':
                    # 做空平仓：用 ask 价格 + 滑点，确保能成交
                    base_price = self._safe_float(ticker.get('ask', current_price), current_price)
                    limit_price = base_price * (1 + slippage_pct)
                else:
                    # 做多平仓：用 bid 价格 - 滑点，确保能成交
                    base_price = self._safe_float(ticker.get('bid', current_price), current_price)
                    limit_price = base_price * (1 - slippage_pct)

                print(f"\n--- 限价平仓 {contracts} | 当前价={current_price:.2f} | 限价={limit_price:.2f} | 滑点={slippage_pct*100:.2f}% ---")
                response = self._create_order_idempotent(
                    'limit',
                    close_side,
                    contracts,
                    limit_price,
                    {'reduceOnly': True}
                )
                order_id = response.get('id') if response else None
                print(f"✅ 限价平仓单已挂出 (orderId={order_id})")

                safe_to_supplement, remaining = self._resolve_limit_close_remainder(
                    order_id,
                    contracts,
                    timeout_sec=5.0,
                )
                if not safe_to_supplement:
                    print("🛑 限价平仓终态不明确，不盲目追加市价单")
                    return response
                if remaining > 0:
                    latest_position = self.get_active_position()
                    if latest_position is self._POSITION_API_ERROR:
                        print("🛑 无法复核市价补单前仓位，不盲目追加")
                        return response
                    live_remaining = self._safe_float(
                        (latest_position or {}).get('contracts', 0.0),
                        0.0,
                    )
                    supplement = self._normalize_amount(min(remaining, live_remaining))
                    if supplement > 0:
                        response = self._create_order_idempotent(
                            'market', close_side, supplement, None,
                            {'reduceOnly': True}
                        )
                        print(f"✅ 市价补单完成: {supplement}")
                else:
                    print("✅ 限价平仓已完成，无需市价补单")
                return response
            except Exception as e:
                print(f"❌ 平仓失败: {e}")
                return None

    def _wait_for_order_terminal(
        self,
        order_id: str,
        timeout_sec: float = 5.0,
    ) -> Optional[Dict[str, Any]]:
        if not order_id:
            return None
        deadline = time.time() + timeout_sec
        latest: Optional[Dict[str, Any]] = None
        while time.time() < deadline:
            try:
                latest = self.exchange.fetch_order(order_id, self.symbol) or {}
                status = str(latest.get('status', '')).lower()
                if status in {
                    'closed', 'filled', 'canceled', 'cancelled', 'expired', 'rejected'
                }:
                    return latest
            except Exception:
                pass
            time.sleep(0.3)
        return latest

    def _wait_for_order_fill(self, order_id: str, timeout_sec: float = 5.0) -> bool:
        """Backward-compatible full-fill check."""
        order = self._wait_for_order_terminal(order_id, timeout_sec=timeout_sec)
        return str((order or {}).get('status') or '').lower() in {'closed', 'filled'}

    def _resolve_limit_close_remainder(
        self,
        order_id: str,
        requested_amount: float,
        timeout_sec: float = 5.0,
    ) -> Tuple[bool, float]:
        """Confirm a limit close terminal state and return its safe market remainder."""
        if not order_id:
            return False, 0.0
        requested = max(self._safe_float(requested_amount, 0.0), 0.0)
        snapshot = self._wait_for_order_terminal(order_id, timeout_sec=timeout_sec)
        observed_filled = self._safe_float((snapshot or {}).get('filled', 0.0), 0.0)
        status = str((snapshot or {}).get('status') or '').lower()
        if status in {'closed', 'filled'}:
            return True, 0.0

        if status not in {'canceled', 'cancelled', 'expired', 'rejected'}:
            try:
                self.exchange.cancel_order(order_id, self.symbol)
            except Exception as exc:
                print(f"⚠️ 限价减仓单 {order_id} 撤单响应不确定，继续查询终态: {exc}")
            final_snapshot = self._wait_for_order_terminal(order_id, timeout_sec=2.0)
            if final_snapshot:
                observed_filled = max(
                    observed_filled,
                    self._safe_float(final_snapshot.get('filled', 0.0), 0.0),
                )
                snapshot = final_snapshot
                status = str(final_snapshot.get('status') or '').lower()

        if status in {'closed', 'filled'}:
            return True, 0.0
        if status not in {'canceled', 'cancelled', 'expired', 'rejected'}:
            return False, 0.0

        observed_filled = min(max(observed_filled, 0.0), requested)
        remaining = self._normalize_amount(max(requested - observed_filled, 0.0))
        return True, remaining

    def _wait_for_position_close(self, _expected_contracts: float, timeout_sec: float = 3.0) -> bool:
        deadline = time.time() + max(timeout_sec, 0.5)
        empty_confirmations = 0
        while time.time() < deadline:
            latest = self.get_active_position()
            if latest is self._POSITION_API_ERROR:
                empty_confirmations = 0
                time.sleep(0.2)
                continue
            if not latest:
                empty_confirmations += 1
                if empty_confirmations >= 2:
                    return True
                time.sleep(0.2)
                continue
            remaining = self._safe_float(latest.get('contracts', 0.0), 0.0)
            if remaining <= 0:
                empty_confirmations += 1
                if empty_confirmations >= 2:
                    return True
            else:
                empty_confirmations = 0
            time.sleep(0.2)
        return False

    def _execute_exit_pipeline(self, reason: str, position: Optional[Dict[str, Any]] = None) -> bool:
        if self._exit_in_progress.is_set():
            return False

        with self.action_lock:
            if self._exit_in_progress.is_set():
                return False

            self._exit_in_progress.set()
            try:
                print(f"🎯 {reason}")
                self._clear_protective_stop(remote=True, force_all=True)
                open_orders = self.fetch_open_orders()
                if open_orders is None:
                    print("🛑 平仓前无法读取挂单，拒绝全撤并进入隔离")
                    self._write_live_snapshot(force=True, include_market=True)
                    return False
                owned_orders = [
                    order for order in open_orders
                    if self._is_owned_exit_order(order)
                ]
                foreign_orders = [
                    order for order in open_orders
                    if not self._is_owned_exit_order(order)
                ]
                if owned_orders and not self._cancel_owned_open_orders(owned_orders):
                    print("🛑 机器人自有挂单未确认撤销，拒绝继续平仓并进入隔离")
                    self._write_live_snapshot(force=True, include_market=True)
                    return False
                if foreign_orders:
                    print(
                        "🛑 平仓前发现外来挂单，保留其原状并进入隔离，"
                        "不执行全撤回退"
                    )
                    self._write_live_snapshot(force=True, include_market=True)
                    return False

                # 必须在拿到 action_lock 且完成撤单后重取仓位。调用方传入的快照
                # 可能在等待锁期间因加仓成交而过期。
                latest_position = self.get_active_position()
                if latest_position is self._POSITION_API_ERROR:
                    live_position = (
                        position
                        if position and position is not self._POSITION_API_ERROR
                        else None
                    )
                    if live_position:
                        print("⚠️ 平仓前实时仓位查询失败，暂按调用方快照下单；不会据此确认全平")
                    else:
                        print("🛑 平仓前无法获取任何可信仓位快照，本次不清除运行态")
                        return False
                else:
                    live_position = latest_position

                if not live_position:
                    return self._finalize_full_exit(reason="未检测到持仓")

                close_response = self.close_position(live_position)
                closed = self._wait_for_position_close(self._safe_float(live_position.get('contracts', 0.0), 0.0))
                if closed:
                    closed = self._finalize_full_exit(reason="平仓完成后")
                elif close_response:
                    print("⚠️ 平仓单已提交，但短时间内未确认平仓，跳过全撤单以免撤掉减仓单")
                    self.sync_state_with_exchange()
                    self._write_live_snapshot(force=True, include_market=True)
                else:
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
        last_fill_price = (
            self._safe_float(self.state.last_fill_price, 0.0)
            if self._has_verified_last_fill_price()
            else 0.0
        )
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
            return self._fallback_trailing_values_for_layer(self.state.layer)
        print(
            f"📊 动态移动止盈: 第{context['layer']}层 ADX={context['adx']:.1f} "
            f"波动率={context['volatility_pct']*100:.2f}% "
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
        fallback_activate_pct, fallback_trail_ratio = self._fallback_trailing_values_for_layer(self.state.layer)
        activate_pct = context['activate_pct'] if context else fallback_activate_pct
        trail_ratio = context['trail_ratio'] if context else fallback_trail_ratio
        best_profit_pct = self.state.best_profit_pct
        if not self.state.activated:
            if best_profit_pct < activate_pct:
                return
            if self._set_runtime_flag('activated', True):
                print(
                    f"⚡ WS移动止盈已激活: 第{max(self.state.layer, 1)}层 "
                    f"{best_profit_pct*100:.2f}% >= {activate_pct*100:.2f}%"
                )

        # activated 是当前持仓周期内的粘性状态。即使后续指标上下文缺失、
        # 配置阈值变化或利润回落，也必须继续执行回撤保护。
        trail_ratio = self._tighten_active_trail_ratio(trail_ratio)
        self._arm_protective_stop(
            position,
            current_profit_pct=current_profit_pct,
            best_profit_pct=best_profit_pct,
            trail_ratio=trail_ratio,
            reason="WS移动止盈激活",
        )

        current_price = live_price or self._safe_float(
            position.get('markPrice', 0),
            self._safe_float((context or {}).get('current_price', 0), 0.0),
        )
        drawdown = best_profit_pct - current_profit_pct
        max_drawdown = trail_ratio * best_profit_pct

        if best_profit_pct > 0 and drawdown >= max_drawdown:
            print(
                f"🔴 WS回撤保护触发: 回撤 {drawdown*100:.2f}% "
                f">= {max_drawdown*100:.2f}%（当前收益 {current_profit_pct*100:.2f}%）"
            )
            self._execute_exit_pipeline("WS 回撤保护触发，执行总平仓", position)
            return

        if context is None:
            return
        if self.state.position_side == 'short':
            should_close = current_price > context['trail_price']
        else:
            should_close = current_price < context['trail_price']

        if should_close:
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
            fallback_activate_pct, fallback_trail_ratio = self._fallback_trailing_values_for_layer(self.state.layer)
            activate_pct = context['activate_pct'] if context else fallback_activate_pct
            trail_ratio = context['trail_ratio'] if context else fallback_trail_ratio
            if self.state.activated:
                trail_ratio = self._tighten_active_trail_ratio(trail_ratio)
            context_text = (
                f"ADX={context['adx']:.1f} 波动率={context['volatility_pct']*100:.2f}%"
                if context
                else "K线指标暂不可用"
            )
            print(
                f"📊 移动止盈: 第{max(self.state.layer, 1)}层 {context_text} "
                f"| 激活阈值={activate_pct*100:.2f}% 允许回撤={trail_ratio*100:.0f}%"
            )

            if not self.state.activated:
                if self.state.best_profit_pct < activate_pct:
                    print(
                        f"⏳ 等待激活: {self.state.best_profit_pct*100:.2f}% "
                        f"< {activate_pct*100:.2f}%"
                    )
                    return False
                if self._set_runtime_flag('activated', True):
                    print(
                        f"⚡ 移动止盈已永久激活（本持仓周期）: "
                        f"{self.state.best_profit_pct*100:.2f}% >= {activate_pct*100:.2f}%"
                    )
                trail_ratio = self._tighten_active_trail_ratio(trail_ratio)

            self._arm_protective_stop(
                position,
                current_profit_pct=current_profit_pct,
                best_profit_pct=self.state.best_profit_pct,
                trail_ratio=trail_ratio,
                reason="轮询移动止盈激活",
            )

            # 回撤保护
            if self.state.best_profit_pct > 0:
                drawdown = self.state.best_profit_pct - current_profit_pct
                max_drawdown = trail_ratio * self.state.best_profit_pct
                if drawdown >= max_drawdown:
                    print(
                        f"🔴 回撤保护触发: 回撤 {drawdown*100:.2f}% "
                        f">= {max_drawdown*100:.2f}%（当前收益 {current_profit_pct*100:.2f}%）"
                    )
                    return True

            if context is None:
                return False

            should_close = (
                current_price > context['trail_price']
                if self.state.position_side == 'short'
                else current_price < context['trail_price']
            )
            print(f"📊 {context['trail_desc']}, 当前价={current_price:.2f}")
            if should_close:
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
            with self.action_lock:
                if self._exit_in_progress.is_set():
                    print("⚠️ 首仓最终提交前检测到平仓流程，跳过下单")
                    return False
                active_position = self.get_active_position()
                if active_position is self._POSITION_API_ERROR:
                    print("⚠️ 首仓最终提交前无法确认仓位，跳过下单")
                    return False
                if active_position:
                    print("⚠️ 首仓最终提交前已检测到持仓，拒绝重复开仓")
                    return False
                latest_open_orders = self.fetch_open_orders()
                if latest_open_orders is None or latest_open_orders:
                    print("⚠️ 首仓最终提交前无法确认空挂单状态，拒绝重复开仓")
                    return False

                order_response = self._submit_entry_order(
                    order_side,
                    amount,
                    entry_price,
                    "首仓",
                    order_type=entry_type,
                )
                if order_response is None:
                    return False
                print("✅ 首仓挂单已挂出")
                order_id, client_oid = self._entry_order_identity(order_response)
                submitted_amount = self._safe_float(order_response.get('amount'), amount)
                if submitted_amount <= 0:
                    submitted_amount = amount

                with self.state_lock:
                    self.state.bot_state = "IN_STRATEGY"
                    self.state.position_side = trade_side.lower()
                    self.state.layer = 1
                    self.state.pending_layer = 1
                    self.state.initial_balance = equity
                    self.state.best_profit_pct = 0.0
                    self.state.active_trailing_drawdown_ratio = 0.0
                    self.state.activated = False
                    self.state.entry_price = entry_price
                    self.state.pending_entry_price = 0.0 if entry_type == 'market' else entry_price
                    self.state.pending_entry_amount = submitted_amount
                    self.state.pending_entry_order_id = order_id
                    self.state.pending_entry_client_oid = client_oid
                    self.state.last_fill_price = 0.0
                    self.state.last_fill_time = ""
                    self.state.last_fill_order_id = ""
                    self.state.last_fill_amount = 0.0
                    self.state.last_fill_price_source = ""
                    self.state.unresolved_fill_order_id = ""
                    self.state.unresolved_fill_accounted_amount = 0.0
                    self.state.zero_fill_canceled_entry_order_id = ""
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
        if self.state.bot_state != "IN_STRATEGY" or self.state.layer <= 0:
            print(
                f"⚠️ 当前运行态不允许补挂第{layer_num}层: "
                f"bot_state={self.state.bot_state}, layer={self.state.layer}"
            )
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
        if position is self._POSITION_API_ERROR:
            print(f"⚠️ 获取持仓API失败，无法补挂第{layer_num}层，下一轮再试")
            return False
        if not position:
            print(f"⚠️ 当前无持仓，跳过补挂第{layer_num}层")
            return False
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

    def _clear_pending_entry_tracking(
        self,
        reset_pending_layer: bool = True,
        clear_unresolved_order_id: Optional[str] = None,
    ) -> None:
        changed = False
        resolved_order_id = str(clear_unresolved_order_id or '').strip()
        with self.state_lock:
            if reset_pending_layer and self.state.pending_layer != self.state.layer:
                self.state.pending_layer = self.state.layer
                changed = True
            for field_name, empty_value in (
                ('pending_entry_price', 0.0),
                ('pending_entry_amount', 0.0),
                ('pending_entry_order_id', ''),
                ('pending_entry_client_oid', ''),
            ):
                if getattr(self.state, field_name) != empty_value:
                    setattr(self.state, field_name, empty_value)
                    changed = True
            if (
                resolved_order_id
                and str(self.state.unresolved_fill_order_id or '').strip() == resolved_order_id
            ):
                self.state.unresolved_fill_order_id = ''
                self.state.unresolved_fill_accounted_amount = 0.0
                changed = True
            if (
                resolved_order_id
                and str(self.state.zero_fill_canceled_entry_order_id or '').strip()
                == resolved_order_id
            ):
                self.state.zero_fill_canceled_entry_order_id = ''
                changed = True
        if changed:
            self._save_runtime_state()

    def _record_unresolved_entry_fill(
        self,
        order_id: str,
        recovered: Optional[Dict[str, Any]] = None,
    ) -> None:
        resolved_order_id = str(
            (recovered or {}).get('order_id') or order_id or ''
        ).strip()
        with self.state_lock:
            if (
                resolved_order_id
                and str(self.state.unresolved_fill_order_id or '').strip()
                == resolved_order_id
            ):
                accounted_amount = self._safe_float(
                    self.state.unresolved_fill_accounted_amount,
                    0.0,
                )
            elif (
                str(self.state.last_fill_order_id or '').strip() == resolved_order_id
                and str(self.state.last_fill_price_source or '').strip().lower()
                == 'trade_history'
            ):
                accounted_amount = self._safe_float(
                    self.state.last_fill_amount,
                    0.0,
                )
            else:
                accounted_amount = 0.0
            if recovered is not None:
                self.state.last_fill_price = self._safe_float(recovered.get('price'), 0.0)
                self.state.last_fill_time = (
                    self._format_timestamp_ms(recovered.get('timestamp_ms')) or self._now_str()
                )
                self.state.last_fill_order_id = resolved_order_id
                self.state.last_fill_amount = self._safe_float(recovered.get('amount'), 0.0)
                self.state.last_fill_price_source = 'trade_history'
            else:
                self.state.last_fill_price = 0.0
                self.state.last_fill_time = ''
                self.state.last_fill_order_id = ''
                self.state.last_fill_amount = 0.0
                self.state.last_fill_price_source = ''
            if resolved_order_id and not self.state.pending_entry_order_id:
                self.state.pending_entry_order_id = resolved_order_id
            self.state.unresolved_fill_order_id = resolved_order_id
            self.state.unresolved_fill_accounted_amount = max(accounted_amount, 0.0)
            if self.state.zero_fill_canceled_entry_order_id == resolved_order_id:
                self.state.zero_fill_canceled_entry_order_id = ''
        self._save_runtime_state()

    def _resolve_missing_pending_entry_order(
        self,
        position: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Classify a pending entry that vanished from the open-order view.

        Only a terminal cancellation plus an authoritative zero-fill query may release
        the identity for replacement. Every uncertain or newly filled state is sticky.
        """
        pending_order_id = str(self.state.pending_entry_order_id or '').strip()
        unresolved_order_id = str(self.state.unresolved_fill_order_id or '').strip()
        # pending 是“下一层订单”，unresolved 是“可能已成交但尚未完成同步的订单”。
        # 正常情况下二者相同；损坏 runtime 可能只剩 unresolved，因此也必须能
        # 对它完成零成交终态核对，避免永久阻断。
        order_id = pending_order_id or unresolved_order_id
        pending_client_oid = str(self.state.pending_entry_client_oid or '').strip()
        client_oid = (
            pending_client_oid
            if pending_order_id or not unresolved_order_id
            else ''
        )
        if not order_id and not client_oid:
            return 'none'
        if not order_id and client_oid:
            try:
                resolved_order_id = self._resolve_order_id_from_client_oid(client_oid)
            except Exception as exc:
                print(f"⚠️ 无法用 clientOid 反查待成交订单 ID: {exc}")
                resolved_order_id = None
            if resolved_order_id:
                order_id = resolved_order_id
                pending_order_id = resolved_order_id
                with self.state_lock:
                    self.state.pending_entry_order_id = resolved_order_id
                self._save_runtime_state()

        position_info = (position or {}).get('info') or {}
        side = str(
            (position or {}).get('side')
            or position_info.get('holdSide')
            or self.state.position_side
            or ''
        ).lower()
        query_complete = False
        recovered: Optional[Dict[str, Any]] = None
        if side in {'long', 'short'}:
            query_complete, recovered = self._query_latest_entry_fill(
                {'side': side},
                preferred_order_id=order_id or None,
                preferred_client_oid=client_oid if not order_id else None,
            )
        if not order_id and recovered is not None:
            order_id = str(recovered.get('order_id') or '').strip()

        if not order_id:
            print("⚠️ 待成交订单只有 clientOid，无法证明其已零成交取消，保留身份等待同步")
            return 'unknown'

        previous_verified_amount = (
            self._safe_float(self.state.last_fill_amount, 0.0)
            if str(self.state.last_fill_order_id or '') == order_id
            else 0.0
        )
        previous_accounted_amount = (
            self._safe_float(self.state.unresolved_fill_accounted_amount, 0.0)
            if str(self.state.unresolved_fill_order_id or '') == order_id
            else 0.0
        )
        previous_known_amount = max(
            previous_verified_amount,
            previous_accounted_amount,
            0.0,
        )
        recovered_amount = self._safe_float((recovered or {}).get('amount'), 0.0)
        amount_tolerance = max(abs(previous_known_amount) * 1e-8, 1e-12)
        pending_layer_unconfirmed = bool(
            pending_order_id
            and order_id == pending_order_id
            and self.state.pending_layer > self.state.layer
        )
        has_new_trade_fill = bool(
            recovered is not None
            and recovered_amount > previous_known_amount + amount_tolerance
        )
        if has_new_trade_fill:
            self._record_unresolved_entry_fill(order_id, recovered)
            return 'filled_wait_position'

        if not callable(
            getattr(self.exchange, 'fetch_order_authoritative', None)
            or getattr(self.exchange, 'fetch_order', None)
        ):
            print("⚠️ 交易所不支持按订单查询终态，保留待成交订单身份等待同步")
            return 'unknown'
        try:
            order = self._fetch_authoritative_order(order_id)
        except Exception as exc:
            if (
                query_complete
                and recovered is None
                and str(self.state.zero_fill_canceled_entry_order_id or '').strip()
                == order_id
            ):
                return 'canceled_zero_fill'
            print(f"⚠️ 无法确认待成交订单 {order_id} 的终态: {exc}")
            return 'unknown'

        if str(order.get('type') or '').strip().lower() == 'trigger':
            execution_order_id = self._entry_execution_order_id(order)
            if not execution_order_id:
                info = order.get('info') or {}
                raw_outcome = str(
                    info.get('finish_as')
                    or info.get('reason')
                    or info.get('status')
                    or ''
                ).strip().lower()
                status = str(order.get('status') or '').strip().lower()
                if status in {'open', 'new', 'live', 'partially_filled'}:
                    return 'open'
                canceled_outcomes = {
                    'canceled', 'cancelled', 'expired', 'failed', 'rejected'
                }
                if (
                    query_complete
                    and recovered is None
                    and raw_outcome in canceled_outcomes
                ):
                    return 'canceled_zero_fill'
                print(
                    f"⚠️ Gate 条件单 {order_id} 已结束但缺少触发子订单 ID，"
                    "无法判定为成交或零成交取消"
                )
                return 'unknown'

            parent_order_id = order_id
            self._promote_trigger_execution_order_id(
                parent_order_id,
                execution_order_id,
            )
            order_id = execution_order_id
            if pending_order_id == parent_order_id:
                pending_order_id = execution_order_id
            if side in {'long', 'short'}:
                query_complete, recovered = self._query_latest_entry_fill(
                    {'side': side},
                    preferred_order_id=execution_order_id,
                )
            previous_verified_amount = (
                self._safe_float(self.state.last_fill_amount, 0.0)
                if str(self.state.last_fill_order_id or '') == order_id
                else 0.0
            )
            previous_accounted_amount = (
                self._safe_float(
                    self.state.unresolved_fill_accounted_amount,
                    0.0,
                )
                if str(self.state.unresolved_fill_order_id or '') == order_id
                else 0.0
            )
            previous_known_amount = max(
                previous_verified_amount,
                previous_accounted_amount,
                0.0,
            )
            recovered_amount = self._safe_float(
                (recovered or {}).get('amount'),
                0.0,
            )
            amount_tolerance = max(
                abs(previous_known_amount) * 1e-8,
                1e-12,
            )
            pending_layer_unconfirmed = bool(
                pending_order_id
                and order_id == pending_order_id
                and self.state.pending_layer > self.state.layer
            )
            if (
                recovered is not None
                and recovered_amount > previous_known_amount + amount_tolerance
            ):
                self._record_unresolved_entry_fill(order_id, recovered)
                return 'filled_wait_position'
            try:
                order = self._fetch_authoritative_order(execution_order_id)
            except Exception as exc:
                print(
                    f"⚠️ Gate 条件单 {parent_order_id} 已生成子订单 "
                    f"{execution_order_id}，但无法确认子订单终态: {exc}"
                )
                return 'unknown'

        status = str(order.get('status') or '').lower()
        filled = self._safe_float(order.get('filled', 0.0), 0.0)
        if filled > previous_known_amount + amount_tolerance:
            self._record_unresolved_entry_fill(order_id, recovered)
            return 'filled_wait_position'
        if status in {'closed', 'filled'}:
            if (
                not pending_layer_unconfirmed
                and previous_known_amount > 0
                and (
                    str(self.state.last_fill_order_id or '') == order_id
                    or str(self.state.unresolved_fill_order_id or '') == order_id
                )
            ):
                return 'known_terminal'
            self._record_unresolved_entry_fill(order_id, recovered)
            return 'filled_wait_position'
        if status in {'canceled', 'cancelled', 'expired', 'rejected'}:
            if not query_complete:
                print(
                    f"⚠️ 订单 {order_id} 虽显示 {status}，但成交历史不完整，"
                    "禁止按零成交重挂"
                )
                return 'unknown'
            if previous_known_amount > 0:
                if (
                    not pending_layer_unconfirmed
                    and (
                        str(self.state.last_fill_order_id or '') == order_id
                        or str(self.state.unresolved_fill_order_id or '') == order_id
                    )
                ):
                    return 'known_terminal'
                self._record_unresolved_entry_fill(order_id, recovered)
                return 'filled_wait_position'
            return 'canceled_zero_fill'
        return 'open' if status in {'open', 'new', 'live', 'partially_filled'} else 'unknown'

    def _resolve_pending_entry_without_position(self) -> str:
        """Backward-compatible wrapper for first-entry eventual consistency."""
        return self._resolve_missing_pending_entry_order(position=None)

    def sync_state_with_exchange(self):
        position_ok, position = self._fetch_active_position()
        if not position_ok:
            print("⚠️ 持仓状态获取失败，保持当前运行态，稍后重试")
            self._write_live_snapshot(force=True, include_market=False)
            return

        open_orders = self.fetch_open_orders()
        orders_available = open_orders is not None
        if open_orders is None:
            open_orders = []
        non_reduce_orders = [o for o in open_orders if not o.get('reduceOnly', False)]

        if position:
            contracts = self._safe_float(position.get('contracts', 0))
            side = str(position.get('side', '')).lower()
            entry_price = self._safe_float(position.get('entryPrice', 0))
            previous_side = str(self.state.position_side or '').lower()
            side_changed = previous_side in {'long', 'short'} and previous_side != side
            previous_contracts = self._safe_float(self.state.last_known_contracts, 0.0)
            contracts_increased = side_changed or contracts > previous_contracts + 1e-9
            unresolved_fill_order_id = str(self.state.unresolved_fill_order_id or '').strip()
            pending_order_id = str(self.state.pending_entry_order_id or '').strip()
            recovery_order_id = str(
                # 仓位增加时，下一层 pending 身份比旧锚点的 unresolved 身份更具体。
                pending_order_id or unresolved_fill_order_id
            )
            recovery_client_oid = str(self.state.pending_entry_client_oid or '').strip()
            has_recovery_identity = bool(recovery_order_id or recovery_client_oid)
            recovery_pending_layer = int(self.state.pending_layer or 0)
            if orders_available:
                owned_entry_orders, foreign_entry_orders = self._split_owned_entry_orders(
                    open_orders,
                    side,
                )
                if foreign_entry_orders:
                    self._mark_state_sync_required(
                        "状态同步检测到外来同向开仓单"
                    )
                    print(
                        "🛑 检测到外来同向开仓单，保留其原状并暂停策略部署: "
                        f"{[self._entry_order_identity(order)[0] or self._entry_order_identity(order)[1] for order in foreign_entry_orders]}"
                    )
                    self._write_live_snapshot(force=True, include_market=True)
                    return
                non_reduce_orders = owned_entry_orders
            if side_changed:
                if not orders_available:
                    self._mark_state_sync_required(
                        "状态同步检测到方向变化，但无法确认旧加仓挂单"
                    )
                    print(
                        "🛑 持仓方向已变化，但挂单查询失败；"
                        "保留旧订单身份并暂停状态同步"
                    )
                    self._write_live_snapshot(force=True, include_market=True)
                    return
                if not self._prepare_position_side_change(
                    previous_side,
                    side,
                    open_orders,
                    reason="状态同步",
                ):
                    self._write_live_snapshot(force=True, include_market=True)
                    return
            elif orders_available and not self._cancel_wrong_direction_entry_orders(
                side,
                open_orders=open_orders,
                reason="状态同步",
            ):
                self._mark_state_sync_required("状态同步时错误方向挂单未确认撤销")
                self._write_live_snapshot(force=True, include_market=True)
                return
            # 成交价恢复成功前不能提前吞掉 contracts 增量，否则下一轮无法再推进 pending_layer。
            self._force_sync_position_side(
                position,
                reason="sync_state_with_exchange",
                sync_contracts=False,
            )
            if side_changed:
                unresolved_fill_order_id = ''
                pending_order_id = ''
                recovery_order_id = ''
                recovery_client_oid = ''
                has_recovery_identity = False
                recovery_pending_layer = 0
            if orders_available:
                expected_order_side = 'buy' if side == 'long' else 'sell'
                non_reduce_orders = [
                    o for o in open_orders
                    if not o.get('reduceOnly', False)
                    and str(o.get('side', '')).lower() == expected_order_side
                ]

            # 兼容旧版 BUG 已造成的损坏态：只剩一个零成交下一层的 unresolved ID，
            # 而正确的第1层锚点已被清空。Gate 对已取消普通单可能直接返回
            # ORDER_NOT_FOUND，因此不能单靠 fetch_order 证明取消；这里要求完整成交
            # 窗口中最新机器人首仓的成交量、方向、价格和当前第1层仓位全部吻合。
            # 若 unresolved 订单有任何成交，它会成为最新同向成交并无法通过该校验。
            if (
                orders_available
                and not non_reduce_orders
                and unresolved_fill_order_id
                and not pending_order_id
                and not contracts_increased
                and int(self.state.layer or 0) == 1
            ):
                if self._recover_single_layer_position_fill(position):
                    unresolved_fill_order_id = ''
                    recovery_order_id = ''
                    has_recovery_identity = False

            if (
                orders_available
                and not non_reduce_orders
                and has_recovery_identity
                and not contracts_increased
            ):
                resolving_order_id = pending_order_id or unresolved_fill_order_id
                pending_resolution = self._resolve_missing_pending_entry_order(position)
                if pending_resolution not in {'canceled_zero_fill', 'known_terminal'}:
                    print(
                        f"⏳ 待成交订单 {recovery_order_id or recovery_client_oid} 已从挂单列表消失，"
                        "但尚未确认零成交取消或仓位同步；保留层级和订单身份"
                    )
                    self._write_live_snapshot(force=True, include_market=True)
                    return
                self._clear_pending_entry_tracking(
                    reset_pending_layer=True,
                    clear_unresolved_order_id=resolving_order_id,
                )
                unresolved_fill_order_id = str(
                    self.state.unresolved_fill_order_id or ''
                ).strip()
                pending_order_id = str(self.state.pending_entry_order_id or '').strip()
                recovery_order_id = pending_order_id or unresolved_fill_order_id
                recovery_client_oid = ''
                has_recovery_identity = bool(recovery_order_id)
                recovery_pending_layer = self.state.layer

            self.state.bot_state = "IN_STRATEGY"
            self.state.position_side = side
            self.state.entry_price = entry_price

            price = entry_price or self._safe_float(position.get('markPrice', 0))
            if price <= 0:
                ticker = self.exchange.fetch_ticker(self.symbol)
                price = self._safe_float(ticker.get('last', 0))
            balance = self.get_wallet_balance() or 0

            # 优先使用 state.layer（runtime 中保存的真实值），避免用 contracts 反推导致误判
            if self.state.layer is not None and self.state.layer > 0:
                print(f"📌 使用 runtime 中的 layer={self.state.layer}")
                reference_balance = self._safe_float(self.state.initial_balance, 0.0) or balance
                estimated = self.estimate_current_layer(contracts, price, reference_balance)
                if (
                    estimated > self.state.layer
                    and not (
                        contracts_increased
                        and recovery_order_id
                        and recovery_pending_layer > self.state.layer
                    )
                ):
                    print(
                        f"⚠️ 检测到 runtime.layer={self.state.layer} 低于仓位规模估算层级 {estimated}，"
                        "已按只增不减原则修正"
                    )
                    self.state.layer = estimated
            else:
                estimated = self.estimate_current_layer(contracts, price, balance)
                print(f"⚠️ state.layer 缺失，回退到估算: {estimated}")
                self.state.layer = estimated
            self.state.phase = self._current_phase(position, current_price=price)
            self._repair_phase2_start_layer()
            if orders_available and non_reduce_orders:
                order_id, client_oid = self._entry_order_identity(non_reduce_orders[0])
                verified_anchor_order_id = str(
                    self.state.last_fill_order_id or ''
                ).strip()
                unresolved_order_id = str(
                    self.state.unresolved_fill_order_id or ''
                ).strip()
                if order_id and order_id == unresolved_order_id:
                    # The fill is known but its position delta is not fully visible
                    # yet. Preserve the original target layer instead of inventing
                    # another next layer from the still-open residual order.
                    self.state.pending_layer = max(
                        self.state.layer,
                        recovery_pending_layer,
                        self.state.pending_layer,
                    )
                elif (
                    order_id
                    and order_id == verified_anchor_order_id
                    and self._has_verified_last_fill_price()
                ):
                    # Same order still has a residual after a partial fill. It belongs
                    # to the current layer, not layer + 1.
                    self.state.pending_layer = self.state.layer
                else:
                    self.state.pending_layer = min(
                        self.max_layers,
                        self.state.layer + 1,
                    )
                self.state.pending_entry_price = self._pending_entry_price_from_orders(non_reduce_orders)
                self.state.pending_entry_amount = self._normalize_amount(
                    self._safe_float(non_reduce_orders[0].get('amount', 0.0), 0.0)
                )
                self.state.pending_entry_order_id = order_id
                self.state.pending_entry_client_oid = client_oid
                self.state.zero_fill_canceled_entry_order_id = ''
            elif orders_available and not has_recovery_identity:
                self.state.pending_layer = self.state.layer
                self.state.pending_entry_price = 0.0
                self.state.pending_entry_amount = 0.0
                self.state.pending_entry_order_id = ''
                self.state.pending_entry_client_oid = ''
                self.state.zero_fill_canceled_entry_order_id = ''
            elif not orders_available:
                print("⚠️ 挂单状态获取失败，本次仅同步持仓，不覆盖 pending 挂单状态")
            else:
                print("⏳ 挂单已消失但待成交身份仍在，本次不覆盖 pending 状态")

            # “上一层真实成交”和“下一层待成交订单”是两个独立身份：
            # - 仓位未增加时，只能复核 last_fill_order_id；绝不能用 pending ID
            #   查询上一层，否则零成交的下一层会清空正确锚点。
            # - 仓位确认增加后，才使用 pending/unresolved ID 晋升新的成交锚点。
            current_pending_order_id = str(self.state.pending_entry_order_id or '').strip()
            current_unresolved_order_id = str(
                self.state.unresolved_fill_order_id or ''
            ).strip()
            current_pending_client_oid = str(
                self.state.pending_entry_client_oid or ''
            ).strip()
            confirmed_anchor_order_id = str(self.state.last_fill_order_id or '').strip()
            preferred_fill_order_id = ''
            preferred_fill_client_oid = ''
            if contracts_increased and not side_changed:
                preferred_fill_order_id = current_pending_order_id or current_unresolved_order_id
                if not preferred_fill_order_id:
                    preferred_fill_client_oid = current_pending_client_oid
            elif not side_changed:
                preferred_fill_order_id = confirmed_anchor_order_id or current_unresolved_order_id

            if preferred_fill_order_id or preferred_fill_client_oid:
                previously_accounted_fill_amount = 0.0
                if (
                    preferred_fill_order_id
                    and current_unresolved_order_id == preferred_fill_order_id
                ):
                    previously_accounted_fill_amount = self._safe_float(
                        self.state.unresolved_fill_accounted_amount,
                        0.0,
                    )
                elif (
                    preferred_fill_order_id
                    and str(self.state.last_fill_order_id or '').strip()
                    == preferred_fill_order_id
                ):
                    previously_accounted_fill_amount = self._safe_float(
                        self.state.last_fill_amount,
                        0.0,
                    )
                fill_ready = self._ensure_verified_last_fill_price(
                    position,
                    force_refresh=True,
                    preferred_order_id=preferred_fill_order_id or None,
                    preferred_client_oid=preferred_fill_client_oid or None,
                    expected_contract_delta=(
                        contracts - previous_contracts
                        if contracts_increased and not side_changed
                        else 0.0
                        if (
                            preferred_fill_order_id
                            and (
                                preferred_fill_order_id == current_unresolved_order_id
                                or previously_accounted_fill_amount > 0
                            )
                        )
                        else None
                    ),
                    previously_accounted_fill_amount=previously_accounted_fill_amount,
                )
            elif int(self.state.layer or 0) == 1:
                # 仅用于修复已因旧版启动 BUG 丢失锚点的第1层状态。成交记录是
                # 价格来源；仓位均价只做一致性核验，绝不作为加仓价格兜底。
                fill_ready = self._recover_single_layer_position_fill(position)
            else:
                # 深层仓位没有精确订单身份时，最近同向成交可能来自手工交易或
                # 其他策略，不能自动成为末层锚点。
                with self.state_lock:
                    self.state.last_fill_price = 0.0
                    self.state.last_fill_time = ''
                    self.state.last_fill_order_id = ''
                    self.state.last_fill_amount = 0.0
                    self.state.last_fill_price_source = ''
                    self.state.unresolved_fill_accounted_amount = 0.0
                fill_ready = False
            if contracts_increased and fill_ready and recovery_pending_layer > self.state.layer:
                self.state.layer = recovery_pending_layer
                self.state.pending_layer = max(self.state.pending_layer, self.state.layer)
            if not contracts_increased or fill_ready:
                self.state.last_known_contracts = contracts
            if fill_ready and non_reduce_orders and self.state.last_fill_order_id:
                residual_order_ids = {
                    self._entry_order_identity(order)[0] for order in non_reduce_orders
                }
                if self.state.last_fill_order_id in residual_order_ids:
                    self.state.pending_layer = max(self.state.layer, recovery_pending_layer)
            if not fill_ready:
                print("🛑 无法从成交记录确认末次加仓价，暂停后续加仓")
                if orders_available and non_reduce_orders:
                    if self._cancel_entry_orders(non_reduce_orders):
                        print(
                            "✅ 已请求撤销缺少可靠间距锚点的加仓挂单；"
                            "保留订单身份，待确认零成交终态后再允许重建"
                        )
                    else:
                        print("⚠️ 无法撤销缺少可靠间距锚点的加仓挂单，请人工检查")
            self._save_runtime_state()
            self._write_live_snapshot(force=True, include_market=True)

            print(f"✅ 同步持仓成功: side={side}, contracts={contracts}, layer={self.state.layer}")
        else:
            if not orders_available:
                print("⚠️ 挂单状态获取失败，保持当前状态，稍后重试")
                self._write_live_snapshot(force=True, include_market=False)
                return
            owned_entry_orders, foreign_entry_orders = self._split_owned_entry_orders(
                open_orders,
                self.state.position_side,
            )
            if foreign_entry_orders:
                self._mark_state_sync_required(
                    "无仓状态检测到外来同向开仓单"
                )
                print(
                    "🛑 无仓状态检测到外来开仓单，保留其原状并暂停首仓部署: "
                    f"{[self._entry_order_identity(order)[0] or self._entry_order_identity(order)[1] for order in foreign_entry_orders]}"
                )
                self._write_live_snapshot(force=True, include_market=True)
                return
            non_reduce_orders = owned_entry_orders
            if not open_orders and (
                self.state.pending_entry_order_id or self.state.pending_entry_client_oid
            ):
                resolving_order_id = str(
                    self.state.pending_entry_order_id
                    or self.state.unresolved_fill_order_id
                    or ''
                ).strip()
                pending_resolution = self._resolve_pending_entry_without_position()
                if pending_resolution not in {'canceled_zero_fill', 'known_terminal'}:
                    print(
                        "⏳ 首仓挂单已从挂单列表消失，但尚未同时确认订单取消与空仓；"
                        "保留订单身份等待交易所最终一致"
                    )
                    self._write_live_snapshot(force=True, include_market=True)
                    return
                self._clear_pending_entry_tracking(
                    reset_pending_layer=True,
                    clear_unresolved_order_id=resolving_order_id,
                )
            if self._has_orphan_entry_orders(open_orders):
                print("⚠️ 检测到无持仓孤儿挂单，执行撤单并重置运行态")
                self._finalize_full_exit(reason="孤儿挂单清理")
                return
            if non_reduce_orders:
                entry_sides = [str(order.get('side') or '').lower() for order in non_reduce_orders]
                if (
                    len(non_reduce_orders) != 1
                    or any(side not in {'buy', 'sell'} for side in entry_sides)
                    or len(set(entry_sides)) != 1
                ):
                    print("🛑 无持仓时检测到多个或方向异常的开仓挂单，执行全撤并拒绝任选其一")
                    self._finalize_full_exit(reason="无仓异常开仓单清理")
                    return
                first_order = non_reduce_orders[0]
                self.state.bot_state = "IN_STRATEGY"
                self.state.position_side = 'long' if first_order['side'] == 'buy' else 'short'
                if self.state.layer == 0:
                    self.state.layer = 1
                self.state.pending_layer = max(self.state.pending_layer, self.state.layer)
                self.state.pending_entry_price = self._pending_entry_price_from_orders(non_reduce_orders)
                self.state.pending_entry_amount = self._normalize_amount(
                    self._safe_float(first_order.get('amount', 0.0), 0.0)
                )
                order_id, client_oid = self._entry_order_identity(first_order)
                self.state.pending_entry_order_id = order_id
                self.state.pending_entry_client_oid = client_oid
                self.state.zero_fill_canceled_entry_order_id = ''
                # 无持仓时这只能是新首仓周期；不得让旧周期的成交来源或 unresolved ID
                # 覆盖当前首仓订单身份。
                self.state.last_fill_price = 0.0
                self.state.last_fill_time = ''
                self.state.last_fill_order_id = ''
                self.state.last_fill_amount = 0.0
                self.state.last_fill_price_source = ''
                self.state.unresolved_fill_order_id = ''
                self.state.unresolved_fill_accounted_amount = 0.0
                self._save_runtime_state()
                self._write_live_snapshot(force=True, include_market=True)
                print(
                    f"✅ 同步挂单状态: {len(non_reduce_orders)} 个开仓挂单, "
                    f"side={self.state.position_side}"
                )
            else:
                has_strategy_cycle = bool(
                    self.state.bot_state == 'IN_STRATEGY'
                    or self.state.last_known_contracts > 0
                    or self.state.layer > 0
                    or self.state.initial_balance > 0
                    or self.state.last_fill_order_id
                )
                if has_strategy_cycle:
                    print("⏳ 检测到策略周期的一次空仓空单快照，连续复核后再重置")
                    self._finalize_full_exit(reason="状态同步确认周期结束")
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
            f"  动态移动止盈范围: {self.trailing_activation_min_pct*100:.2f}%"
            f" ~ {self.trailing_activation_max_pct*100:.2f}%"
        )
        print(
            "  激活层数系数: "
            + " / ".join(f"{value:.2f}" for value in self.trailing_activation_layer_multipliers)
        )
        print(
            "  回撤层数系数: "
            + " / ".join(f"{value:.2f}" for value in self.trailing_drawdown_layer_multipliers)
        )
        print(
            f"  阶段切换: 亏损达到 {self.phase_switch_loss_pct*100:.2f}% "
            f"或层数达到 {self.phase_switch_layer}"
        )
        print("=" * 60)

        self._bootstrap_exchange()
        _, startup_position = self.enforce_exchange_position_sync(reason="启动阶段")
        print(f"🧭 启动阶段: {self._current_phase(startup_position)}")
        self._reconcile_startup_entry_orders()
        self._set_runtime_flag('last_phase', self.state.phase)
        self._start_ws_risk_monitor()
        self._write_live_snapshot(force=True, include_market=True)

        try:
            while True:
                try:
                    sync_ok, position = self.enforce_exchange_position_sync(
                        reason="主循环开始",
                        sync_contracts=False,
                    )
                    if not sync_ok:
                        time.sleep(min(self.error_sleep, max(self.loop_interval, 5)))
                        continue

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
                                reference_price = self._safe_float(
                                    position.get('entryPrice', 0),
                                    self._safe_float(position.get('markPrice', 0), 0.0),
                                )
                                reference_balance = self._safe_float(self.state.initial_balance, 0.0)
                                if reference_balance <= 0:
                                    reference_balance = self.get_wallet_balance() or 0.0
                                if current_contracts > previous_contracts:
                                    added = current_contracts - previous_contracts
                                    if previous_contracts > 0:
                                        print(f"🎉 检测到加仓成交: +{added:.6f}")
                                    else:
                                        print(f"🎉 检测到新开仓: {current_contracts:.6f}")
                                else:
                                    reduced = previous_contracts - current_contracts
                                    print(f"🎉 检测到减仓成交: -{reduced:.6f}")

                                fill_ready = True
                                if current_contracts > previous_contracts:
                                    fill_order_id = str(
                                        self.state.pending_entry_order_id
                                        or self.state.unresolved_fill_order_id
                                        or ''
                                    ).strip()
                                    fill_client_oid = (
                                        str(self.state.pending_entry_client_oid or '').strip()
                                        if not fill_order_id
                                        else ''
                                    )
                                    accounted_fill_amount = 0.0
                                    if (
                                        fill_order_id
                                        and str(self.state.unresolved_fill_order_id or '').strip()
                                        == fill_order_id
                                    ):
                                        accounted_fill_amount = self._safe_float(
                                            self.state.unresolved_fill_accounted_amount,
                                            0.0,
                                        )
                                    elif (
                                        fill_order_id
                                        and str(self.state.last_fill_order_id or '').strip()
                                        == fill_order_id
                                    ):
                                        accounted_fill_amount = self._safe_float(
                                            self.state.last_fill_amount,
                                            0.0,
                                        )
                                    if fill_order_id or fill_client_oid:
                                        fill_ready = self._ensure_verified_last_fill_price(
                                            position,
                                            force_refresh=True,
                                            preferred_order_id=fill_order_id or None,
                                            preferred_client_oid=fill_client_oid or None,
                                            expected_contract_delta=(
                                                current_contracts - previous_contracts
                                            ),
                                            previously_accounted_fill_amount=(
                                                accounted_fill_amount
                                            ),
                                        )
                                    elif previous_contracts <= 0 and self.state.layer == 1:
                                        fill_ready = self._recover_single_layer_position_fill(position)
                                    else:
                                        fill_ready = False
                                    if not fill_ready:
                                        print(
                                            "🛑 仓位已增加，但无法确认对应的真实成交价；"
                                            "已失效旧锚点并暂停后续加仓"
                                        )

                                with self.state_lock:
                                    if current_contracts <= previous_contracts or fill_ready:
                                        self.state.last_known_contracts = current_contracts
                                    if (
                                        current_contracts > previous_contracts
                                        and fill_ready
                                        and self.state.pending_layer > self.state.layer
                                    ):
                                        self.state.layer = self.state.pending_layer
                                    elif (
                                        current_contracts > previous_contracts
                                        and fill_ready
                                        and reference_price > 0
                                        and reference_balance > 0
                                    ):
                                        estimated_layer = self.estimate_current_layer(
                                            current_contracts,
                                            reference_price,
                                            reference_balance,
                                        )
                                        if estimated_layer > self.state.layer:
                                            print(
                                                f"⚠️ pending_layer 未领先，按仓位规模兜底修正 layer: "
                                                f"{self.state.layer} -> {estimated_layer}"
                                            )
                                            self.state.layer = estimated_layer
                                            self.state.pending_layer = max(self.state.pending_layer, estimated_layer)
                                    if self.state.pending_layer < self.state.layer:
                                        self.state.pending_layer = self.state.layer
                                self._save_runtime_state()
                                if current_contracts > previous_contracts and not fill_ready:
                                    self._write_live_snapshot(force=True, include_market=True)
                                    time.sleep(self.loop_interval)
                                    continue

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
                                foreign_orders = [
                                    order for order in open_orders
                                    if not order.get('reduceOnly', False)
                                    and not self._is_owned_entry_order(order)
                                ]
                                if foreign_orders:
                                    self._mark_state_sync_required(
                                        "主循环检测到外来开仓单"
                                    )
                                    print(
                                        "🛑 主循环检测到外来开仓单，保留其原状并暂停部署: "
                                        f"{[self._entry_order_identity(order)[0] or self._entry_order_identity(order)[1] for order in foreign_orders]}"
                                    )
                                    self._write_live_snapshot(force=True, include_market=True)
                                    time.sleep(self.loop_interval)
                                    continue
                                add_orders = [o for o in open_orders if not o.get('reduceOnly', False)]
                                phase_changed = current_phase != str(self.state.last_phase or current_phase).upper()
                                if phase_changed:
                                    print(
                                        f"🧭 运行中阶段切换: {self.state.last_phase or 'UNKNOWN'} -> {current_phase}，"
                                        "检查并按新阶段参数重建加仓挂单"
                                    )
                                if self._retain_residual_fill_order(
                                    add_orders,
                                    current_phase,
                                    position,
                                ):
                                    self._write_live_snapshot(force=False, include_market=True)
                                    time.sleep(self.loop_interval)
                                    continue
                                if not add_orders and (
                                    self.state.pending_entry_order_id
                                    or self.state.pending_entry_client_oid
                                ):
                                    resolving_order_id = str(
                                        self.state.pending_entry_order_id
                                        or self.state.unresolved_fill_order_id
                                        or ''
                                    ).strip()
                                    pending_resolution = self._resolve_missing_pending_entry_order(position)
                                    if pending_resolution not in {
                                        'canceled_zero_fill',
                                        'known_terminal',
                                    }:
                                        print(
                                            "⏳ 待成交加仓单已从挂单列表消失，"
                                            "保留订单身份并等待成交/仓位最终一致"
                                        )
                                        self._write_live_snapshot(force=True, include_market=True)
                                        time.sleep(self.loop_interval)
                                        continue
                                    self._clear_pending_entry_tracking(
                                        reset_pending_layer=True,
                                        clear_unresolved_order_id=resolving_order_id,
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
                                        add_orders = [
                                            o for o in open_orders
                                            if not o.get('reduceOnly', False)
                                            and self._is_owned_entry_order(o)
                                        ]
                                    if phase_changed:
                                        self._set_runtime_flag('last_phase', current_phase)
                                    pending_price = self._pending_entry_price_from_orders(add_orders)
                                    if pending_price > 0 and abs(pending_price - self.state.pending_entry_price) > 1e-9:
                                        self._set_runtime_flag('pending_entry_price', pending_price)
                                    if add_orders:
                                        pending_amount = self._normalize_amount(self._safe_float(add_orders[0].get('amount', 0.0), 0.0))
                                        if abs(pending_amount - self.state.pending_entry_amount) > 1e-9:
                                            self._set_runtime_flag('pending_entry_amount', pending_amount)
                                        pending_order_id, pending_client_oid = self._entry_order_identity(add_orders[0])
                                        if pending_order_id != self.state.pending_entry_order_id:
                                            self._set_runtime_flag('pending_entry_order_id', pending_order_id)
                                        if pending_client_oid != self.state.pending_entry_client_oid:
                                            self._set_runtime_flag('pending_entry_client_oid', pending_client_oid)
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
                                open_orders = self.fetch_open_orders()
                                if open_orders is None:
                                    print("⚠️ 已达阶段上限，但无法确认残余挂单状态，保持订单身份等待重试")
                                    time.sleep(self.loop_interval)
                                    continue
                                foreign_orders = [
                                    order for order in open_orders
                                    if not order.get('reduceOnly', False)
                                    and not self._is_owned_entry_order(order)
                                ]
                                if foreign_orders:
                                    self._mark_state_sync_required(
                                        "主循环检测到外来开仓单"
                                    )
                                    print(
                                        "🛑 主循环检测到外来开仓单，保留其原状并暂停部署: "
                                        f"{[self._entry_order_identity(order)[0] or self._entry_order_identity(order)[1] for order in foreign_orders]}"
                                    )
                                    self._write_live_snapshot(force=True, include_market=True)
                                    time.sleep(self.loop_interval)
                                    continue
                                add_orders = [
                                    order for order in open_orders
                                    if not order.get('reduceOnly', False)
                                ]
                                if self._retain_residual_fill_order(
                                    add_orders,
                                    current_phase,
                                    position,
                                ):
                                    self._write_live_snapshot(force=False, include_market=True)
                                    time.sleep(self.loop_interval)
                                    continue

                                orders_cleared = not add_orders
                                if add_orders:
                                    print("⚠️ 已达阶段上限，撤销不属于当前部分成交层的额外加仓挂单")
                                    orders_cleared = self._cancel_entry_orders(add_orders)
                                    if orders_cleared:
                                        print(
                                            "⏳ 阶段上限挂单已请求撤销，保留订单身份，"
                                            "待确认零成交终态后再清理"
                                        )
                                elif self.state.pending_entry_order_id or self.state.pending_entry_client_oid:
                                    resolving_order_id = str(
                                        self.state.pending_entry_order_id
                                        or self.state.unresolved_fill_order_id
                                        or ''
                                    ).strip()
                                    pending_resolution = self._resolve_missing_pending_entry_order(position)
                                    if pending_resolution in {'canceled_zero_fill', 'known_terminal'}:
                                        self._clear_pending_entry_tracking(
                                            reset_pending_layer=True,
                                            clear_unresolved_order_id=resolving_order_id,
                                        )
                                    else:
                                        print("⏳ 阶段上限的末次订单终态尚不明确，继续保留身份")
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
                                # 二次确认：API可能短暂返回无持仓，再查一次避免误判
                                recheck = self.get_active_position()
                                if recheck:
                                    print(f"⚠️ API短暂返回无持仓，二次确认仍有仓位 {self._safe_float(recheck.get('contracts', 0)):.6f}，跳过重置")
                                    self._write_live_snapshot(force=True, include_market=True)
                                    time.sleep(self.loop_interval)
                                    continue
                                if (
                                    self.state.pending_entry_order_id
                                    or self.state.pending_entry_client_oid
                                ):
                                    self.sync_state_with_exchange()
                                    time.sleep(self.loop_interval)
                                    continue
                                print("✅ 检测到持仓已关闭，本轮结束")
                                self._finalize_full_exit(reason="主循环确认持仓关闭")
                                time.sleep(self.loop_interval)
                                continue

                            open_orders = self.fetch_open_orders()
                            if open_orders is None:
                                print("⚠️ 当前无法确认挂单状态，保持原状态，下一轮再试")
                                time.sleep(self.loop_interval)
                                continue
                            if not open_orders:
                                if (
                                    self.state.pending_entry_order_id
                                    or self.state.pending_entry_client_oid
                                ):
                                    self.sync_state_with_exchange()
                                    time.sleep(self.loop_interval)
                                    continue
                                if (
                                    self.state.bot_state == 'IN_STRATEGY'
                                    or self.state.layer > 0
                                    or self.state.initial_balance > 0
                                ):
                                    print("⏳ 无仓无单但仍有策略周期状态，连续确认后再回到 IDLE")
                                    self._finalize_full_exit(reason="主循环确认空仓空单")
                                else:
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
                                if self._finalize_full_exit(reason="首仓反向前清理"):
                                    self.place_first_order('short', price)

                            elif self.state.position_side == 'short' and signal == 'LONG':
                                print("⚠️ 原计划做空，但当前更适合反向做多，撤单重挂")
                                if self._finalize_full_exit(reason="首仓反向前清理"):
                                    self.place_first_order('long', price)

                            elif signal == "WAIT":
                                if self._is_pending_entry_signal_compatible(
                                    str(self.state.position_side or ''),
                                    trend_context,
                                ):
                                    print("📊 信号转为 stretched，但趋势方向仍兼容，保留首仓挂单继续等待")
                                else:
                                    print("📊 横盘/方向失效，取消挂单，等待更清晰信号")
                                    self._finalize_full_exit(reason="首仓信号失效清理")

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
