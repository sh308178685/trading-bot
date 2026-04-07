#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import sys
import time
import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

BOT_PATH = ROOT / 'scripts' / 'martin-bot.py'
SPEC = importlib.util.spec_from_file_location('martin_bot', BOT_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f'无法加载 MartinBot: {BOT_PATH}')
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
MartinBot = MODULE.MartinBot


def load_json(path: Path):
    if not path.exists():
        return {}
    with open(path, 'r', encoding='utf-8') as handle:
        return json.load(handle)


def summarize_local_state(runtime_data: dict, live_data: dict) -> None:
    runtime = runtime_data or {}
    live_runtime = (live_data or {}).get('runtime') or {}
    live_position = (live_data or {}).get('position') or {}
    live_orders = (live_data or {}).get('open_orders') or []

    print("=== 本地状态检查 ===")
    print(
        f"runtime.position_side={runtime.get('position_side')}, "
        f"runtime.layer={runtime.get('layer')}, runtime.bot_state={runtime.get('bot_state')}"
    )
    print(
        f"live.runtime.position_side={live_runtime.get('position_side')}, "
        f"live.position.side={live_position.get('side')}, "
        f"live.position.contracts={live_position.get('contracts')}"
    )
    print(f"live.open_orders={len(live_orders)}")
    for order in live_orders:
        print(
            f"  - id={order.get('id')} side={order.get('side')} type={order.get('type')} "
            f"price={order.get('price')} amount={order.get('amount')} reduceOnly={order.get('reduceOnly')}"
        )


def main() -> int:
    runtime_path = ROOT / 'data' / 'martin-runtime.json'
    live_path = ROOT / 'data' / 'martin-live.json'
    runtime_data = load_json(runtime_path)
    live_data = load_json(live_path)
    summarize_local_state(runtime_data, live_data)

    bot = MartinBot()
    ok, position = bot.enforce_exchange_position_sync(reason="临时修复脚本")
    if not ok:
        print("❌ 无法从交易所确认真实持仓，未执行撤单")
        return 2

    if not position:
        print("⚠️ 交易所当前无持仓，跳过错误方向挂单清理")
        bot.sync_state_with_exchange()
        return 0

    actual_side = str(position.get('side', '')).lower()
    print(
        f"=== 交易所实时状态 ===\n"
        f"position.side={actual_side}, contracts={position.get('contracts')}, "
        f"entryPrice={position.get('entryPrice')}"
    )

    open_orders = bot.fetch_open_orders()
    if open_orders is None:
        print("❌ 无法获取当前挂单，未执行撤单")
        return 3

    wrong_orders = bot._wrong_direction_entry_orders(actual_side, open_orders=open_orders)
    print(f"当前挂单数={len(open_orders)}, 错误方向挂单数={len(wrong_orders)}")
    for order in wrong_orders:
        print(
            f"  - wrong id={order.get('id')} side={order.get('side')} type={order.get('type')} "
            f"price={order.get('price')} amount={order.get('amount')}"
        )

    if wrong_orders:
        if not bot._cancel_wrong_direction_entry_orders(
            actual_side,
            open_orders=open_orders,
            reason="临时修复脚本",
        ):
            print("❌ 撤销错误方向挂单失败")
            return 4
        time.sleep(1.0)
    else:
        print("✅ 未发现错误方向挂单")

    bot.sync_state_with_exchange()
    remaining_orders = bot.fetch_open_orders() or []
    remaining_wrong_orders = bot._wrong_direction_entry_orders(actual_side, open_orders=remaining_orders)
    print(
        f"=== 修复结果 ===\n"
        f"remaining_open_orders={len(remaining_orders)}, "
        f"remaining_wrong_direction_orders={len(remaining_wrong_orders)}"
    )
    if remaining_wrong_orders:
        print("⚠️ 仍有错误方向挂单残留，需要人工复核")
        return 5
    print("✅ 错误方向挂单已清理完成")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
