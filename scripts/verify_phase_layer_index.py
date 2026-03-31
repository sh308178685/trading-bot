#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
MODULE_PATH = ROOT / "scripts" / "martin-bot.py"


def load_martin_module():
    spec = importlib.util.spec_from_file_location("martin_bot_module", MODULE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"无法加载模块: {MODULE_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def build_bot(module, phase2_start_layer: int, phase1_max_layers: int = 3):
    bot = module.MartinBot.__new__(module.MartinBot)
    bot.phase1_max_layers = phase1_max_layers
    bot.state = module.RuntimeState(
        layer=max(phase2_start_layer - 1, 0),
        phase="PHASE2",
        phase2_start_layer=phase2_start_layer,
    )
    return bot


def validate_scenario(module, name: str, phase2_start_layer: int, expected_layers, expected_multipliers):
    bot = build_bot(module, phase2_start_layer=phase2_start_layer)
    phase_cfg = {
        "phase": "PHASE2",
        "layer_index_offset": bot.phase1_max_layers,
        "layer_multipliers": [2.4, 3.9, 5.3, 6.8, 8.4, 10.1],
    }

    actual_indexes = [
        bot._resolve_phase_layer_index(phase_cfg, layer_num)
        for layer_num in expected_layers
    ]
    actual_multipliers = [
        phase_cfg["layer_multipliers"][index]
        for index in actual_indexes
    ]

    expected_indexes = list(range(len(expected_layers)))
    if actual_indexes != expected_indexes:
        raise AssertionError(
            f"{name} 索引错误: expected={expected_indexes}, actual={actual_indexes}"
        )
    if actual_multipliers != expected_multipliers:
        raise AssertionError(
            f"{name} 倍率错误: expected={expected_multipliers}, actual={actual_multipliers}"
        )

    print(f"{name}:")
    for layer_num, index, multiplier in zip(expected_layers, actual_indexes, actual_multipliers):
        print(f"  layer {layer_num} -> index {index}, multiplier {multiplier}")


def main():
    module = load_martin_module()

    validate_scenario(
        module,
        name="正常路径",
        phase2_start_layer=4,
        expected_layers=[4, 5, 6, 7],
        expected_multipliers=[2.4, 3.9, 5.3, 6.8],
    )
    validate_scenario(
        module,
        name="提前切换路径",
        phase2_start_layer=2,
        expected_layers=[2, 3, 4, 5],
        expected_multipliers=[2.4, 3.9, 5.3, 6.8],
    )

    legacy_bot = module.MartinBot.__new__(module.MartinBot)
    legacy_bot.phase_switch_layer = 4
    legacy_bot.phase1_max_layers = 3
    legacy_bot.state = module.RuntimeState(
        layer=2,
        phase="PHASE2",
        phase2_start_layer=0,
    )
    repaired = legacy_bot._repair_phase2_start_layer()
    if not repaired or legacy_bot.state.phase2_start_layer != 2:
        raise AssertionError(
            "历史状态修复错误: "
            f"repaired={repaired}, phase2_start_layer={legacy_bot.state.phase2_start_layer}"
        )
    legacy_phase_cfg = {
        "phase": "PHASE2",
        "layer_index_offset": legacy_bot.phase1_max_layers,
        "layer_multipliers": [2.4, 3.9, 5.3, 6.8, 8.4, 10.1],
    }
    legacy_index = legacy_bot._resolve_phase_layer_index(legacy_phase_cfg, 3)
    if legacy_index != 1:
        raise AssertionError(f"历史状态索引错误: expected=1, actual={legacy_index}")
    print("历史状态修复:")
    print(
        f"  layer {legacy_bot.state.layer} -> "
        f"phase2_start_layer {legacy_bot.state.phase2_start_layer}, next index {legacy_index}"
    )

    print("验证通过")


if __name__ == "__main__":
    main()
