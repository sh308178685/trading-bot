# `scripts/martin-bot.py` 交易逻辑文档

本文按策略真实运行顺序，完整梳理 `scripts/martin-bot.py` 的交易逻辑。读者默认已经理解基础概念，例如 EMA、RSI、ADX、ATR、限价单、条件单、止盈止损、马丁分层加仓。

这套策略本质上不是“纯趋势跟随”，也不是“纯抄底摸顶”，而是：

- 入场信号层面：同时支持趋势跟随和区间均值回归
- 持仓管理层面：使用分阶段马丁加仓
- 风控层面：同时有轮询风控和 WebSocket 实时风控两条线
- 出场层面：组合使用动态止盈、分批止盈、回撤保护、ATR 追踪和平仓保护止损

---

## 1. 运行时状态机

策略核心状态保存在 `RuntimeState` 中，关键字段如下：

- `bot_state`：`IDLE` / `IN_STRATEGY`
- `position_side`：当前策略方向，`long` 或 `short`
- `layer`：已成交确认的层数
- `pending_layer`：已挂出、等待成交的目标层数
- `phase`：当前阶段，`PHASE1` 或 `PHASE2`
- `last_phase`：上一轮主循环看到的阶段
- `phase2_start_layer`：如果因为亏损提前切到 `PHASE2`，记录 PHASE2 实际从第几层开始
- `pending_entry_price`：当前未成交加仓单/首仓单的委托价
- `last_fill_price`：最近一次实际成交价
- `best_profit_pct`：本轮持仓生命周期内的最高浮盈
- `partial_tp_1_done` / `partial_tp_2_done`：两档分批止盈是否已执行
- `activated`：移动止盈是否已经激活
- `protective_stop_*`：保护止损单是否挂出、挂单 ID、触发价等
- `last_known_contracts`：上次确认的持仓张数，用来判断是否刚成交、是否刚减仓

时间顺序上，状态流转是：

1. `IDLE`
2. 信号出现，挂首仓
3. 切到 `IN_STRATEGY`
4. 首仓成交后开始管理持仓
5. 按条件补下一层
6. 进入 `PHASE1` 或切到 `PHASE2`
7. 动态止盈 / 分批止盈 / 回撤保护 / ATR 追踪 / 止损
8. 全部平掉后撤单、清状态，回到 `IDLE`

---

## 2. 核心参数与默认值

下面只列交易逻辑里最关键的参数。默认值来自脚本内 `self.config.get(..., 默认值)`，与 `config/config.example.json` 基本一致。

### 2.1 市场与指标参数

- `symbol="ETH/USDT:USDT"`
- `leverage=3`
- `timeframe="5m"`
- `sr_timeframe="1h"`
- `trend_lookback=120`
- `fast_ema_period=20`
- `slow_ema_period=50`
- `rsi_period=14`
- `adx_period=14`
- `atr_period=14`
- `rsi_threshold=50`
- `adx_threshold=16.5`

### 2.2 信号参数

- `trend_follow_enabled=true`
- `trend_follow_adx_threshold=28`
- `mean_reversion_adx_max=20`
- `mean_reversion_long_rsi=35`
- `mean_reversion_short_rsi=65`
- `mean_reversion_entry_atr_ratio=0.35`
- `strong_trend_entry_enabled=true`
- `strong_trend_entry_adx=max(trend_follow_adx_threshold+4, adx_threshold+10)`，默认算出来是 `32`
- `strong_trend_entry_mode="marketable_limit"`
- `strong_trend_limit_offset_pct=0.0003`
- `strong_trend_pullback_atr_ratio=0.15`
- `strong_trend_max_pullback_pct=0.0015`
- `keep_pending_entry_on_stretched=true`

### 2.3 分阶段马丁参数

- `phase_switch_loss_pct=0.025`
- `phase_switch_layer=4`
- `phase1_max_layers=3`
- `phase1_first_order_ratio=0.025`
- `phase1_layer_multipliers=[1, 1.15, 1.35]`
- `phase1_layer_min_gap_pct=0.0`
- `phase1_layer_min_gap_atr_multiplier=0.35`
- `phase1_layer_trigger_base_pct=0.0`
- `phase1_layer_trigger_atr_multiplier=0.35`
- `phase2_extra_layers=6`
- `phase2_layer_multipliers=[1.7, 2.1, 2.6, 3.2, 3.9, 4.8]`
- `phase2_layer_min_gap_pct=0.010`
- `phase2_layer_min_gap_atr_multiplier=1.4`
- `phase2_layer_trigger_base_pct=0.012`
- `phase2_layer_trigger_atr_multiplier=1.5`

总层数：

- `max_layers = phase1_max_layers + phase2_extra_layers = 9`

### 2.4 结构位 / 加仓挂单参数

- `level_offset_pct=0.001`
- `add_layer_base_offset_pct=0.005`
- `structure_refresh_threshold=0.008`
- `structure_min_gap_pct=0.005`
- `structure_min_gap_atr_multiplier=0.8`
- `layer_min_gap_pct=0.01`，是旧兼容参数，默认回落到 `phase2_layer_min_gap_pct`
- `layer_min_gap_atr_multiplier=1.4`
- `layer_trigger_base_pct=0.012`
- `layer_trigger_atr_multiplier=1.5`
- `layer_trigger_type="mark_price"`
- `use_ema_structure=false`

### 2.5 风控与止盈止损参数

- `fee_rate=0.0005`
- `max_loss_pct=0.5`
- `protective_stop_enabled=true`
- `protective_stop_trigger_type="mark_price"`
- `protective_stop_execute_price=0.0`
- `protective_stop_profit_lock_ratio=0.25`
- `protective_stop_min_profit_pct=max(2*fee_rate*leverage+0.002, 0.005)`，默认杠杆 3 下等于 `0.005`
- `protective_stop_update_step_pct=0.003`
- `trailing_min_close_profit_pct=0.005`

### 2.6 资金与运行参数

- `loop_interval=20`
- `error_sleep=60`
- `min_balance=10.0`
- `order_margin_safety_ratio=0.95`
- `insufficient_balance_shrink_ratio=0.90`
- `ws_risk_monitor_enabled=true`
- `ws_risk_check_interval=0.25`
- `ws_risk_context_refresh_sec=5.0`
- `ws_risk_log_step_pct=0.25%`

---

## 3. `IDLE` 阶段：先判断是否该开仓

主循环里，如果 `bot_state == "IDLE"`，策略会做三件事：

1. 先检查交易所上是否已经有持仓
2. 如果有持仓，直接同步状态，转入 `IN_STRATEGY`
3. 如果没有持仓，再计算信号，决定是否挂首仓

也就是说，这个机器人不会盲目假设本地状态正确，而是先以交易所真实状态为准。

---

## 4. 信号判断逻辑

信号来自 `get_trend_context()`，底层核心是 `_entry_bias_from_row()`。

### 4.1 先算指标

策略取最近 `trend_lookback=120` 根 K 线，并计算：

- EMA 快线 `ema_fast`，周期 20
- EMA 慢线 `ema_slow`，周期 50
- RSI，周期 14
- ATR，周期 14
- ADX，周期 14

### 4.2 趋势跟踪信号

满足下面条件时，走趋势跟随逻辑：

- `trend_follow_enabled=true`
- `adx >= trend_follow_adx_threshold(28)`

然后再判断方向：

- `ema_fast > ema_slow` 且 `rsi > rsi_threshold(50)`，视为强势上涨，返回 `market_state="TREND_UP"`，`signal="LONG"`
- `ema_fast < ema_slow` 且 `rsi < rsi_threshold(50)`，视为强势下跌，返回 `market_state="TREND_DOWN"`，`signal="SHORT"`

这里要注意，脚本顶部注释里早期写的是“上涨趋势反向做空、下跌趋势反向做多”，但当前代码实际不是这样。当前实现是：

- 强上升趋势，做多
- 强下降趋势，做空

也就是当前版本首仓信号已经支持顺趋势入场。

### 4.3 均值回归信号

如果趋势不强，且：

- `adx <= mean_reversion_adx_max(20)`

则认为市场更偏区间震荡，开始找超跌/超涨：

- 做多条件：
  - `rsi <= mean_reversion_long_rsi(35)`
  - `current_price <= ema_fast - atr * mean_reversion_entry_atr_ratio`
  - 默认 ATR 带宽比例是 `0.35`
- 做空条件：
  - `rsi >= mean_reversion_short_rsi(65)`
  - `current_price >= ema_fast + atr * mean_reversion_entry_atr_ratio`

满足时统一返回：

- `market_state="RANGE_MEAN_REVERSION"`
- `signal="LONG"` 或 `signal="SHORT"`

### 4.4 强趋势但已经拉伸过度

如果趋势方向已经很明确，但又不满足可直接入场的条件，则返回：

- `BULLISH_BUT_STRETCHED + WAIT`
- `BEARISH_BUT_STRETCHED + WAIT`

这类状态的含义是：方向大概率没错，但位置不够舒服，先别追。

### 4.5 中性状态

都不满足时返回：

- `market_state="NEUTRAL"`
- `signal="WAIT"`

---

## 5. 首仓开仓逻辑

当 `IDLE` 阶段得到 `LONG` 或 `SHORT` 信号时，会调用 `place_first_order()`。

### 5.1 首仓前置检查

首仓下单前必须先过这些条件：

1. 当前不能处于平仓流程中，即 `_exit_in_progress` 不能为真
2. 账户权益 `equity` 不能低于 `min_balance(10 USDT)`
3. 当前不能已有任何挂单
4. 当前不能已有同方向未成交挂单

如果挂单状态获取失败，策略宁可跳过，也不冒险重复下单。

### 5.2 首仓资金大小

首仓目标保证金：

- `desired_margin = equity * first_order_ratio`
- 其中 `first_order_ratio` 实际等于 `phase1_first_order_ratio=0.025`

然后还要经过 `_calculate_order_margin()` 二次限制：

- 真实可用的“安全新增保证金”是 `safe_tradable_margin = tradable_margin * order_margin_safety_ratio`
- 默认安全系数 `order_margin_safety_ratio=0.95`
- 如果目标保证金超过这个安全额度，会自动缩小

### 5.3 首仓价格怎么决定

首仓价格由 `_build_first_entry_plan()` 决定，分三类。

#### 5.3.1 强趋势入场

如果 `_first_entry_should_use_aggressive_trend_plan()` 返回真，说明：

- `strong_trend_entry_enabled=true`
- 信号方向和开仓方向一致
- `adx >= strong_trend_entry_adx`，默认 `32`

这时走“强趋势入场”：

- 如果 `strong_trend_entry_mode="market"`：
  - 直接市价开仓
- 如果 `strong_trend_entry_mode="marketable_limit"`：
  - 做多：挂 `current_price * (1 + 0.0003)`
  - 做空：挂 `current_price * (1 - 0.0003)`

这个价格设计成非常接近当前价，本质上是“能快速成交的限价单”。

#### 5.3.2 普通结构位入场

如果不是强趋势追单，就会找支撑阻力。

支撑阻力来自 `find_support_resistance()`，来源包括：

- 最近一段时间的高低点
- Pivot 的 `R1/R2/R3` 和 `S1/S2/S3`
- 可选的 EMA20 上下偏移结构位
- Volume Profile 高成交量价位

做空首仓时：

- 取最近阻力位 `levels[0]`
- 预期挂单价是 `阻力位 * (1 - level_offset_pct)`
- 默认 `level_offset_pct=0.001`

做多首仓时：

- 取最近支撑位 `levels[0]`
- 预期挂单价是 `支撑位 * (1 + level_offset_pct)`

#### 5.3.3 结构位价格夹紧

为了避免挂得过远或过于激进，代码会根据趋势背景做“夹紧”。

如果做空且当前趋势信号仍是 `SHORT`：

- 允许略微追价，但不会超过 `current_price * (1 + allowed_pullback_pct)`

如果做多且当前趋势信号仍是 `LONG`：

- 允许略微追价，但不会低于 `current_price * (1 - allowed_pullback_pct)`

这里：

- `allowed_pullback_pct = max(level_offset_pct, min(strong_trend_max_pullback_pct, ATR/current_price * strong_trend_pullback_atr_ratio))`
- 默认上限是 `strong_trend_max_pullback_pct=0.0015`

#### 5.3.4 没有结构位时的首仓兜底

如果没有找到有效支撑/阻力：

- 做空：`current_price * (1 + level_offset_pct)`
- 做多：`current_price * (1 - level_offset_pct)`

### 5.4 首仓下单方式

最终首仓下单分两种：

- 强趋势模式可能是 `market`
- 其他情况基本是 `limit`

下单通过 `_submit_entry_order()` 执行。

如果限价下单报“余额不足”，会自动缩量重试一次：

- 新数量 = 原数量 * `insufficient_balance_shrink_ratio(0.90)`
- 同时不会超过安全保证金所允许的最大数量

### 5.5 首仓挂出后的状态更新

挂单成功后，策略立即把状态改为：

- `bot_state="IN_STRATEGY"`
- `position_side=trade_side`
- `layer=1`
- `pending_layer=1`
- `best_profit_pct=0`
- `partial_tp_1_done=false`
- `partial_tp_2_done=false`
- `activated=false`
- `pending_entry_price=委托价`，如果是市价则记为 `0`

注意：这里的 `layer=1` 更准确地说是“策略已经进入第一层计划”，并不等于首仓已经成交。真实成交还要靠后续合约数量变化来确认。

---

## 6. 支撑阻力与结构位选择逻辑

这一部分既服务首仓，也服务后续加仓。

### 6.1 支撑阻力来源

`find_support_resistance()` 会组合以下候选：

- 最近 `lookback` 周期内最高点 / 最低点
- Pivot：`R1/R2/R3/S1/S2/S3`
- 如果 `use_ema_structure=true`，加入 EMA20 上下 1% 偏移
- `Volume Profile` 的高成交量价格带

### 6.2 结构位去重

价格太近的结构位会被 `_dedupe_price_levels()` 合并。

最小去重间距不是固定值，而是取下面两者较大值：

- `structure_min_gap_pct=0.005`
- `ATR * structure_min_gap_atr_multiplier / current_price`

默认 `structure_min_gap_atr_multiplier=0.8`。

### 6.3 不同层级使用不同时间框架

`_structure_timeframes_for_layer()` 的规则：

- 所有层先看 `sr_timeframe`，默认 `1h`
- 从第 2 层起，再补看交易主周期 `timeframe`，默认 `5m`

实现上是先按周期从小到大排序，所以第 2 层及以后通常会先看 `5m`，再看 `1h`。这意味着深层加仓会更关注近端结构。

---

## 7. 加仓逻辑

策略不会一次性把所有层全部挂好，而是“只挂下一层”。核心函数是 `place_add_order()` 和 `_build_add_order_plan()`。

### 7.1 什么情况下考虑补下一层

在 `IN_STRATEGY` 且“已有持仓”时：

1. 先做止盈止损检查
2. 再根据当前持仓量变化，确认是否有加仓/减仓成交
3. 然后只考虑 `next_layer = layer + 1`

只有当：

- 当前阶段允许继续加层
- 当前没有其他非减仓挂单

才会尝试补下一层。

### 7.2 加仓价格选择优先级

加仓价格选择顺序是：

1. 结构位
2. ATR 距离
3. 固定百分比兜底

这和脚本开头的策略说明一致。

### 7.3 第一优先级：结构位加仓

函数 `_select_structure_entry_price()` 的原则：

- 做多：选低于当前价、也低于持仓均价的支撑位
- 做空：选高于当前价、也高于持仓均价的阻力位

然后再做两个修正：

1. 用 `level_offset_pct=0.001` 做一点点偏移
2. 如果已有 `last_fill_price`，还要检查和上一笔成交价之间是否满足最小层间距

PHASE1 和 PHASE2 的最小层间距不同：

- `PHASE1`：`phase1_layer_min_gap_pct=0`，同时还会参考 `phase1_layer_min_gap_atr_multiplier=0.35`
- `PHASE2`：`phase2_layer_min_gap_pct=1%`，同时还会参考 `phase2_layer_min_gap_atr_multiplier=1.4`

代码的实际间距计算由 `_gap_ratio()` 完成，取：

- 固定百分比间距
- ATR 换算间距

两者中的较大值。

### 7.4 结构位“粘性”与价格稳定

为了避免价格小幅变化就不停撤单重挂，脚本加入了“粘性结构位”机制。

核心点有两个：

#### 7.4.1 `_select_sticky_structure_price()`

如果新的候选结构价与当前未成交挂单价非常接近，就优先沿用接近旧挂单的那个价格。

阈值取三者最大值：

- `structure_refresh_threshold=0.008`
- `level_offset_pct*2`
- `0.001`

默认情况下大约是 `0.8%`。

#### 7.4.2 `_stabilize_plan_with_existing_entry()`

如果已有挂单价格虽然不是理论最优，但新价格没有“明显更优”，就保留旧挂单。

这里“明显更优”定义为：

- 新结构位距离当前价更近
- 且改善幅度达到 `structure_refresh_threshold`

因此，这套策略是“会动态校准挂单”，但不会过度抖动。

### 7.5 第二优先级：ATR 距离加仓

没有合适结构位时，走 `_select_atr_entry_price()`。

规则：

- 第 2 层：`1.0 ATR`
- 第 3 层：`1.5 ATR`
- 第 4 层：`2.0 ATR`
- 第 5 层：`2.5 ATR`
- 更深层继续每层 `+0.5 ATR`

公式：

- `atr_mult = 1.0 + max(layer_num - 2, 0) * 0.5`

做多时：

- `price = min(avg_price, current_price) - distance`

做空时：

- `price = max(avg_price, current_price) + distance`

### 7.6 第三优先级：固定百分比兜底

如果 ATR 也拿不到，就走 `_select_fallback_entry_price()`。

公式：

- `offset_pct = add_layer_base_offset_pct * layer_num`
- 默认 `add_layer_base_offset_pct=0.005`

所以：

- 第 2 层大约偏移 `1.0%`
- 第 3 层大约偏移 `1.5%`
- 第 4 层大约偏移 `2.0%`

做多时从均价往下偏，做空时从均价往上偏。

### 7.7 非结构位价格还要再做层间距约束

如果本层价格来源不是 `STRUCTURE`，策略会调用 `_enforce_layer_spacing()` 强制拉开距离。

锚点价格 `_layer_anchor_price()` 取以下候选中最“保守”的一个：

- 当前待成交挂单价 `pending_entry_price`
- 最近成交价 `last_fill_price`
- 当前持仓均价 `avg_price`
- 当前价格 `current_price`

做多取最小值，做空取最大值。

这样做的含义是：后续补仓不能比已有更深的挂单还更浅。

### 7.8 加仓层间触发逻辑

脚本里保留了 `_next_layer_trigger_ready()` 和 `trigger_price` 方案，设计上支持“先等价格走到足够不利位置，再触发下一层条件单”。

触发距离仍然由 `_gap_ratio(..., kind="trigger")` 计算：

- `PHASE1` 默认：`max(0, 0.35*ATR/current_price)`
- `PHASE2` 默认：`max(1.2%, 1.5*ATR/current_price)`

但是当前版本 `_build_add_order_plan()` 并没有主动给 `plan['trigger_price']` 赋值，默认始终是 `0`。所以在现有实现里：

- 加仓单通常直接挂限价单
- `_submit_add_order_plan()` 只有在 `trigger_price > 0` 时才会创建条件单

也就是说，代码结构支持“条件加仓”，但当前主流程实际上主要是“直接挂下一层限价单”。

### 7.9 加仓保证金与倍率

加仓保证金计算方式：

- `desired_margin = equity * phase_cfg['first_order_ratio'] * 当前层倍率`

其中：

- `first_order_ratio` 仍然是 `phase1_first_order_ratio=0.025`
- 倍率来自当前阶段对应的 `layer_multipliers`

PHASE1 默认：

- 第 1 层：`1`
- 第 2 层：`1.15`
- 第 3 层：`1.35`

PHASE2 默认追加倍率：

- 第 4 层起通常对应 `1.7, 2.1, 2.6, 3.2, 3.9, 4.8`

实际下单前还会再被 `_calculate_order_margin()` 裁到“安全保证金上限”以内。

### 7.10 加仓下单后的状态更新

加仓挂单成功后：

- `state.phase = 当前阶段`
- `state.pending_layer = max(state.layer, layer_num)`
- `state.pending_entry_price = execute_price`

这里有一个重要语义：

- `layer` 是已确认成交层
- `pending_layer` 是已挂出去等待成交的目标层

这样主循环就能在成交后把层数正确推进。

---

## 8. PHASE1 / PHASE2 阶段切换逻辑

### 8.1 阶段划分的目的

这套策略把加仓分为两个阶段：

- `PHASE1`：前几层相对温和，层间距小，倍率小
- `PHASE2`：更深层防守，层间距更大，倍率也更大

本质上是把“正常补仓”和“深水区补仓”拆开。

### 8.2 什么时候切到 PHASE2

函数 `_should_switch_to_phase2()` 的逻辑是：

满足任一条件就切换：

1. 当前已经在 `PHASE2`
2. `layer >= phase_switch_layer`
   - 默认 `phase_switch_layer=4`
3. 当前持仓收益率 `profit_pct <= -phase_switch_loss_pct`
   - 默认亏损线 `-2.5%`

所以切阶段有两条路径：

- 层数触发：做到第 4 层及更深，自然进入 PHASE2
- 亏损触发：哪怕层数还不深，只要浮亏超过 2.5%，也可以提前进入 PHASE2

### 8.3 切换时会记录 `phase2_start_layer`

在 `_current_phase()` 里，一旦第一次切到 `PHASE2`，会执行：

- `state.phase = "PHASE2"`
- `state.phase2_start_layer = max(state.layer + 1, 1)`

这个字段非常关键，因为它记录了：

- 如果不是正常从第 4 层才进入 PHASE2
- 而是“亏损提前切换”
- 那么 PHASE2 实际是从哪一层开始用自己的倍率序列

### 8.4 切回 PHASE1 的条件

如果 `_should_switch_to_phase2()` 不再成立，则 `_current_phase()` 会把状态改回：

- `phase="PHASE1"`
- `phase2_start_layer=0`

不过在真实运行里，只要持仓还在、层数已经深了，通常不会轻易回到 PHASE1。更常见的是新一轮交易重置后重新从 PHASE1 开始。

---

## 9. 边界 bug：提前切换后 layer4 primary 抢先命中导致倍率跳回问题

这是你特别要求指出的边界问题，当前脚本已经有修复思路，修复点在 `_resolve_phase_layer_index()`。

### 9.1 bug 场景

问题场景是：

1. 策略原本还没走到默认的 `phase_switch_layer=4`
2. 但由于浮亏先达到 `phase_switch_loss_pct`，所以提前切到 `PHASE2`
3. 这时下一层虽然可能还是“第 2 层、第 3 层或第 4 层”这种全局层号，但它在逻辑上已经属于 PHASE2 的第一层或第二层
4. 如果仍然用旧的“主索引算法”：
   - `primary_index = layer_num - offset - 1`
   - 其中 `offset=phase1_max_layers=3`
5. 那么某些边界情况下会把 PHASE2 层错误映射回较小索引
6. 结果就是后续某一层，尤其是你提到的 `layer4 primary` 抢先命中时，使用了错误倍率，出现“倍率跳回”而不是继续递增

### 9.2 当前代码如何处理

当前实现：

```python
primary_index = layer_num - offset - 1
if 0 <= primary_index < len(layer_multipliers):
    return primary_index

if phase == 'PHASE2':
    phase2_start_layer = int(getattr(self.state, 'phase2_start_layer', 0) or 0)
    if phase2_start_layer > 0:
        fallback_index = layer_num - phase2_start_layer
    else:
        fallback_index = layer_num - offset - 1
    if 0 <= fallback_index < len(layer_multipliers):
        return fallback_index
```

它的核心思想是：

- 正常情况先走 `primary_index`
- 如果当前是 `PHASE2` 且存在“提前切换”的可能
- 就改用 `phase2_start_layer` 去算阶段内索引

这样一来：

- PHASE2 第一次实际生效的那一层，会对应 PHASE2 的第 0 个倍率
- 后续层数会按 PHASE2 序列继续递增
- 不会因为全局层号和固定偏移量错位，导致倍率回退

### 9.3 文档化结论

可以把这个 bug 概括为：

- 根因：`PHASE2` 在“提前切换”路径下，不能再用固定 `phase1_max_layers` 作为倍率索引偏移
- 修复：引入 `phase2_start_layer`，按 PHASE2 的实际起始层计算阶段内倍率索引
- 效果：避免“layer4 primary 抢先命中后倍率跳回”

---

## 10. 挂单管理逻辑

这部分是策略非常重要、也很容易被忽略的一块。

### 10.1 策略只允许一个未完成的非减仓挂单

无论首仓还是加仓，策略都尽量保证：

- 同一时间只有一个“下一步”的非减仓挂单

这样做的目的：

- 降低状态复杂度
- 防止多层同时成交，打乱层级管理
- 防止由于价格剧烈波动导致超预期一次性吃进多层

### 10.2 启动时挂单校准

程序启动后会执行 `_reconcile_startup_entry_orders()`：

1. 如果没有持仓，直接跳过
2. 如果有持仓且有未完成加仓单
3. 重新根据最新价格、阶段、结构位，计算“理论上下一层应该挂在哪里”
4. 比较现有挂单和理论计划是否一致

如果当前阶段已经不需要下一层挂单，会直接撤掉旧挂单。

### 10.3 运行中挂单校准

在主循环中，只要已经有加仓挂单，就会调用 `_reconcile_active_entry_orders()` 做校准。

它会检查：

- 挂单方向是否一致
- 数量是否一致
- 委托价是否偏离太多
- 如果是条件单，还会检查触发价是否偏离太多
- 阶段切换后，是否需要用新阶段参数重建挂单

### 10.4 什么情况下会撤单重挂

`_entry_order_refresh_reason()` 里定义了重建理由，典型包括：

- 当前没有挂单
- 当前有不止一个加仓挂单
- 挂单方向不一致
- 挂单数量不一致
- 挂单类型不一致
- 触发价偏离过大
- 委托价偏离过大

偏离阈值用的是：

- `structure_refresh_threshold=0.008`

即默认约 `0.8%`。

### 10.5 如何撤单

定向撤加仓单用 `_cancel_entry_orders()`：

- 只撤 `reduceOnly=False` 的单
- 如果适配器支持 `cancel_orders`，就只撤指定挂单
- 如果适配器不支持，就打印警告，保留旧挂单，避免误撤其他单

### 10.6 全撤单

`cancel_all_orders()` 用于更大范围场景：

- 平仓后清理
- 信号反向、准备换方向重挂
- 无效等待单需要取消时

---

## 11. 成交确认与层级推进

策略不会只凭“我挂了第 N 层单”就认为已经成交，而是看真实持仓张数变化。

主循环里，如果发现：

- `current_contracts > previous_contracts`

就认为发生了开仓或加仓成交。

然后：

1. 更新 `last_known_contracts`
2. 如果之前是空仓，现在有仓了：
   - 记录首笔成交价到 `last_fill_price`
3. 如果 `pending_layer > layer`：
   - 说明等待中的下一层已成交
   - 把 `layer` 推进到 `pending_layer`
   - 把成交价写入 `last_fill_price`
   - 清空 `pending_entry_price`

如果持仓减少，则认为发生了分批止盈或其他减仓。

---

## 12. 动态止盈逻辑

止盈不是固定 5% 或 8%，而是动态的。

### 12.1 动态移动止盈激活阈值

`_dynamic_tp_values(adx, volatility_pct)` 输出两个值：

- `activate_pct`：移动止盈激活阈值
- `trail_ratio`：允许回撤比例

它会综合考虑：

- 杠杆
- 手续费
- 当前 ADX
- 当前 ATR 波动率

基础思路是：

1. 先根据手续费和保护止损底线，算一个最低盈利门槛
2. 再根据趋势强弱和波动大小做乘数放大
3. 最后限制在合理区间内，最高不超过 `15%`

趋势越强、波动越大，激活阈值通常越高；趋势较弱时会更容易激活。

### 12.2 风险上下文

`_build_risk_context()` 会同时计算：

- `adx`
- `atr`
- `volatility_pct = atr/current_price`
- `activate_pct`
- `trail_ratio`
- 最近 `30` 根的最高价 / 最低价
- ATR 追踪止盈价 `trail_price`

ATR 追踪乘数：

- 层数 `< 4`：`4.0 * ATR`
- 层数 `>= 4`：`3.0 * ATR`

也就是深层仓位反而更快收紧追踪。

---

## 13. 分批止盈逻辑

### 13.1 分批止盈阈值是动态的

`_dynamic_partial_tp_targets()` 会根据这些因素调整 TP1 / TP2：

- `adx`
- `volatility_pct`
- `layer`
- `phase`

经验方向是：

- 趋势强、波动大，可以适当放宽目标
- 层数深、已经进入 `PHASE2`，则目标会更保守一些，尽早锁利润

### 13.2 默认比例

虽然阈值是动态的，但分批比例是固定的：

- `tp1_ratio = 0.30`
- `tp2_ratio = 0.20`

也就是：

- 第一档平 30%
- 第二档再平 20%
- 剩余 50% 继续用移动保护和追踪止盈

### 13.3 典型阈值范围

代码里阈值会被限制在大致这些范围内：

- `tp1_threshold` 最低不低于约 `1.5%`，最高不超过 `8%`
- `tp2_threshold` 至少比 TP1 多 `1.5%`，最高不超过 `14%`

---

## 14. 保护止损逻辑

保护止损是这套策略里非常关键的“锁盈底线”机制。

### 14.1 什么时候会挂保护止损

在下面这些场景，会调用 `_arm_protective_stop()`：

- 轮询移动止盈激活后
- WS 实时风控激活后
- 本来想触发回撤平仓或 ATR 平仓，但当前利润太低，不适合直接平时

### 14.2 保护止损价格如何算

`_protective_stop_target()` 的算法：

1. 取当前生命周期内最高浮盈 `best_profit_pct`
2. 计算应锁定利润：
   - 至少是 `protective_stop_min_profit_pct`
   - 或者是 `best_profit_pct * protective_stop_profit_lock_ratio`
   - 取两者较大值
3. 如果当前利润离最高利润已经回撤很多，会再额外限制，防止挂出一个“不可能成交前仍保住”的价
4. 再把锁定利润按杠杆换算回标的价格变动

默认：

- `protective_stop_profit_lock_ratio=0.25`
- `protective_stop_min_profit_pct=0.5%`

意思就是：最高浮盈越高，保护止损也会抬得越高，但至少要锁住一小部分正收益。

### 14.3 保护止损更新频率

如果新计算出的保护止损价相对旧价格变化不够大，就不更新。

阈值：

- `protective_stop_update_step_pct=0.003`

即默认新止损要至少改善 `0.3%`，才重挂。

### 14.4 保护止损与追踪平仓的关系

函数 `_allow_trailing_close()` 的逻辑很重要：

- 如果当前收益还高于 `trailing_min_close_profit_pct`，允许直接执行追踪平仓
- 如果当前收益太低，不想因为轻微回撤把本来不大的利润全部抹掉
- 那就改挂保护止损，不立即总平

默认：

- `trailing_min_close_profit_pct=0.5%`

所以这套策略不是“只要触发回撤就立刻平”，而是会先判断值不值得直接平。

---

## 15. 总止损逻辑

最硬的一条总止损非常简单：

- `current_profit_pct < -max_loss_pct`

默认：

- `max_loss_pct=0.5`

也就是杠杆收益率亏损超过 `50%` 时，直接触发总平仓。

这条规则存在于两条风控链路里：

- `check_trailing_tp()` 的轮询风控
- `_ws_risk_step()` 的 WS 实时风控

因此即便轮询还没到时间，只要 WS 价格更新正常，也会更快触发。

---

## 16. 回撤保护逻辑

一旦移动止盈已经激活，策略会比较：

- 当前利润 `current_profit_pct`
- 历史最高利润 `best_profit_pct`

回撤量：

- `drawdown = best_profit_pct - current_profit_pct`

允许最大回撤：

- `max_drawdown = trail_ratio * best_profit_pct`

如果层数已经比较深，即 `layer >= 4`，还会进一步收紧：

- 当最高浮盈超过 `10%` 时，最大回撤不超过 `3.5%`
- 否则最大回撤不超过 `5%`

满足条件时，触发“回撤保护”总平仓。

这条逻辑同时存在于：

- 轮询版：`check_trailing_tp()`
- WS 版：`_ws_risk_step()`

---

## 17. ATR 追踪止盈逻辑

这部分可以理解为“价格跌破/涨破动态保护线后退出剩余仓位”。

### 17.1 追踪线怎么计算

在 `_build_risk_context()` 里：

做空时：

- `trail_price = recent_low + atr_multiplier * atr`

做多时：

- `trail_price = recent_high - atr_multiplier * atr`

其中：

- `recent_high/recent_low` 取最近 30 根 K 线
- `atr_multiplier` 见上文，浅层为 4，深层为 3

### 17.2 什么情况下触发

- 持有空单时，如果 `current_price > trail_price`
- 持有多单时，如果 `current_price < trail_price`

就认为价格已经明显回吐，触发 ATR 追踪止盈。

---

## 18. 平仓逻辑

### 18.1 分批平仓

由 `_execute_partial_take_profit()` 调用 `_partial_close()` 完成。

特点：

- 使用市价单
- 带 `reduceOnly=True`
- 平仓成功后更新 `partial_tp_1_done` 或 `partial_tp_2_done`

### 18.2 总平仓

总平流程统一由 `_execute_exit_pipeline()` 管理，顺序是：

1. 设置 `_exit_in_progress`
2. 获取最新持仓
3. 调用 `close_position()` 市价全平
4. 取消保护止损 `_clear_protective_stop()`
5. `cancel_all_orders()` 撤所有挂单
6. `sync_state_with_exchange()` 与交易所再同步一次
7. 写本地快照
8. 清掉 `_exit_in_progress`

这个统一出口很重要，因为它避免了不同风控路径各自平仓，导致状态不一致。

---

## 19. WS 风控保护与轮询风控的关系

策略同时存在两条风险控制链路。

### 19.1 WS 实时风控

通过 `_start_ws_risk_monitor()` 启动后台线程，默认每：

- `ws_risk_check_interval=0.25s`

执行一次 `_ws_risk_step()`。

它尽量使用：

- WebSocket 最新 ticker
- WebSocket 最新 position

如果拿不到完整仓位，但还能拿到最新价，还会尝试用 `_synthetic_runtime_position()` 构造一个“临时仓位对象”继续风控。

因此 WS 风控的优先级很高，目的是在快速波动时比 20 秒一轮的主循环更快反应。

### 19.2 轮询风控

主循环中的 `check_trailing_tp()` 属于兜底轮询链路。

它的特点：

- 逻辑更完整，打印也更详细
- 每轮主循环执行一次，默认 `20s`
- 即便 WS 暂时不可用，也仍能做止盈止损

### 19.3 两条链路避免冲突的方式

避免重复平仓的关键机制有两个：

- `_exit_in_progress`
- `action_lock`

只要一条链路已经进入平仓流程，另一条链路会自动跳过。

---

## 20. 无持仓但有挂单时的逻辑

这是从“等待首仓成交”回看很重要的一段。

当 `bot_state="IN_STRATEGY"` 但实际上没有持仓时，说明通常是：

- 首仓还没成交
- 或者刚刚平掉仓位，但挂单还没清掉

这时策略会看挂单和当前信号是否还兼容。

### 20.1 如果没有挂单

直接：

- `_reset_state()`
- 回到 `IDLE`

### 20.2 如果挂单方向与当前信号相反

例如：

- 原来等做多，但现在信号变成 `SHORT`
- 原来等做空，但现在信号变成 `LONG`

则：

1. 撤掉所有挂单
2. 重置状态
3. 按新方向重新挂首仓

### 20.3 如果信号变成 `WAIT`

这时会调用 `_is_pending_entry_signal_compatible()`：

做多等待单在以下情况可继续保留：

- 当前信号还是 `LONG`
- 或者 `keep_pending_entry_on_stretched=true` 且市场状态是 `BULLISH_BUT_STRETCHED`

做空等待单同理：

- 当前信号还是 `SHORT`
- 或者允许 stretched 保留，且市场状态是 `BEARISH_BUT_STRETCHED`

如果不兼容，就撤单回到 `IDLE`。

这相当于给“方向没变但位置变差了”的行情留了一点耐心。

---

## 21. 状态同步与异常处理

### 21.1 启动阶段自动重连与恢复

`_bootstrap_exchange()` 会循环执行：

- 设置账户
- 加载市场
- 同步交易所状态

如果启动时连接交易所失败：

- 打印错误
- 睡眠 `error_sleep=60s`
- 自动重试

不会直接退出。

### 21.2 运行中异常处理

主循环内部包了大 `try/except`：

- 普通异常会打印 traceback
- 然后等待 `error_sleep`
- 继续运行

这意味着策略设计目标是“常驻进程”，不是一次报错就停。

### 21.3 状态同步逻辑

`sync_state_with_exchange()` 分三种情况。

#### 21.3.1 有持仓

则同步：

- `bot_state="IN_STRATEGY"`
- `position_side`
- `entry_price`
- `last_known_contracts`
- 根据持仓量估算当前层数 `estimate_current_layer()`
- 根据当前盈亏/层数确定 `phase`
- 根据是否存在挂单决定 `pending_layer`
- 同步 `pending_entry_price`

#### 21.3.2 无持仓但有挂单

则认为策略还在等待成交：

- `bot_state="IN_STRATEGY"`
- `position_side` 由第一张挂单方向推断
- `pending_entry_price` 从挂单同步

#### 21.3.3 无持仓也无挂单

直接 `_reset_state()`，回到初始状态。

### 21.4 本地状态持久化

状态保存在：

- `data/martin-runtime.json`

每次关键状态变化都会 `_save_runtime_state()`。

因此即使脚本重启，也能尽量恢复：

- 当前层数
- 最佳浮盈
- 当前阶段
- 挂单价
- 最近成交价
- 保护止损信息

### 21.5 本地实时快照

策略还会把综合快照写到：

- `data/martin-live.json`

里面包含：

- 策略参数快照
- 账户信息
- 持仓信息
- 挂单
- 最近成交
- 账本
- 市场指标

这主要服务监控和 dashboard，也有助于异常排查。

---

## 22. 用一句话总结完整交易流程

这套脚本的实际交易流程可以概括为：

1. `IDLE` 时先看交易所真实状态，再做信号判断
2. 信号出来后按“强趋势追入”或“结构位等待”挂首仓
3. 首仓成交后进入持仓管理，只挂下一层，不一次性挂满
4. 加仓价格优先用结构位，其次 ATR，再次固定偏移，并带挂单粘性与价格校准
5. 持仓过程中根据层数或浮亏切换 `PHASE1/PHASE2`
6. 盈利后先激活动态移动止盈，再执行两档分批止盈
7. 剩余仓位继续受保护止损、回撤保护和 ATR 追踪止盈管理
8. 如果亏损超过硬阈值，立即总平
9. 平仓后撤掉所有挂单，重置状态，回到 `IDLE`

---

## 23. 这套策略最关键的设计特点

- 它不是“固定网格”，而是每次补仓都重新看最新结构
- 它不是“只靠主循环”，而是主循环加 WS 实时风控双保险
- 它不是“固定止盈线”，而是根据趋势、波动、层数、阶段动态调整
- 它不是“简单马丁翻倍”，而是分 `PHASE1/PHASE2` 两阶段扩张风险
- 它对挂单管理非常谨慎，尽量保证任一时刻只有一个明确的下一步动作

如果后续你还需要，我可以继续把这份文档再补成两版：

- 一版“给交易员看”的纯策略说明版
- 一版“给开发者看”的函数级调用关系版
