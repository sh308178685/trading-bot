const money = new Intl.NumberFormat("zh-CN", {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});

const qty = new Intl.NumberFormat("zh-CN", {
  minimumFractionDigits: 0,
  maximumFractionDigits: 4,
});

const state = {
  refreshTimer: null,
};

function fmtMoney(value) {
  return `${money.format(Number(value || 0))} USDT`;
}

function fmtNumber(value, digits = 2) {
  return Number(value || 0).toFixed(digits);
}

function fmtPct(value, digits = 2) {
  return `${Number(value || 0).toFixed(digits)}%`;
}

function signClass(value) {
  return Number(value || 0) >= 0 ? "positive" : "negative";
}

function translateSignal(value) {
  if (value === "SHORT") return "做空";
  if (value === "LONG") return "做多";
  if (value === "WAIT") return "等待";
  return value || "--";
}

function translateTrend(value) {
  if (value === "UPTREND") return "上涨趋势";
  if (value === "DOWNTREND") return "下跌趋势";
  if (value === "CHOPPY") return "震荡";
  if (value === "未知") return "未知";
  return value || "--";
}

function translateSide(value) {
  if (value === "long") return "做多";
  if (value === "short") return "做空";
  if (value === "buy") return "买入";
  if (value === "sell") return "卖出";
  return value || "--";
}

function translateMode(value) {
  if (value === "sandbox") return "模拟盘";
  if (value === "live") return "实盘";
  return value || "--";
}

function translateSource(value) {
  if (value === "STRUCTURE") return "结构位";
  if (value === "ATR") return "ATR";
  if (value === "FALLBACK") return "兜底偏移";
  if (value === "OFFSET") return "偏移";
  if (value === "RISK") return "风控";
  return value || "--";
}

function translateReferenceType(value) {
  if (value === "support") return "支撑位";
  if (value === "resistance") return "阻力位";
  if (value === "market") return "现价";
  if (value === "atr") return "ATR 距离";
  if (value === "avg_price") return "均价偏移";
  return value || "--";
}

function translateState(value) {
  if (value === "filled") return "已成交";
  if (value === "next") return "下一层";
  if (value === "standby") return "待命";
  if (value === "IDLE") return "空闲";
  if (value === "IN_STRATEGY") return "策略中";
  return value || "--";
}

function translateOrderType(item) {
  if (item.reduce_only) return "减仓";
  if (item.type === "limit") return "限价";
  if (item.type === "market") return "市价";
  return item.type || "--";
}

function setText(id, value) {
  const node = document.getElementById(id);
  if (node) {
    node.textContent = value;
  }
}

function isSamePrice(left, right) {
  const a = Number(left || 0);
  const b = Number(right || 0);
  if (!a || !b) {
    return false;
  }
  const tolerance = Math.max(0.01, Math.max(Math.abs(a), Math.abs(b)) * 0.0002);
  return Math.abs(a - b) <= tolerance;
}

function createMetricCard(metric) {
  const card = document.createElement("article");
  card.className = "metric-card";
  card.innerHTML = `
    <p>${metric.label}</p>
    <h3 class="${metric.emphasis || ""}">${metric.value}</h3>
    <div class="delta ${metric.deltaClass || ""}">${metric.detail}</div>
  `;
  return card;
}

function renderMetrics(snapshot) {
  const grid = document.getElementById("metrics-grid");
  grid.innerHTML = "";

  const metrics = [
    {
      label: "权益估算",
      value: fmtMoney(snapshot.performance.equity_estimate),
      detail: `余额 ${fmtMoney(snapshot.account.total)}`,
    },
    {
      label: "浮动盈亏",
      value: fmtMoney(snapshot.performance.unrealized_pnl),
      detail: `收益率 ${fmtPct(snapshot.performance.roi_pct)}`,
      emphasis: signClass(snapshot.performance.unrealized_pnl),
      deltaClass: signClass(snapshot.performance.unrealized_pnl),
    },
    {
      label: "当前层级",
      value: `${snapshot.runtime.layer || 0}/${snapshot.strategy.max_layers}`,
      detail: `${translateState(snapshot.runtime.bot_state)} | ${translateSide(snapshot.runtime.position_side)}`,
    },
    {
      label: "当前信号",
      value: translateSignal(snapshot.market.indicators.signal),
      detail: `${translateTrend(snapshot.market.indicators.trend)} | 止盈激活 ${fmtPct(snapshot.market.indicators.dynamic_tp_activate_pct)}`,
    },
    {
      label: "近期成交额",
      value: fmtMoney(snapshot.performance.turnover_recent),
      detail: `手续费 ${fmtMoney(snapshot.performance.fees_recent)}`,
    },
    {
      label: "胜率",
      value: fmtPct(snapshot.trade_analytics.win_rate_pct),
      detail: `${snapshot.trade_analytics.closed_cycle_count} 个已完成轮次`,
    },
  ];

  metrics.forEach((metric) => grid.appendChild(createMetricCard(metric)));
}

function buildLinePath(values, width, height, padTop, padBottom, minValue, maxValue) {
  if (!values.length) {
    return "";
  }
  const usableHeight = height - padTop - padBottom;
  const stepX = values.length > 1 ? width / (values.length - 1) : width;
  return values
    .map((value, index) => {
      const normalized = maxValue === minValue ? 0.5 : (value - minValue) / (maxValue - minValue);
      const x = index * stepX;
      const y = padTop + (1 - normalized) * usableHeight;
      return `${index === 0 ? "M" : "L"} ${x.toFixed(2)} ${y.toFixed(2)}`;
    })
    .join(" ");
}

function renderChart(targetId, series, options = {}) {
  const target = document.getElementById(targetId);
  if (!target) {
    return;
  }

  const pointsCount = Math.max(...series.map((item) => item.values.length), 0);
  if (!pointsCount) {
    target.innerHTML = `<div class="empty-state">暂无图表数据。</div>`;
    return;
  }

  const width = 900;
  const height = options.height || 240;
  const padTop = 18;
  const padBottom = 28;

  const allValues = series.flatMap((item) => item.values).filter((value) => Number.isFinite(value));
  const minValue = Math.min(...allValues);
  const maxValue = Math.max(...allValues);

  const gridLines = [0.1, 0.5, 0.9]
    .map((ratio) => {
      const y = ratio * height;
      return `<line class="chart-grid-line" x1="0" y1="${y}" x2="${width}" y2="${y}"></line>`;
    })
    .join("");

  const paths = series
    .map((item, index) => {
      const path = buildLinePath(item.values, width, height, padTop, padBottom, minValue, maxValue);
      const area = `${path} L ${width} ${height - padBottom} L 0 ${height - padBottom} Z`;
      const areaMarkup = index === 0
        ? `<path class="chart-area" d="${area}" fill="${item.color}"></path>`
        : "";
      return `
        ${areaMarkup}
        <path class="chart-path" d="${path}" stroke="${item.color}"></path>
      `;
    })
    .join("");

  const labels = options.labels || [];
  const firstLabel = labels[0] || "";
  const lastLabel = labels[labels.length - 1] || "";

  target.innerHTML = `
    <svg class="chart-svg" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" role="img">
      ${gridLines}
      ${paths}
      <text class="chart-label" x="0" y="${height - 6}">${firstLabel}</text>
      <text class="chart-label" x="${width}" y="${height - 6}" text-anchor="end">${lastLabel}</text>
    </svg>
  `;
}

function renderRuntimeFlags(snapshot) {
  const wrap = document.getElementById("runtime-flags");
  const flags = [
    { label: "最高浮盈", value: fmtPct(snapshot.runtime.best_profit_pct) },
    { label: "分批止盈 1", value: snapshot.runtime.partial_tp_1_done ? "已完成" : "未完成" },
    { label: "分批止盈 2", value: snapshot.runtime.partial_tp_2_done ? "已完成" : "未完成" },
    { label: "移动止盈", value: snapshot.runtime.activated ? "已激活" : "待激活" },
  ];

  wrap.innerHTML = flags
    .map(
      (item) => `
        <div class="runtime-flag">
          <span>${item.label}</span>
          <strong>${item.value}</strong>
        </div>
      `
    )
    .join("");
}

function renderTradeAnalytics(snapshot) {
  const analytics = snapshot.trade_analytics;
  const grid = document.getElementById("analytics-grid");
  const note = document.getElementById("analytics-note");
  const openCycle = document.getElementById("open-cycle");
  const cyclesBody = document.getElementById("cycles-body");

  note.textContent = `最近成交 ${analytics.latest_fill_at || "--"}`;

  const cards = [
    {
      label: "近期已实现净收益",
      value: fmtMoney(analytics.realized_net_recent),
      detail: `毛收益 ${fmtMoney(analytics.realized_gross_recent)}`,
      emphasis: signClass(analytics.realized_net_recent),
      deltaClass: signClass(analytics.realized_net_recent),
    },
    {
      label: "已完成轮次",
      value: `${analytics.closed_cycle_count}`,
      detail: `盈利 ${analytics.winning_cycles} | 亏损 ${analytics.losing_cycles}`,
    },
    {
      label: "最大单笔成交额",
      value: fmtMoney(analytics.largest_fill_cost),
      detail: `开仓 ${analytics.entry_fills} | 平仓 ${analytics.exit_fills}`,
    },
    {
      label: "资金占用率",
      value: fmtPct(snapshot.account.utilization_pct),
      detail: `可用 ${fmtMoney(snapshot.account.free)}`,
    },
  ];

  grid.innerHTML = "";
  cards.forEach((card) => grid.appendChild(createMetricCard(card)));

  if (analytics.open_cycle) {
    openCycle.innerHTML = `
      <div class="definition-row"><span>方向</span><strong>${analytics.open_cycle.direction}</strong></div>
      <div class="definition-row"><span>开始时间</span><strong>${analytics.open_cycle.opened_at || "--"}</strong></div>
      <div class="definition-row"><span>平均进场价</span><strong>${fmtMoney(analytics.open_cycle.entry_avg_price)}</strong></div>
      <div class="definition-row"><span>跟踪数量</span><strong>${qty.format(analytics.open_cycle.entry_qty)}</strong></div>
      <div class="definition-row"><span>成交次数</span><strong>${analytics.open_cycle.fills}</strong></div>
      <div class="definition-row"><span>手续费</span><strong>${fmtMoney(analytics.open_cycle.fees)}</strong></div>
    `;
  } else {
    openCycle.innerHTML = `<div class="empty-state">当前没有正在进行中的交易轮次。</div>`;
  }

  if (!analytics.closed_cycles.length) {
    cyclesBody.innerHTML = `<tr><td colspan="4" class="empty-state">还没有完成的交易轮次。</td></tr>`;
    return;
  }

  cyclesBody.innerHTML = analytics.closed_cycles
    .map(
      (cycle) => `
        <tr>
          <td>${cycle.opened_at || "--"}</td>
          <td>${cycle.direction}</td>
          <td class="${signClass(cycle.net_pnl)}">${fmtMoney(cycle.net_pnl)}</td>
          <td>${fmtNumber(cycle.duration_minutes, 1)} 分钟</td>
        </tr>
      `
    )
    .join("");
}

function renderNextAction(snapshot) {
  const target = document.getElementById("next-action");
  const action = snapshot.next_action;
  const pricing = action.pricing || null;
  const candidateLabel = pricing?.candidate_index
    ? `第 ${pricing.candidate_index} 档 / 共 ${pricing.candidate_count || pricing.candidate_index} 档`
    : "--";
  const pricingMarkup = pricing
    ? `
      <div class="pricing-panel">
        <div class="pricing-head">
          <span>挂单定价拆解</span>
          <strong>${pricing.reference_label || translateReferenceType(pricing.reference_type)}</strong>
        </div>
        <div class="definition-list">
          <div class="definition-row"><span>锚点价位</span><strong>${pricing.reference_price ? fmtMoney(pricing.reference_price) : "--"}</strong></div>
          <div class="definition-row"><span>偏移后目标</span><strong>${pricing.preferred_price ? fmtMoney(pricing.preferred_price) : "--"}</strong></div>
          <div class="definition-row"><span>保护边界</span><strong>${pricing.guard_price ? fmtMoney(pricing.guard_price) : "--"}</strong></div>
          <div class="definition-row"><span>最终挂单价</span><strong>${pricing.final_price ? fmtMoney(pricing.final_price) : "--"}</strong></div>
          <div class="definition-row"><span>候选档位</span><strong>${candidateLabel}</strong></div>
          <div class="definition-row"><span>偏移比例</span><strong>${fmtPct(pricing.offset_pct)}</strong></div>
        </div>
        <p class="pricing-note">${pricing.rule_label || "暂无定价说明。"}</p>
      </div>
    `
    : "";

  target.innerHTML = `
    <p class="eyebrow">主动作建议</p>
    <h3>${action.title}</h3>
    <p>${action.detail}</p>
    <div class="action-meta">
      <span>方向：${translateSide(action.side)}</span>
      <span>层级：${action.layer || "--"}</span>
      <span>预估价格：${action.projected_price ? fmtMoney(action.projected_price) : "--"}</span>
      <span>来源：${translateSource(action.source)}</span>
    </div>
    ${pricingMarkup}
  `;
}

function renderIndicators(snapshot) {
  const indicators = snapshot.market.indicators;
  const target = document.getElementById("indicator-list");
  const rows = [
    ["最新价格", fmtMoney(indicators.price)],
    ["快速 EMA", fmtMoney(indicators.ema_fast)],
    ["慢速 EMA", fmtMoney(indicators.ema_slow)],
    ["RSI", fmtNumber(indicators.rsi)],
    ["ADX", fmtNumber(indicators.adx)],
    ["ATR", fmtMoney(indicators.atr)],
    ["止盈激活值", fmtPct(indicators.dynamic_tp_activate_pct)],
    ["回撤比例", fmtPct(indicators.dynamic_tp_trail_ratio)],
  ];

  target.innerHTML = rows
    .map(
      ([label, value]) => `
        <div class="definition-row">
          <span>${label}</span>
          <strong>${value}</strong>
        </div>
      `
    )
    .join("");
}

function renderLevels(targetId, values, options = {}) {
  const target = document.getElementById(targetId);
  const selectedPrice = Number(options.selectedPrice || 0);
  if (!values.length) {
    target.innerHTML = `<div class="empty-state">暂无可用价位。</div>`;
    return;
  }
  target.innerHTML = values
    .map(
      (value, index) => {
        const selected = isSamePrice(value, selectedPrice);
        return `
        <div class="level-chip ${selected ? "selected" : ""}">
          <div class="level-chip-meta">
            <span>价位 ${index + 1}</span>
            ${selected ? '<em class="level-flag">当前选中</em>' : ""}
          </div>
          <strong>${fmtMoney(value)}</strong>
        </div>
      `;
      }
    )
    .join("");
}

function renderPosition(snapshot) {
  const target = document.getElementById("position-card");
  const position = snapshot.position;

  if (!position) {
    target.innerHTML = `
      <div class="empty-state">
        当前没有持仓，机器人正在等待首仓挂单或新的触发信号。
      </div>
    `;
    return;
  }

  target.innerHTML = `
    <div class="position-hero">
      <div>
        <p class="panel-kicker">实时暴露</p>
        <h3>${fmtMoney(position.notional)}</h3>
      </div>
      <div class="position-side">${translateSide(position.side)}</div>
    </div>
    <div class="definition-list">
      <div class="definition-row"><span>开仓价</span><strong>${fmtMoney(position.entry_price)}</strong></div>
      <div class="definition-row"><span>标记价</span><strong>${fmtMoney(position.mark_price)}</strong></div>
      <div class="definition-row"><span>合约数量</span><strong>${qty.format(position.contracts)}</strong></div>
      <div class="definition-row"><span>浮动盈亏</span><strong class="${signClass(position.unrealized_pnl)}">${fmtMoney(position.unrealized_pnl)}</strong></div>
      <div class="definition-row"><span>收益率</span><strong class="${signClass(position.percentage)}">${fmtPct(position.percentage)}</strong></div>
      <div class="definition-row"><span>强平价</span><strong>${position.liquidation_price ? fmtMoney(position.liquidation_price) : "--"}</strong></div>
      <div class="definition-row"><span>距强平</span><strong>${fmtPct(position.distance_to_liquidation_pct)}</strong></div>
    </div>
  `;
}

function renderLadder(snapshot) {
  const target = document.getElementById("ladder-list");
  target.innerHTML = snapshot.ladder
    .map((item) => {
      const progress = (item.layer / snapshot.strategy.max_layers) * 100;
      return `
        <div class="ladder-item">
          <div class="ladder-head">
            <div>
              <strong>第 ${item.layer} 层</strong>
              <div class="panel-note">${translateState(item.state)} | x${fmtNumber(item.multiplier, 2)}</div>
            </div>
            <div>
              <strong>${fmtMoney(item.margin_estimate)}</strong>
              <div class="panel-note">保证金估算</div>
            </div>
          </div>
          <div class="progress-track">
            <div class="progress-bar" style="width:${progress}%"></div>
          </div>
          <div class="action-meta">
            <span>数量 ${qty.format(item.amount_estimate)}</span>
            <span>预估价 ${item.projected_price ? fmtMoney(item.projected_price) : "--"}</span>
            <span>${translateSource(item.price_source)}</span>
          </div>
        </div>
      `;
    })
    .join("");
}

function renderOrders(snapshot) {
  const summary = document.getElementById("orders-summary");
  summary.innerHTML = `
    <div class="definition-list">
      <div class="definition-row"><span>总挂单数</span><strong>${snapshot.orders.count}</strong></div>
      <div class="definition-row"><span>加仓单</span><strong>${snapshot.orders.add_count}</strong></div>
      <div class="definition-row"><span>减仓单</span><strong>${snapshot.orders.reduce_count}</strong></div>
    </div>
  `;

  const body = document.getElementById("orders-body");
  if (!snapshot.orders.items.length) {
    body.innerHTML = `<tr><td colspan="5" class="empty-state">当前没有挂单。</td></tr>`;
    return;
  }

  body.innerHTML = snapshot.orders.items
    .map(
      (item) => `
        <tr>
          <td>${translateSide(item.side)}</td>
          <td>${translateOrderType(item)}</td>
          <td>${fmtMoney(item.price)}</td>
          <td>${qty.format(item.amount)}</td>
          <td>${fmtPct(item.distance_pct)}</td>
        </tr>
      `
    )
    .join("");
}

function renderEvents(snapshot) {
  const target = document.getElementById("event-tape");
  if (!snapshot.events.length) {
    target.innerHTML = `<div class="empty-state">还没有记录到状态变化。</div>`;
    return;
  }
  target.innerHTML = snapshot.events
    .map(
      (event) => `
        <div class="event-item">
          <small>${event.timestamp} | ${event.type}</small>
          <h3>${event.title}</h3>
          <p>${event.detail}</p>
        </div>
      `
    )
    .join("");
}

function renderTrades(snapshot) {
  const body = document.getElementById("trades-body");
  if (!snapshot.recent_trades.length) {
    body.innerHTML = `<tr><td colspan="6" class="empty-state">暂无私有成交记录。</td></tr>`;
    return;
  }
  body.innerHTML = snapshot.recent_trades
    .map(
      (trade) => `
        <tr>
          <td>${trade.timestamp || "--"}</td>
          <td>${translateSide(trade.side)}</td>
          <td>${fmtMoney(trade.price)}</td>
          <td>${qty.format(trade.amount)}</td>
          <td>${fmtMoney(trade.cost)}</td>
          <td>${trade.fee ? fmtMoney(trade.fee) : "--"}</td>
        </tr>
      `
    )
    .join("");
}

function renderWarnings(snapshot) {
  const strip = document.getElementById("warning-strip");
  const warnings = snapshot.server.warnings || [];
  if (!warnings.length) {
    strip.classList.remove("active");
    strip.textContent = "";
    return;
  }
  strip.classList.add("active");
  strip.textContent = `数据告警：${warnings.join(" | ")}`;
}

function renderCharts(snapshot) {
  const historyPoints = snapshot.history.points || [];
  renderChart(
    "equity-chart",
    [
      {
        values: historyPoints.map((point) => Number(point.equity_estimate || 0)),
        color: "#7fd2ff",
      },
    ],
    { labels: [historyPoints[0]?.timestamp || "", historyPoints[historyPoints.length - 1]?.timestamp || ""] }
  );

  renderChart(
    "pnl-chart",
    [
      {
        values: historyPoints.map((point) => Number(point.unrealized_pnl || 0)),
        color: "#ffd78c",
      },
    ],
    { labels: [historyPoints[0]?.timestamp || "", historyPoints[historyPoints.length - 1]?.timestamp || ""] }
  );

  const pricePoints = snapshot.market.price_series.points || [];
  renderChart(
    "price-chart",
    [
      {
        values: pricePoints.map((point) => Number(point.close || 0)),
        color: "#95a8ff",
      },
      {
        values: pricePoints.map((point) => Number(point.ema_fast || 0)),
        color: "#7fd2ff",
      },
      {
        values: pricePoints.map((point) => Number(point.ema_slow || 0)),
        color: "#ffd78c",
      },
    ],
    { labels: [pricePoints[0]?.timestamp || "", pricePoints[pricePoints.length - 1]?.timestamp || ""] }
  );
}

function renderHeader(snapshot) {
  setText("last-update", snapshot.timestamp || "--");
  setText("refresh-cadence", `${snapshot.server.refresh_ttl_sec || 5} 秒`);
  setText("equity-note", `24 小时已实现 ${fmtMoney(snapshot.performance.realized_pnl_24h)}`);
  setText("equity-current", fmtMoney(snapshot.performance.equity_estimate));
  setText("pnl-current", fmtMoney(snapshot.performance.unrealized_pnl));
  setText("price-current", fmtMoney(snapshot.market.indicators.price));
  setText("ladder-note", `已完成 ${snapshot.runtime.layer || 0} 层 / 最大 ${snapshot.strategy.max_layers} 层`);

  const modePill = document.getElementById("mode-pill");
  modePill.textContent = `${translateMode(snapshot.strategy.mode)} | ${snapshot.strategy.symbol}`;

  const stalePill = document.getElementById("stale-pill");
  stalePill.textContent = snapshot.server.stale ? "缓存快照" : "实时快照";
  stalePill.className = snapshot.server.stale ? "ghost-pill negative" : "ghost-pill";

  const signalBadge = document.getElementById("signal-badge");
  signalBadge.textContent = `${translateSignal(snapshot.market.indicators.signal)} | ${translateTrend(snapshot.market.indicators.trend)}`;
}

function applySnapshot(snapshot) {
  const pricing = snapshot.next_action?.pricing || null;
  renderHeader(snapshot);
  renderMetrics(snapshot);
  renderTradeAnalytics(snapshot);
  renderRuntimeFlags(snapshot);
  renderNextAction(snapshot);
  renderIndicators(snapshot);
  renderLevels("support-list", snapshot.market.support_resistance.support || [], {
    selectedPrice: pricing?.reference_type === "support" ? pricing.reference_price : 0,
  });
  renderLevels("resistance-list", snapshot.market.support_resistance.resistance || [], {
    selectedPrice: pricing?.reference_type === "resistance" ? pricing.reference_price : 0,
  });
  renderPosition(snapshot);
  renderLadder(snapshot);
  renderOrders(snapshot);
  renderEvents(snapshot);
  renderTrades(snapshot);
  renderWarnings(snapshot);
  renderCharts(snapshot);
}

async function fetchSnapshot(force = false) {
  const response = await fetch(`/api/dashboard${force ? "?force=1" : ""}`, {
    cache: "no-store",
  });
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }
  return response.json();
}

async function refreshDashboard(force = false) {
  try {
    const snapshot = await fetchSnapshot(force);
    applySnapshot(snapshot);
  } catch (error) {
    const strip = document.getElementById("warning-strip");
    strip.classList.add("active");
    strip.textContent = `面板刷新失败：${error.message}`;
  }
}

function startAutoRefresh() {
  refreshDashboard(true);
  state.refreshTimer = window.setInterval(() => refreshDashboard(false), 2000);
}

document.getElementById("refresh-button")?.addEventListener("click", () => {
  refreshDashboard(true);
});

startAutoRefresh();
