const state = {
  minutes: 60,
  rangeMode: "preset",
  sinceMs: null,
  untilMs: null,
  streams: [],
  latest: new Map(),
  chart: null,
  lastRange: null,
};

const colors = {
  extended_rest: "#f472b6",
  extended_bbo: "#34d399",
  extended_l2: "#60a5fa",
  extended_trades_payload_lag: "#f97316",
  extended_trades_trade_age: "#fb7185",
  extended_mark: "#14b8a6",
  extended_index: "#e879f9",
  extended_order_place: "#a78bfa",
  extended_order_cancel: "#facc15",
  extended_order_ws: "#22d3ee",
  extended_fill_place: "#ef4444",
  extended_fill_ws: "#f59e0b",
};

const streamMeta = {
  extended_rest: {
    name: "Extended 公共 REST",
    note: "公共 markets REST RTT",
  },
  extended_bbo: {
    name: "Extended BBO",
    note: "BBO 消息时间戳 lag",
  },
  extended_l2: {
    name: "Extended L2 event_lag",
    note: "orderbook 顶层 ts 到本机收到的延迟",
  },
  extended_trades_payload_lag: {
    name: "Extended 成交 payload lag",
    note: "publicTrades 顶层 ts 到本机收到的延迟",
  },
  extended_trades_trade_age: {
    name: "Extended 成交 trade age",
    note: "publicTrades data[].T 到本机收到的延迟",
  },
  extended_mark: {
    name: "Extended 标记价格",
    note: "mark price 消息 lag",
  },
  extended_index: {
    name: "Extended 指数价格",
    note: "index price 消息 lag",
  },
  extended_order_place: {
    name: "Extended 下单 ACK",
    note: "测试网真实下单 REST ACK",
  },
  extended_order_cancel: {
    name: "Extended 撤单 ACK",
    note: "测试网撤单 REST ACK",
  },
  extended_order_ws: {
    name: "Extended 私有回报",
    note: "私有 WebSocket 下单/撤单回报",
  },
  extended_order_test: {
    name: "Extended 下单测试开关",
    note: "启用后采集下单 ACK、撤单 ACK、私有 WebSocket 回报",
  },
  extended_fill_place: {
    name: "Extended 实际成交下单",
    note: "IOC 可成交订单 REST ACK",
  },
  extended_fill_ws: {
    name: "Extended 实际成交回报",
    note: "私有 WebSocket FILLED/PARTIALLY_FILLED 回报",
  },
  extended_fill_test: {
    name: "Extended 实际成交开关",
    note: "启用后发送 IOC 可成交订单，默认只允许测试网",
  },
};

const metricNames = {
  message_gap: "消息间隔",
  payload_lag: "payload lag",
  trade_age: "trade age",
  order_ack: "下单 ACK",
  cancel_ack: "撤单 ACK",
  order_ws_ack: "下单回报",
  cancel_ws_ack: "撤单回报",
  fill_order_ack: "成交下单 ACK",
  fill_ws_ack: "成交回报",
  rest_rtt: "REST RTT",
  event_lag: "event_lag",
};

const severityNames = {
  info: "提示",
  warning: "警告",
  error: "错误",
};

function streamName(stream) {
  return streamMeta[stream]?.name || "未知指标";
}

function streamLabel(stream) {
  return `${streamName(stream)}（${stream}）`;
}

function streamNote(stream) {
  return streamMeta[stream]?.note || stream;
}

function fmt(value, digits = 1) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "-";
  return Number(value).toFixed(digits);
}

function timeLabel(ts) {
  return new Date(ts).toLocaleTimeString("zh-CN", { hour12: false });
}

function axisTimeLabel(ts) {
  const options =
    state.lastRange && state.lastRange.until_ms - state.lastRange.since_ms > 24 * 60 * 60 * 1000
      ? { month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", hour12: false }
      : { hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false };
  return new Date(Number(ts)).toLocaleString("zh-CN", options);
}

function inputDateTime(ms) {
  const date = new Date(ms);
  const local = new Date(date.getTime() - date.getTimezoneOffset() * 60_000);
  return local.toISOString().slice(0, 16);
}

function initRangeInputs() {
  const end = Date.now();
  const start = end - 60 * 60 * 1000;
  document.querySelector("#rangeStart").value = inputDateTime(start);
  document.querySelector("#rangeEnd").value = inputDateTime(end);
}

function quality(value) {
  if (value === null || value === undefined) return "";
  if (value < 50) return "good";
  if (value < 120) return "warn";
  return "bad";
}

function windowLabel(seconds) {
  if (seconds === null || seconds === undefined || Number.isNaN(Number(seconds))) return "最新窗口";
  if (Number(seconds) < 60) return `最近 ${Math.round(Number(seconds))} 秒`;
  return `最近 ${fmt(Number(seconds) / 60, 1)} 分钟`;
}

function valueOrDash(value) {
  return value || "-";
}

async function getJson(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`${url} ${res.status}`);
  return res.json();
}

function rangeQuery() {
  if (state.rangeMode === "custom" && state.sinceMs && state.untilMs) {
    return `since_ms=${state.sinceMs}&until_ms=${state.untilMs}`;
  }
  return `minutes=${state.minutes}`;
}

function renderPlacement(placement) {
  const el = document.querySelector("#placementBar");
  if (!el) return;

  const target = placement?.extended_target || {};
  const match = placement?.az_match || "unknown";
  const badge = {
    same: ["same", "同 AZ"],
    different: ["different", "不同 AZ"],
    cross_cloud: ["neutral", "跨云节点"],
    unknown: ["neutral", "未知"],
  }[match] || ["neutral", "未知"];

  const provider = placement?.provider === "aws" ? "AWS EC2" : placement?.provider === "vultr" ? "Vultr" : "未知节点";
  const currentLines =
    placement?.provider === "aws"
      ? [
          ["Subnet", placement.subnet_id],
          ["AZ", placement.az],
          ["AZ ID", placement.az_id],
          ["VPC", placement.vpc_id],
          ["Private IP", placement.private_ip],
          ["Public IP", placement.public_ip],
        ]
      : [
          ["Provider", provider],
          ["Region", placement?.region],
          ["Private IP", placement?.private_ip],
          ["Public IP", placement?.public_ip],
          ["说明", placement?.note],
        ];

  const fields = currentLines
    .filter(([, value]) => value)
    .map(([label, value]) => `<span><b>${label}</b> ${valueOrDash(value)}</span>`)
    .join("");

  el.innerHTML = `
    <div class="placement-main">
      <span class="placement-badge ${badge[0]}">${badge[1]}</span>
      <span class="placement-title">${provider}</span>
      <span class="placement-fields">${fields}</span>
    </div>
    <div class="placement-target">
      Extended 目标: AWS ${valueOrDash(target.region)} / ${valueOrDash(target.az)} / ${valueOrDash(target.az_id)}
    </div>
  `;
}

function renderActiveStreams(streams) {
  const el = document.querySelector("#activeStreams");
  if (!el) return;

  const ordered = [
    "extended_rest",
    "extended_bbo",
    "extended_l2",
    "extended_trades_payload_lag",
    "extended_trades_trade_age",
    "extended_mark",
    "extended_index",
    "extended_order_test",
    "extended_order_place",
    "extended_order_cancel",
    "extended_order_ws",
    "extended_fill_test",
    "extended_fill_place",
    "extended_fill_ws",
  ].filter((stream) => streams.includes(stream));

  el.innerHTML = `
    <div class="active-streams-title">当前测试 Extended API</div>
    <div class="active-stream-list">
      ${ordered
        .map(
          (stream) => `
            <span class="stream-chip">
              <b>${streamName(stream)}</b>
              <span>${stream}</span>
              <small>${streamNote(stream)}</small>
            </span>
          `,
        )
        .join("")}
    </div>
  `;
}

function renderCards(items, activeStreams = []) {
  const el = document.querySelector("#cards");
  const preferred = [
    "extended_rest",
    "extended_bbo",
    "extended_l2",
    "extended_trades_payload_lag",
    "extended_trades_trade_age",
    "extended_mark",
    "extended_index",
    "extended_order_place",
    "extended_order_cancel",
    "extended_order_ws",
    "extended_fill_place",
    "extended_fill_ws",
  ];
  const byStream = new Map(items.map((item) => [item.stream, item]));
  const active = new Set([...activeStreams, ...byStream.keys()]);
  const ordered = preferred.filter((stream) => active.has(stream));
  el.innerHTML = ordered
    .map((stream) => {
      const item = byStream.get(stream);
      const title = streamName(stream);
      const p95 = item ? item.p95_ms : null;
      return `
        <article class="card">
          <div class="card-title"><span>${title}</span><span>${metricNames[item?.metric_type] || item?.metric_type || "等待采样"}</span></div>
          <div class="stream-key">${stream}</div>
          <div class="metric ${quality(p95)}">${fmt(p95)} ms</div>
          <div class="window-note">${windowLabel(item?.window_s)}</div>
          <div class="stream-note">${streamNote(stream)}</div>
          <div class="submetrics">
            <span>p50 <b>${fmt(item?.p50_ms)}</b></span>
            <span>p99 <b>${fmt(item?.p99_ms)}</b></span>
            <span>max <b>${fmt(item?.max_ms)}</b></span>
          </div>
          <div class="submetrics">
            <span>消息 <b>${item?.messages ?? "-"}</b></span>
            <span>重连 <b>${item?.reconnects ?? "-"}</b></span>
            <span>超时 <b>${item?.timeouts ?? "-"}</b></span>
          </div>
        </article>
      `;
    })
    .join("");
}

function renderSummary(items) {
  document.querySelector("#summaryRows").innerHTML = items
    .map(
      (item) => `
      <tr>
        <td>${streamLabel(item.stream)}</td>
        <td>${fmt(item.p50_ms)}</td>
        <td>${fmt(item.p95_ms)}</td>
        <td>${fmt(item.p99_ms)}</td>
        <td>${fmt(item.max_ms)}</td>
      </tr>
    `,
    )
    .join("");
}

function renderStability(items) {
  const el = document.querySelector("#stabilityRows");
  if (!el) return;
  if (!items.length) {
    el.innerHTML = `<tr><td colspan="6">暂无稳定性数据</td></tr>`;
    return;
  }
  el.innerHTML = items
    .map(
      (item) => `
      <tr>
        <td>
          <div>${streamName(item.stream)}</div>
          <small>${item.stream}</small>
        </td>
        <td>${fmt(item.jitter_ms)}</td>
        <td>${fmt(item.max_ms)}</td>
        <td>${fmt(item.reconnects_per_hour, 2)}</td>
        <td>${fmt(item.timeouts_per_hour, 2)}</td>
        <td>${item.failure_events ?? 0}</td>
      </tr>
    `,
    )
    .join("");
}

function incidentText(item) {
  const stream = streamLabel(item.stream);
  const type = {
    connect: "连接成功",
    timeout: "超时",
    text: "收到文本消息",
    gap_spike: "消息间隔尖峰",
    rtt_spike: "RTT 尖峰",
    order_test_error: "模拟下单失败",
    order_test_spike: "模拟下单尖峰",
    extended_rest_error: "Extended REST 失败",
    extended_order_error: "Extended 下单测试失败",
    extended_fill_error: "Extended 实际成交测试失败",
    extended_lag_spike: "Extended 延迟尖峰",
    config_error: "配置错误",
    error: "连接错误",
  }[item.type] || item.type;

  const extra = (() => {
    try {
      return item.extra_json ? JSON.parse(item.extra_json) : {};
    } catch {
      return {};
    }
  })();

  if (item.type === "connect") {
    const connectMs = extra.connect_ms;
    return { title: `${stream} ${type}`, detail: connectMs ? `连接耗时 ${fmt(connectMs, 2)} ms` : item.message };
  }
  if (item.type === "timeout") {
    return { title: `${stream} ${type}`, detail: item.message || "请求或消息接收超时" };
  }
  if (item.type === "gap_spike") {
    return { title: `${stream} ${type}`, detail: `最大消息间隔 ${fmt(extra.max_ms, 2)} ms` };
  }
  if (item.type === "rtt_spike") {
    return { title: `${stream} ${type}`, detail: `最大 RTT ${fmt(extra.max_ms, 2)} ms` };
  }
  if (item.type === "order_test_error") {
    return { title: `${stream} ${type}`, detail: item.message };
  }
  if (item.type === "order_test_spike") {
    return { title: `${stream} ${type}`, detail: `最大 ACK 耗时 ${fmt(extra.max_ms, 2)} ms` };
  }
  if (item.type === "extended_rest_error") {
    return { title: `${stream} ${type}`, detail: item.message };
  }
  if (item.type === "extended_order_error") {
    return { title: `${stream} ${type}`, detail: item.message };
  }
  if (item.type === "extended_fill_error") {
    return { title: `${stream} ${type}`, detail: item.message };
  }
  if (item.type === "extended_lag_spike") {
    return { title: `${stream} ${type}`, detail: `最大延迟 ${fmt(extra.max_ms, 2)} ms` };
  }
  if (item.type === "error") {
    return { title: `${stream} ${type}`, detail: item.message };
  }
  return { title: `${stream} ${type}`, detail: item.message };
}

function renderIncidents(items) {
  const el = document.querySelector("#incidents");
  if (!items.length) {
    el.innerHTML = `<div class="incident"><div class="time">暂无异常</div></div>`;
    return;
  }
  el.innerHTML = items
    .map((item) => {
      const text = incidentText(item);
      return `
      <div class="incident ${item.severity}">
        <div><b>${text.title}</b></div>
        <div>${text.detail}</div>
        <div class="time">${timeLabel(item.ts_ms)} ${severityNames[item.severity] || item.severity}</div>
      </div>
    `;
    })
    .join("");
}

function buildChart(items) {
  if (!window.Chart) return;

  const streams = [...new Set(items.map((item) => item.stream))];
  const datasets = streams.map((stream) => {
    const rows = items.filter((item) => item.stream === stream).sort((a, b) => a.ts_ms - b.ts_ms);
    return {
      label: streamLabel(stream),
      data: rows.map((item) => ({
        x: item.ts_ms,
        y: item.p95_ms,
        max: item.max_ms,
        p50: item.p50_ms,
        p99: item.p99_ms,
        messages: item.messages,
      })),
      borderColor: colors[stream] || "#d7e1ee",
      backgroundColor: colors[stream] || "#d7e1ee",
      borderWidth: 2,
      pointRadius: 0,
      tension: 0.25,
      spanGaps: true,
    };
  });

  const ctx = document.querySelector("#latencyChart");
  if (state.chart) state.chart.destroy();
  state.chart = new Chart(ctx, {
    type: "line",
    data: {
      datasets,
    },
    options: {
      animation: false,
      responsive: true,
      maintainAspectRatio: false,
      parsing: false,
      normalized: true,
      interaction: { mode: "index", intersect: false },
      plugins: {
        legend: { labels: { color: "#d7e1ee" } },
        tooltip: {
          callbacks: {
            title: (items) => (items[0] ? axisTimeLabel(items[0].parsed.x) : ""),
            label: (item) => {
              const raw = item.raw || {};
              return `${item.dataset.label}: p95 ${fmt(raw.y)} ms / max ${fmt(raw.max)} ms / p99 ${fmt(raw.p99)} ms`;
            },
          },
        },
      },
      scales: {
        x: {
          type: "linear",
          ticks: {
            color: "#7d8da3",
            maxTicksLimit: 9,
            callback: (value) => axisTimeLabel(value),
          },
          grid: { color: "rgba(29,42,58,0.55)" },
        },
        y: { ticks: { color: "#7d8da3" }, grid: { color: "rgba(29,42,58,0.55)" } },
      },
    },
  });
}

async function refreshAll() {
  const query = rangeQuery();
  const [status, series, summary, stability, incidents] = await Promise.all([
    getJson("/api/status"),
    getJson(`/api/series?${query}`),
    getJson(`/api/summary?${query}`),
    getJson(`/api/stability?${query}`),
    getJson(`/api/incidents?${query}`),
  ]);

  state.lastRange = series.range;
  document.querySelector("#region").textContent = `region: ${status.region}`;
  document.querySelector("#symbol").textContent =
    `市场: ${status.extended_market} / 环境: ${status.extended_env}`;
  renderPlacement(status.placement);
  state.streams = status.streams;
  renderActiveStreams(status.streams);
  renderCards(status.latest, state.streams);
  buildChart(series.items);
  renderSummary(summary.items);
  renderStability(stability.items);
  renderIncidents(incidents.items);
}

function connectWs() {
  const dot = document.querySelector("#connDot");
  const text = document.querySelector("#connText");
  const protocol = location.protocol === "https:" ? "wss" : "ws";
  const ws = new WebSocket(`${protocol}://${location.host}/ws`);

  ws.onopen = () => {
    dot.className = "dot ok";
    text.textContent = "实时";
  };

  ws.onmessage = (event) => {
    const payload = JSON.parse(event.data);
    if (payload.type === "sample") {
      state.latest.set(payload.data.stream, payload.data);
      renderCards([...state.latest.values()], state.streams);
    }
    if (payload.type === "incident") {
      refreshAll();
    }
  };

  ws.onclose = () => {
    dot.className = "dot bad";
    text.textContent = "断开，重连中";
    setTimeout(connectWs, 2000);
  };
}

document.querySelectorAll("[data-minutes]").forEach((button) => {
  button.addEventListener("click", () => {
    document.querySelectorAll("[data-minutes]").forEach((node) => node.classList.remove("active"));
    button.classList.add("active");
    state.rangeMode = "preset";
    state.minutes = Number(button.dataset.minutes);
    refreshAll();
  });
});

document.querySelector("#applyRange").addEventListener("click", () => {
  const startValue = document.querySelector("#rangeStart").value;
  const endValue = document.querySelector("#rangeEnd").value;
  const start = startValue ? new Date(startValue).getTime() : NaN;
  const end = endValue ? new Date(endValue).getTime() : NaN;
  if (Number.isNaN(start) || Number.isNaN(end)) return;
  state.rangeMode = "custom";
  state.sinceMs = start;
  state.untilMs = end;
  document.querySelectorAll("[data-minutes]").forEach((node) => node.classList.remove("active"));
  refreshAll();
});

initRangeInputs();
refreshAll();
connectWs();
setInterval(refreshAll, 30000);
