const state = {
  minutes: 60,
  latest: new Map(),
  chart: null,
};

const colors = {
  contract_ping: "#ffd133",
  spot_bbo: "#2dd4bf",
  spot_trades: "#38bdf8",
  spot_l2: "#fb923c",
};

const streamNames = {
  contract_ping: "合约 ping",
  spot_bbo: "现货 BBO",
  spot_trades: "现货 trades",
  spot_l2: "现货 L2",
};

const metricNames = {
  rtt: "RTT",
  message_gap: "消息间隔",
};

const severityNames = {
  info: "提示",
  warning: "警告",
  error: "错误",
};

function fmt(value, digits = 1) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "-";
  return Number(value).toFixed(digits);
}

function timeLabel(ts) {
  return new Date(ts).toLocaleTimeString("zh-CN", { hour12: false });
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

async function getJson(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`${url} ${res.status}`);
  return res.json();
}

function renderCards(items) {
  const el = document.querySelector("#cards");
  const ordered = ["contract_ping", "spot_bbo", "spot_trades", "spot_l2"];
  const byStream = new Map(items.map((item) => [item.stream, item]));
  el.innerHTML = ordered
    .map((stream) => {
      const item = byStream.get(stream);
      const title = streamNames[stream] || stream;
      const p95 = item ? item.p95_ms : null;
      return `
        <article class="card">
          <div class="card-title"><span>${title}</span><span>${metricNames[item?.metric_type] || item?.metric_type || "-"}</span></div>
          <div class="metric ${quality(p95)}">${fmt(p95)} ms</div>
          <div class="window-note">${windowLabel(item?.window_s)}</div>
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
        <td>${streamNames[item.stream] || item.stream}</td>
        <td>${fmt(item.p50_ms)}</td>
        <td>${fmt(item.p95_ms)}</td>
        <td>${fmt(item.p99_ms)}</td>
        <td>${fmt(item.max_ms)}</td>
      </tr>
    `,
    )
    .join("");
}

function incidentText(item) {
  const stream = streamNames[item.stream] || item.stream;
  const type = {
    connect: "连接成功",
    timeout: "超时",
    text: "收到文本消息",
    gap_spike: "消息间隔尖峰",
    rtt_spike: "RTT 尖峰",
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
    const timeoutText = item.stream === "contract_ping" ? "5 秒内没有收到 pong" : "10 秒内没有收到消息";
    return { title: `${stream} ${type}`, detail: timeoutText };
  }
  if (item.type === "gap_spike") {
    return { title: `${stream} ${type}`, detail: `最大消息间隔 ${fmt(extra.max_ms, 2)} ms` };
  }
  if (item.type === "rtt_spike") {
    return { title: `${stream} ${type}`, detail: `最大 RTT ${fmt(extra.max_ms, 2)} ms` };
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
  const labels = [...new Set(items.map((item) => item.ts_ms))].sort((a, b) => a - b);
  const datasets = streams.map((stream) => {
    const byTs = new Map(items.filter((item) => item.stream === stream).map((item) => [item.ts_ms, item.p95_ms]));
    return {
      label: streamNames[stream] || stream,
      data: labels.map((ts) => byTs.get(ts) ?? null),
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
      labels: labels.map(timeLabel),
      datasets,
    },
    options: {
      animation: false,
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      plugins: {
        legend: { labels: { color: "#d7e1ee" } },
      },
      scales: {
        x: { ticks: { color: "#7d8da3", maxTicksLimit: 10 }, grid: { color: "rgba(29,42,58,0.55)" } },
        y: { ticks: { color: "#7d8da3" }, grid: { color: "rgba(29,42,58,0.55)" } },
      },
    },
  });
}

async function refreshAll() {
  const [status, series, summary, incidents] = await Promise.all([
    getJson("/api/status"),
    getJson(`/api/series?minutes=${state.minutes}`),
    getJson(`/api/summary?minutes=${state.minutes}`),
    getJson(`/api/incidents?minutes=${state.minutes}`),
  ]);

  document.querySelector("#region").textContent = `region: ${status.region}`;
  document.querySelector("#symbol").textContent = `symbol: ${status.symbol}`;
  renderCards(status.latest);
  buildChart(series.items);
  renderSummary(summary.items);
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
      renderCards([...state.latest.values()]);
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
    state.minutes = Number(button.dataset.minutes);
    refreshAll();
  });
});

refreshAll();
connectWs();
setInterval(refreshAll, 30000);
