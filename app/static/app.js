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
      const title = {
        contract_ping: "合约 ping",
        spot_bbo: "现货 BBO",
        spot_trades: "现货 trades",
        spot_l2: "现货 L2",
      }[stream];
      const p95 = item ? item.p95_ms : null;
      return `
        <article class="card">
          <div class="card-title"><span>${title}</span><span>${item?.metric_type || "-"}</span></div>
          <div class="metric ${quality(p95)}">${fmt(p95)} ms</div>
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
        <td>${item.stream}</td>
        <td>${fmt(item.p50_ms)}</td>
        <td>${fmt(item.p95_ms)}</td>
        <td>${fmt(item.p99_ms)}</td>
        <td>${fmt(item.max_ms)}</td>
      </tr>
    `,
    )
    .join("");
}

function renderIncidents(items) {
  const el = document.querySelector("#incidents");
  if (!items.length) {
    el.innerHTML = `<div class="incident"><div class="time">暂无异常</div></div>`;
    return;
  }
  el.innerHTML = items
    .map(
      (item) => `
      <div class="incident ${item.severity}">
        <div><b>${item.stream}</b> ${item.type}</div>
        <div>${item.message}</div>
        <div class="time">${timeLabel(item.ts_ms)} ${item.severity}</div>
      </div>
    `,
    )
    .join("");
}

function buildChart(items) {
  const streams = [...new Set(items.map((item) => item.stream))];
  const labels = [...new Set(items.map((item) => item.ts_ms))].sort((a, b) => a - b);
  const datasets = streams.map((stream) => {
    const byTs = new Map(items.filter((item) => item.stream === stream).map((item) => [item.ts_ms, item.p95_ms]));
    return {
      label: stream,
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
