/* Naval Battle 艦隊管制 GUI */
"use strict";

const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => Array.from(document.querySelectorAll(sel));

const STATE_LABELS = {
  NEED_APPROVAL: "承認待ち",
  NEED_INPUT: "入力待ち",
  RUNNING: "実行中",
  ENQUEUED: "待機中",
  DONE: "完了",
  FAILED: "失敗",
  BUDGET_DENIED: "予算超過",
  CANCELLED: "中断",
};

const SHIP_LABELS = {
  CVL: "軽空母 CVL（索敵・要約）",
  DD: "駆逐艦 DD（実装）",
  CL: "軽巡 CL（テスト）",
  CVB: "装甲空母 CVB（検証）",
  CA: "重巡 CA（艦隊指揮）",
  BB: "戦艦 BB（決戦兵力）",
};

let missions = [];
let knownNeedIds = new Set();
let notifyEnabled = false;

/* ---------------- helpers ---------------- */

async function api(path, opts = {}) {
  const resp = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...opts,
  });
  let body = null;
  try { body = await resp.json(); } catch (e) { /* empty body */ }
  if (!resp.ok) {
    const detail = body && body.detail
      ? (typeof body.detail === "string" ? body.detail : JSON.stringify(body.detail))
      : `HTTP ${resp.status}`;
    throw new Error(detail);
  }
  return body;
}

function toast(msg, isErr = false) {
  const t = $("#toast");
  t.textContent = msg;
  t.className = isErr ? "err" : "";
  t.hidden = false;
  clearTimeout(t._timer);
  t._timer = setTimeout(() => { t.hidden = true; }, 4000);
}

function stateBadge(status) {
  const label = STATE_LABELS[status] || status;
  return `<span class="badge st-${status}">${label}</span>`;
}

function fmtTime(ts) {
  if (!ts) return "-";
  const d = new Date(ts * 1000);
  return d.toLocaleString("ja-JP", { month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit" });
}

function esc(s) {
  return String(s ?? "").replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  }[c]));
}

/* ---------------- tabs ---------------- */

$$("#tabs button").forEach((btn) => {
  btn.addEventListener("click", () => {
    $$("#tabs button").forEach((b) => b.classList.remove("active"));
    $$(".tab").forEach((s) => s.classList.remove("active"));
    btn.classList.add("active");
    $(`#tab-${btn.dataset.tab}`).classList.add("active");
    const loader = TAB_LOADERS[btn.dataset.tab];
    if (loader) loader();
  });
});

/* ---------------- notifications ---------------- */

$("#notify-btn").addEventListener("click", async () => {
  if (!("Notification" in window)) { toast("このブラウザは通知に対応していません", true); return; }
  const perm = await Notification.requestPermission();
  notifyEnabled = perm === "granted";
  $("#notify-btn").textContent = notifyEnabled ? "🔔 通知オン" : "🔕 通知オフ";
  if (notifyEnabled) toast("承認・入力待ちが発生するとブラウザ通知が届きます");
});

function notifyNeeds(rows) {
  const needs = rows.filter((m) => m.status === "NEED_APPROVAL" || m.status === "NEED_INPUT");
  const fresh = needs.filter((m) => !knownNeedIds.has(m.mission_id));
  knownNeedIds = new Set(needs.map((m) => m.mission_id));
  if (notifyEnabled && fresh.length) {
    const first = fresh[0];
    new Notification("Naval Battle: 対応が必要です", {
      body: `${STATE_LABELS[first.status]}: ${first.mission_id}` + (fresh.length > 1 ? ` 他${fresh.length - 1}件` : ""),
    });
  }
}

/* ---------------- live updates (SSE) ---------------- */

function startEvents() {
  const es = new EventSource("/api/events");
  es.addEventListener("missions", (ev) => {
    $("#conn-status").textContent = "● ライブ";
    $("#conn-status").style.color = "var(--green)";
    missions = JSON.parse(ev.data).missions;
    notifyNeeds(missions);
    renderDashboard();
    renderPending();
  });
  es.onerror = () => {
    $("#conn-status").textContent = "○ 再接続中…";
    $("#conn-status").style.color = "var(--yellow)";
  };
}

/* ---------------- dashboard ---------------- */

function renderDashboard() {
  const filter = $("#dash-filter").value.trim().toLowerCase();
  let rows = missions;
  if (filter) {
    rows = rows.filter((m) =>
      m.mission_id.toLowerCase().includes(filter) ||
      m.status.toLowerCase().includes(filter) ||
      (STATE_LABELS[m.status] || "").includes(filter));
  }

  const needCount = missions.filter((m) => m.status === "NEED_APPROVAL" || m.status === "NEED_INPUT").length;
  const alert = $("#dash-alert");
  if (needCount) {
    alert.hidden = false;
    alert.innerHTML = `⚠️ <strong>${needCount}件</strong>のミッションがあなたの対応（承認・入力）を待っています。「承認・入力」タブで対応してください。`;
  } else {
    alert.hidden = true;
  }
  const cnt = $("#pending-count");
  cnt.hidden = !needCount;
  cnt.textContent = needCount;

  const box = $("#mission-cards");
  if (!rows.length) {
    box.innerHTML = `<p class="hint">ミッションはありません。「新規ミッション」タブから投入できます。</p>`;
    return;
  }
  box.innerHTML = rows.map((m) => `
    <div class="card ${m.status.startsWith("NEED_") ? "need" : ""}">
      <h3>${esc(m.mission_id)}</h3>
      ${stateBadge(m.status)}
      <div class="meta">
        艦種: ${esc(m.ship_class || "-")} ／ タスク: ${esc(m.task_id || "-")}<br>
        更新: ${fmtTime(m.updated_at)}
        ${m.confidence != null ? `／ 確信度: ${m.confidence}` : ""}
      </div>
    </div>`).join("");
}

$("#dash-filter").addEventListener("input", renderDashboard);
$("#dash-refresh").addEventListener("click", async () => {
  try {
    missions = (await api("/api/missions")).missions;
    renderDashboard();
    renderPending();
  } catch (e) { toast(e.message, true); }
});

/* ---------------- new mission ---------------- */

async function loadFormations() {
  const sel = $("#doctrine-select");
  try {
    const { formations } = await api("/api/fleet/formations");
    const names = Object.keys(formations);
    sel.innerHTML = names.map((n) =>
      `<option value="${esc(n)}" ${n === "standard_full" ? "selected" : ""}>${esc(n)}</option>`).join("");
    const updateDesc = () => {
      const f = formations[sel.value];
      $("#doctrine-desc").textContent = f ? `${f.description}（ステージ: ${f.stages.join(" → ")}）` : "";
    };
    sel.addEventListener("change", updateDesc);
    updateDesc();
  } catch (e) {
    sel.innerHTML = `<option value="standard_full">standard_full</option>`;
  }
}

$("#new-form").addEventListener("submit", async (ev) => {
  ev.preventDefault();
  const form = ev.target;
  const btn = form.querySelector("button[type=submit]");
  const result = $("#new-result");
  btn.disabled = true;
  result.textContent = "投入中…（しばらくお待ちください）";
  result.className = "result";
  try {
    const body = {
      objective: form.objective.value,
      repo_url: form.repo_url.value,
      doctrine: form.doctrine.value,
      task_id: form.task_id.value,
    };
    const res = await api("/api/missions", { method: "POST", body: JSON.stringify(body) });
    result.textContent = `✅ 投入しました（タスクID: ${res.task_id}）。進行状況は「タスク」タブで確認できます。`;
    result.className = "result ok";
    form.objective.value = "";
  } catch (e) {
    result.textContent = `❌ 投入に失敗しました: ${e.message}`;
    result.className = "result err";
  } finally {
    btn.disabled = false;
  }
});

/* ---------------- pending (approve / input) ---------------- */

function renderPending() {
  const rows = missions.filter((m) => m.status === "NEED_APPROVAL" || m.status === "NEED_INPUT");
  $("#pending-empty").hidden = rows.length > 0;
  const box = $("#pending-list");
  const open = new Set($$("#pending-list details[open]").map((d) => d.dataset.mid));
  box.innerHTML = rows.map((m) => `
    <details class="panel" data-mid="${esc(m.mission_id)}" ${open.has(m.mission_id) ? "open" : ""}>
      <summary>${stateBadge(m.status)} ${esc(m.mission_id)}
        <span class="hint">（艦種 ${esc(m.ship_class || "-")} ／ ${fmtTime(m.updated_at)}）</span></summary>
      <div class="pending-body" data-mid="${esc(m.mission_id)}" data-status="${esc(m.status)}">
        <p class="hint">読み込み中…</p>
      </div>
    </details>`).join("");

  $$("#pending-list details").forEach((d) => {
    d.addEventListener("toggle", () => { if (d.open) loadPendingDetail(d.dataset.mid); });
    if (d.open) loadPendingDetail(d.dataset.mid);
  });
}

async function loadPendingDetail(mid) {
  const body = $(`.pending-body[data-mid="${CSS.escape(mid)}"]`);
  if (!body || body.dataset.loaded) return;
  body.dataset.loaded = "1";
  const status = body.dataset.status;
  let commsHtml = "";
  try {
    const detail = await api(`/api/missions/${encodeURIComponent(mid)}`);
    if (detail.comms && detail.comms.length) {
      commsHtml = `<div class="comms">` + detail.comms.slice(-8).map((c) =>
        `<div class="c"><span class="who">${esc(c.sender)}</span>${esc(c.content).slice(0, 300)}</div>`).join("") + `</div>`;
    }
  } catch (e) { /* comms は補助情報なので失敗しても続行 */ }

  if (status === "NEED_APPROVAL") {
    body.innerHTML = `
      ${commsHtml}
      <label>コメント（任意）<input class="note" placeholder="例: 問題ないので進めてください"></label>
      <button class="ok-btn act-approve">✅ 承認して再開</button>
      <button class="danger act-reject">❌ 却下する</button>
      <p class="result"></p>`;
  } else {
    body.innerHTML = `
      ${commsHtml}
      <label>AI への回答<textarea class="note" rows="4" placeholder="質問への回答や追加の指示を入力してください"></textarea></label>
      <button class="primary act-input">📨 回答を送信して再開</button>
      <p class="result"></p>`;
  }

  const note = body.querySelector(".note");
  const result = body.querySelector(".result");
  const act = async (fn, okMsg) => {
    body.querySelectorAll("button").forEach((b) => (b.disabled = true));
    try {
      await fn();
      result.textContent = okMsg;
      result.className = "result ok";
      setTimeout(() => $("#dash-refresh").click(), 800);
    } catch (e) {
      result.textContent = `❌ ${e.message}`;
      result.className = "result err";
      body.querySelectorAll("button").forEach((b) => (b.disabled = false));
    }
  };

  body.querySelector(".act-approve")?.addEventListener("click", () =>
    act(() => api(`/api/missions/${encodeURIComponent(mid)}/approve`,
      { method: "POST", body: JSON.stringify({ approve: true, note: note.value }) }),
      "✅ 承認しました。ミッションを再開します。"));
  body.querySelector(".act-reject")?.addEventListener("click", () =>
    act(() => api(`/api/missions/${encodeURIComponent(mid)}/approve`,
      { method: "POST", body: JSON.stringify({ approve: false, note: note.value }) }),
      "却下しました。"));
  body.querySelector(".act-input")?.addEventListener("click", () => {
    if (!note.value.trim()) { toast("回答を入力してください", true); return; }
    act(() => api(`/api/missions/${encodeURIComponent(mid)}/input`,
      { method: "POST", body: JSON.stringify({ message: note.value }) }),
      "📨 回答を送信しました。ミッションを再開します。");
  });
}

/* ---------------- tasks ---------------- */

async function loadTasks() {
  const box = $("#task-list");
  box.innerHTML = `<p class="hint">読み込み中…</p>`;
  try {
    const { tasks } = await api("/api/tasks");
    if (!tasks.length) {
      box.innerHTML = `<p class="hint">タスクはありません。</p>`;
      return;
    }
    box.innerHTML = tasks.map((t) => `
      <div class="card ${t.status.startsWith("NEED_") ? "need" : ""}" data-tid="${esc(t.task_id)}" style="cursor:pointer">
        <h3>${esc(t.task_id)}</h3>
        ${stateBadge(t.status)}
        <div class="meta">ステージ: ${esc(t.current_stage || "-")} ／ 予算月: ${esc(t.budget_month || "-")}<br>更新: ${fmtTime(t.updated_at)}</div>
      </div>`).join("");
    $$("#task-list .card").forEach((c) =>
      c.addEventListener("click", () => loadTaskDetail(c.dataset.tid)));
  } catch (e) {
    box.innerHTML = `<p class="result err">読み込みに失敗しました: ${esc(e.message)}</p>`;
  }
}

async function loadTaskDetail(tid) {
  const panel = $("#task-detail");
  panel.hidden = false;
  panel.innerHTML = `<p class="hint">読み込み中…</p>`;
  try {
    const t = await api(`/api/tasks/${encodeURIComponent(tid)}`);
    const stagesRows = t.stages.map((s) => `
      <tr>
        <td>${esc(s.stage)}</td>
        <td>${stateBadge(s.status)}</td>
        <td>${esc(s.mission_id || "-")}</td>
        <td>${s.confidence != null ? s.confidence : "-"}</td>
        <td>${fmtTime(s.updated_at)}</td>
        <td>${s.mission_id ? `<button class="ghost art-btn" data-mid="${esc(s.mission_id)}">📦 成果物</button>` : ""}</td>
      </tr>`).join("");
    panel.innerHTML = `
      <h2>${esc(t.task_id)} ${stateBadge(t.status)}</h2>
      <p class="hint">現在ステージ: ${esc(t.current_stage || "-")} ／ 予算月: ${esc(t.budget_month || "-")}
        ${t.abort_reason ? `／ 中断理由: ${esc(t.abort_reason)}` : ""}</p>
      <table class="stage-table">
        <tr><th>ステージ</th><th>状態</th><th>ミッションID</th><th>確信度</th><th>更新</th><th></th></tr>
        ${stagesRows}
      </table>
      <div class="toolbar" style="margin-top:14px">
        <button class="danger" id="task-abort">⛔ タスクを中断</button>
        <button id="task-retry">🔁 失敗をリトライ</button>
      </div>
      <label>艦隊指揮（CA）への指示
        <textarea id="ca-text" rows="3" placeholder="例: Execute: DD_SONNET / Skip: BB / Confidence: 75"></textarea>
      </label>
      <button class="primary" id="ca-send">📡 指示を送る</button>
      <div id="task-artifacts"></div>
      <p class="result" id="task-result"></p>`;

    const result = $("#task-result");
    const run = async (fn, ok) => {
      try {
        await fn();
        result.textContent = ok;
        result.className = "result ok";
        loadTasks();
      } catch (e) {
        result.textContent = `❌ ${e.message}`;
        result.className = "result err";
      }
    };
    $("#task-abort").addEventListener("click", () => {
      if (!confirm(`タスク ${tid} を中断します。実行中のミッションも全て止まります。よろしいですか？`)) return;
      run(() => api(`/api/tasks/${encodeURIComponent(tid)}/abort`, { method: "POST", body: "{}" }), "⛔ 中断しました。");
    });
    $("#task-retry").addEventListener("click", () =>
      run(() => api(`/api/tasks/${encodeURIComponent(tid)}/retry`, { method: "POST", body: "{}" }), "🔁 リトライしました。"));
    $("#ca-send").addEventListener("click", () => {
      const directive = $("#ca-text").value.trim();
      if (!directive) { toast("指示を入力してください", true); return; }
      run(() => api(`/api/tasks/${encodeURIComponent(tid)}/ca`,
        { method: "POST", body: JSON.stringify({ directive }) }), "📡 指示を送りました。");
    });
    $$(".art-btn").forEach((b) =>
      b.addEventListener("click", () => loadArtifacts(b.dataset.mid)));
  } catch (e) {
    panel.innerHTML = `<p class="result err">読み込みに失敗しました: ${esc(e.message)}</p>`;
  }
}

async function loadArtifacts(mid) {
  const box = $("#task-artifacts");
  box.innerHTML = `<p class="hint">成果物を取得中…</p>`;
  try {
    const { files } = await api(`/api/missions/${encodeURIComponent(mid)}/artifacts`);
    if (!files.length) { box.innerHTML = `<p class="hint">成果物はまだありません。</p>`; return; }
    box.innerHTML = `<h3>📦 ${esc(mid)} の成果物</h3>` + files.map((f) =>
      `<div><a href="/api/missions/${encodeURIComponent(mid)}/artifacts/${f.path}" download>${esc(f.path)}</a>
       <span class="hint">(${(f.size / 1024).toFixed(1)} KB)</span></div>`).join("");
  } catch (e) {
    box.innerHTML = `<p class="result err">取得に失敗しました: ${esc(e.message)}</p>`;
  }
}

$("#tasks-refresh").addEventListener("click", loadTasks);

/* ---------------- fleet (ship agents) ---------------- */

async function loadFleet() {
  const box = $("#fleet-cards");
  box.innerHTML = `<p class="hint">読み込み中…</p>`;
  try {
    const [{ ships }, { models }] = await Promise.all([
      api("/api/fleet/ships"),
      api("/api/fleet/models"),
    ]);
    const names = Object.keys(ships);
    if (!names.length) {
      box.innerHTML = `<p class="hint">config.yaml に ships: セクションがありません。</p>`;
      return;
    }
    box.innerHTML = names.map((name) => shipCard(name, ships[name], models)).join("");
    names.forEach((name) => bindShipCard(name));
  } catch (e) {
    box.innerHTML = `<p class="result err">読み込みに失敗しました: ${esc(e.message)}</p>`;
  }
}

function shipCard(name, p, models) {
  const base = name.split("_")[0];
  const title = SHIP_LABELS[base] ? `${SHIP_LABELS[base]}${name !== base ? ` — ${name}` : ""}` : name;
  const arm = p.armament || {};
  const maxTokens = arm.max_tokens ?? 1000;
  const temperature = arm.temperature ?? 0.2;
  const modelOpts = models.map((m) => {
    const dis = m.configured === "false" ? "（未設定）" : "";
    return `<option value="${esc(m.model_env)}" ${p.model_env === m.model_env ? "selected" : ""}>${esc(m.label)}${dis}</option>`;
  }).join("");
  const listVal = (v) => esc((v || []).join("\n"));
  return `
  <div class="card" data-ship="${esc(name)}">
    <h3>${esc(title)}</h3>
    <label>担当 AI モデル
      <select name="model_env">${modelOpts}</select>
    </label>
    <div class="ship-grid">
      <label>最大トークン数
        <div class="slider-row">
          <input type="range" name="max_tokens" min="100" max="8000" step="100" value="${maxTokens}">
          <output>${maxTokens}</output>
        </div>
      </label>
      <label>創造性（temperature）
        <div class="slider-row">
          <input type="range" name="temperature" min="0" max="1" step="0.05" value="${temperature}">
          <output>${temperature}</output>
        </div>
      </label>
    </div>
    <details>
      <summary class="hint">ペルソナ・行動指針を編集</summary>
      <label>役割 <input name="role" value="${esc(p.role || "")}"></label>
      <label>口調 <input name="voice" value="${esc(p.voice || "")}"></label>
      <label>目的 <input name="objective" value="${esc(p.objective || "")}"></label>
      <label>思考様式 <textarea name="thinking" rows="2">${esc(p.thinking || "")}</textarea></label>
      <label>制約（1行1項目） <textarea name="constraints" rows="3">${listVal(p.constraints)}</textarea></label>
      <label>チェックリスト（1行1項目） <textarea name="checklist" rows="3">${listVal(p.checklist)}</textarea></label>
    </details>
    <div class="toolbar" style="margin:10px 0 0">
      <button class="primary save-ship">💾 保存</button>
      <span class="result"></span>
    </div>
  </div>`;
}

function bindShipCard(name) {
  const card = $(`.card[data-ship="${CSS.escape(name)}"]`);
  card.querySelectorAll('input[type="range"]').forEach((r) => {
    r.addEventListener("input", () => { r.closest(".slider-row").querySelector("output").value = r.value; });
  });
  card.querySelector(".save-ship").addEventListener("click", async () => {
    const result = card.querySelector(".result");
    const lines = (v) => v.split("\n").map((s) => s.trim()).filter(Boolean);
    const body = {
      model_env: card.querySelector('[name="model_env"]').value,
      armament: {
        max_tokens: parseInt(card.querySelector('[name="max_tokens"]').value, 10),
        temperature: parseFloat(card.querySelector('[name="temperature"]').value),
      },
      role: card.querySelector('[name="role"]').value,
      voice: card.querySelector('[name="voice"]').value,
      objective: card.querySelector('[name="objective"]').value,
      thinking: card.querySelector('[name="thinking"]').value,
      constraints: lines(card.querySelector('[name="constraints"]').value),
      checklist: lines(card.querySelector('[name="checklist"]').value),
    };
    try {
      const res = await api(`/api/fleet/ships/${encodeURIComponent(name)}`,
        { method: "PUT", body: JSON.stringify(body) });
      result.textContent = res.worker_restart_required
        ? "✅ 保存しました（反映にはワーカー再起動が必要です）"
        : "✅ 保存しました";
      result.className = "result ok";
    } catch (e) {
      result.textContent = `❌ ${e.message}`;
      result.className = "result err";
    }
  });
}

/* ---------------- budget ---------------- */

function gauge(label, used, cap, unit = "") {
  const pct = cap > 0 ? Math.min(100, (used / cap) * 100) : 0;
  const cls = pct >= 90 ? "crit" : pct >= 70 ? "warn" : "";
  return `
  <div class="gauge">
    <div class="lbl"><span>${esc(label)}</span><span>${used}${unit} / ${cap}${unit}（${pct.toFixed(0)}%）</span></div>
    <div class="bar"><div class="fill ${cls}" style="width:${pct}%"></div></div>
  </div>`;
}

async function loadBudget() {
  const monthInput = $("#budget-month");
  if (!monthInput.value) {
    monthInput.value = new Date().toISOString().slice(0, 7);
  }
  const box = $("#budget-body");
  box.innerHTML = `<p class="hint">読み込み中…</p>`;
  try {
    const b = await api(`/api/budget?month=${monthInput.value}`);
    box.innerHTML = `
      <h2>${esc(b.month)} の予算</h2>
      ${gauge("燃料（API 費用）", b.fuel_burned.toFixed(2), b.fuel_cap.toFixed(2), " USD")}
      ${gauge("戦艦 BB 主砲（Opus 出撃）", b.ammo_used.BB, b.ammo_caps.BB, " 回")}
      ${gauge("重巡 CA 斉射（Sonnet 出撃）", b.ammo_used.CA, b.ammo_caps.CA, " 回")}
      ${gauge("装甲空母 CVB 航空隊", b.ammo_used.CVB, b.ammo_caps.CVB, " 回")}
      <p class="hint">残り燃料: $${b.fuel_remaining.toFixed(2)} ／ 総出撃数: ${b.sorties_total}</p>`;
  } catch (e) {
    box.innerHTML = `<p class="result err">予算情報を取得できません: ${esc(e.message)}</p>`;
  }
}

$("#budget-refresh").addEventListener("click", loadBudget);

/* ---------------- doctor / setup ---------------- */

async function runDoctor() {
  const box = $("#doctor-body");
  box.hidden = false;
  box.innerHTML = `<p class="hint">チェック中…（数十秒かかることがあります）</p>`;
  try {
    const { results, region } = await api("/api/doctor");
    const icon = (s) => s === "OK" ? "✅" : s === "WARN" ? "⚠️" : "❌";
    box.innerHTML = `<h2>環境チェック結果（リージョン: ${esc(region)}）</h2>
      <table class="checks">` + results.map((r) =>
        `<tr><td>${icon(r.status)}</td><td>${esc(r.name)}</td><td>${esc(r.detail)}</td></tr>`).join("") +
      `</table>`;
  } catch (e) {
    box.innerHTML = `<p class="result err">チェックに失敗しました: ${esc(e.message)}</p>`;
  }
}

$("#doctor-run").addEventListener("click", runDoctor);

async function loadSetupForm() {
  const form = $("#setup-form");
  try {
    const { keys, values } = await api("/api/setup");
    form.innerHTML = keys.map((k) =>
      `<label>${esc(k)}<input name="${esc(k)}" value="${esc(values[k] || "")}"></label>`).join("") +
      `<button type="submit" class="primary">💾 .env に保存</button><p class="result"></p>`;
    form.addEventListener("submit", async (ev) => {
      ev.preventDefault();
      const result = form.querySelector(".result");
      const vals = {};
      keys.forEach((k) => { vals[k] = form.elements[k].value; });
      try {
        const res = await api("/api/setup", { method: "POST", body: JSON.stringify({ values: vals }) });
        result.textContent = `✅ 保存しました: ${res.written}`;
        result.className = "result ok";
      } catch (e) {
        result.textContent = `❌ ${e.message}`;
        result.className = "result err";
      }
    });
  } catch (e) {
    form.innerHTML = `<p class="result err">${esc(e.message)}</p>`;
  }
}

/* ---------------- init ---------------- */

const TAB_LOADERS = {
  tasks: loadTasks,
  fleet: loadFleet,
  budget: loadBudget,
};

loadFormations();
loadSetupForm();
startEvents();
$("#dash-refresh").click();
