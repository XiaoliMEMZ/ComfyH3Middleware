"use strict";

const state = {
  authenticated: false,
  view: "overview",
  summary: null,
  jobs: [],
  upstreams: [],
  queue: { paused: false, items: [] },
  keys: [],
  events: [],
  refreshTimer: null,
};

const viewMeta = {
  overview: ["概览", "集群状态与近期任务"],
  jobs: ["任务", "查询、取消与重试"],
  queue: ["队列", "中间件持久化调度顺序"],
  upstreams: ["上游", "ComfyUI 健康、容量与原子操作"],
  keys: ["API Keys", "客户端调用凭证"],
};

const $ = (selector) => document.querySelector(selector);
const $$ = (selector) => Array.from(document.querySelectorAll(selector));

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

async function api(path, options = {}) {
  const init = { credentials: "same-origin", ...options };
  if (init.body && typeof init.body !== "string" && !(init.body instanceof FormData)) {
    init.headers = { "Content-Type": "application/json", ...(init.headers || {}) };
    init.body = JSON.stringify(init.body);
  }
  const response = await fetch(path, init);
  let payload = null;
  try {
    payload = await response.json();
  } catch {
    payload = { ok: false, error: { message: `HTTP ${response.status}` } };
  }
  if (response.status === 401) {
    showLogin();
    throw new Error(payload?.error?.message || "登录已失效");
  }
  if (!response.ok || payload?.ok === false) {
    throw new Error(payload?.error?.message || `请求失败 (${response.status})`);
  }
  return payload;
}

function showLogin() {
  state.authenticated = false;
  clearInterval(state.refreshTimer);
  state.refreshTimer = null;
  $("#app-shell").hidden = true;
  $("#login-layer").hidden = false;
  setTimeout(() => $("#admin-token").focus(), 0);
}

function showApp() {
  state.authenticated = true;
  $("#login-layer").hidden = true;
  $("#app-shell").hidden = false;
  if (!state.refreshTimer) {
    state.refreshTimer = setInterval(() => refreshAll(false), 5000);
  }
}

async function logout() {
  try { await api("/admin/api/logout", { method: "POST" }); } catch { /* session may already be gone */ }
  showLogin();
}

function toast(message, error = false) {
  const element = $("#toast");
  element.textContent = message;
  element.classList.toggle("error", error);
  element.classList.add("show");
  clearTimeout(element.timer);
  element.timer = setTimeout(() => element.classList.remove("show"), 2600);
}

function formatTime(value) {
  if (!value) return "-";
  return new Intl.DateTimeFormat("zh-CN", {
    month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit",
  }).format(new Date(value * 1000));
}

function formatDuration(job) {
  const start = job.started_at || job.submitted_at;
  const end = job.finished_at || (start ? Date.now() / 1000 : null);
  if (!start || !end) return "-";
  const seconds = Math.max(0, end - start);
  if (seconds < 60) return `${seconds.toFixed(1)}s`;
  return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`;
}

function shortId(value) {
  return value ? `${value.slice(0, 8)}...${value.slice(-4)}` : "-";
}

function statusBadge(status) {
  const classes = {
    succeeded: "success", running: "running", submitted: "running", dispatching: "running",
    queued: "warning", retrying: "warning", canceling: "warning",
    failed: "error", upstream_unreachable: "error", canceled: "neutral",
  };
  return `<span class="badge ${classes[status] || "neutral"}">${escapeHtml(status)}</span>`;
}

function upstreamName(id) {
  return state.upstreams.find((item) => item.id === id)?.name || (id ? shortId(id) : "-");
}

function emptyRow(columns, message) {
  return `<tr class="empty-row"><td colspan="${columns}">${escapeHtml(message)}</td></tr>`;
}

function switchView(name) {
  if (!viewMeta[name]) return;
  state.view = name;
  $$(".nav-item").forEach((item) => item.classList.toggle("active", item.dataset.view === name));
  $$(".view").forEach((item) => item.classList.toggle("active", item.id === `view-${name}`));
  $("#page-title").textContent = viewMeta[name][0];
  $("#page-subtitle").textContent = viewMeta[name][1];
  location.hash = name;
}

async function loadJobs() {
  const params = new URLSearchParams({ limit: "200" });
  const search = $("#job-search")?.value.trim();
  const status = $("#job-status-filter")?.value;
  const mode = $("#job-mode-filter")?.value;
  if (search) params.set("search", search);
  if (status) params.set("status", status);
  if (mode) params.set("mode", mode);
  const result = await api(`/admin/api/jobs?${params}`);
  state.jobs = result.jobs;
  $("#job-count").textContent = `${result.total} 条任务`;
}

async function refreshAll(showNotice = false) {
  if (!state.authenticated) return;
  try {
    const [summary, upstreams, queue, keys, events] = await Promise.all([
      api("/admin/api/summary"),
      api("/admin/api/upstreams"),
      api("/admin/api/queue"),
      api("/admin/api/api-keys"),
      api("/admin/api/events?limit=30"),
      loadJobs(),
    ]);
    state.summary = summary.summary;
    state.upstreams = upstreams.upstreams;
    state.queue = queue.queue;
    state.keys = keys.api_keys;
    state.events = events.events;
    renderAll();
    $("#last-updated").textContent = `更新于 ${new Date().toLocaleTimeString("zh-CN", { hour12: false })}`;
    if (showNotice) toast("已刷新");
  } catch (error) {
    if (state.authenticated) toast(error.message, true);
  }
}

function renderAll() {
  renderHealth();
  renderSummary();
  renderOverviewUpstreams();
  renderEvents();
  renderRecentJobs();
  renderJobs();
  renderQueue();
  renderUpstreams();
  renderKeys();
}

function renderHealth() {
  const total = state.summary?.upstreams?.total || 0;
  const healthy = state.summary?.upstreams?.healthy || 0;
  const dot = $("#global-health-dot");
  dot.className = healthy > 0 ? "ok" : "bad";
  $("#global-health-text").textContent = healthy > 0 ? `${healthy}/${total} 健康` : "无可用上游";
}

function renderSummary() {
  const jobs = state.summary?.jobs || {};
  const running = (jobs.running || 0) + (jobs.submitted || 0) + (jobs.dispatching || 0);
  const queued = (jobs.queued || 0) + (jobs.retrying || 0);
  $("#metric-running").textContent = running;
  $("#metric-queued").textContent = queued;
  $("#metric-day").textContent = state.summary?.jobs_24h || 0;
  $("#metric-upstreams").textContent = `${state.summary?.upstreams?.healthy || 0} / ${state.summary?.upstreams?.total || 0}`;
  $("#metric-busy").textContent = `${state.summary?.upstreams?.busy || 0} 个忙碌`;
  $("#metric-queue-state").textContent = state.summary?.queue_paused ? "分发已暂停" : "正常分发";
}

function renderOverviewUpstreams() {
  const container = $("#overview-upstreams");
  if (!state.upstreams.length) {
    container.innerHTML = `<div class="upstream-mini">尚未配置上游</div>`;
    return;
  }
  container.innerHTML = state.upstreams.slice(0, 4).map((item) => {
    const runtime = item.runtime || {};
    const health = runtime.healthy ? '<span class="badge healthy">healthy</span>' : '<span class="badge unhealthy">unhealthy</span>';
    return `<article class="upstream-mini">
      <header><strong>${escapeHtml(item.name)}</strong>${health}</header>
      <dl>
        <div><dt>运行</dt><dd>${runtime.queue_running || 0}</dd></div>
        <div><dt>等待</dt><dd>${runtime.queue_pending || 0}</dd></div>
        <div><dt>容量</dt><dd>${escapeHtml(item.max_concurrency)}</dd></div>
        <div><dt>权重</dt><dd>${escapeHtml(item.weight)}</dd></div>
      </dl>
    </article>`;
  }).join("");
}

function renderEvents() {
  const list = $("#event-list");
  if (!state.events.length) {
    list.innerHTML = "<li><strong>暂无事件</strong></li>";
    return;
  }
  list.innerHTML = state.events.slice(0, 12).map((event) => `<li>
    <strong>${escapeHtml(event.message)}</strong>
    <span>${escapeHtml(event.kind)} · ${formatTime(event.created_at)}</span>
  </li>`).join("");
}

function jobPrompt(job) {
  return job.params?.prompt || "";
}

function renderRecentJobs() {
  const body = $("#recent-jobs-body");
  if (!state.jobs.length) {
    body.innerHTML = emptyRow(5, "暂无任务");
    return;
  }
  body.innerHTML = state.jobs.slice(0, 8).map((job) => `<tr>
    <td class="job-cell"><code title="${escapeHtml(job.id)}">${escapeHtml(shortId(job.id))}</code><span>${escapeHtml(jobPrompt(job))}</span></td>
    <td>${escapeHtml(job.mode.toUpperCase())}</td><td>${statusBadge(job.status)}</td>
    <td>${escapeHtml(upstreamName(job.upstream_id))}</td><td>${formatTime(job.created_at)}</td>
  </tr>`).join("");
}

function jobActions(job) {
  const local = ["queued", "retrying"].includes(job.status);
  const active = ["queued", "retrying", "dispatching", "submitted", "running", "canceling", "upstream_unreachable"].includes(job.status);
  const terminal = ["succeeded", "failed", "canceled"].includes(job.status);
  return `<div class="row-actions">
    <button class="button small" data-job-action="detail" data-id="${job.id}">详情</button>
    ${local ? `<button class="button small" data-job-action="front" data-id="${job.id}">置顶</button>` : ""}
    ${active ? `<button class="button small danger" data-job-action="cancel" data-id="${job.id}">取消</button>` : ""}
    ${terminal ? `<button class="button small" data-job-action="retry" data-id="${job.id}">重试</button>` : ""}
  </div>`;
}

function renderJobs() {
  const body = $("#jobs-body");
  if (!state.jobs.length) {
    body.innerHTML = emptyRow(8, "没有匹配的任务");
    return;
  }
  body.innerHTML = state.jobs.map((job) => `<tr>
    <td class="job-cell"><code title="${escapeHtml(job.id)}">${escapeHtml(shortId(job.id))}</code><span>${escapeHtml(jobPrompt(job))}</span></td>
    <td>${escapeHtml(job.mode.toUpperCase())}</td><td>${statusBadge(job.status)}</td>
    <td>${escapeHtml(job.priority)}</td><td>${escapeHtml(upstreamName(job.upstream_id))}</td>
    <td>${formatDuration(job)}</td><td>${formatTime(job.created_at)}</td><td class="actions-col">${jobActions(job)}</td>
  </tr>`).join("");
}

function renderQueue() {
  const paused = Boolean(state.queue.paused);
  $("#queue-banner").classList.toggle("paused", paused);
  $("#queue-state-title").textContent = paused ? "队列已暂停" : "队列运行中";
  $("#queue-state-copy").textContent = paused ? "任务继续入队，但不会下发到 ComfyUI" : "新任务会按优先级自动分发";
  $("#queue-toggle").textContent = paused ? "恢复分发" : "暂停分发";
  $("#queue-toggle").classList.toggle("warning", !paused);
  $("#queue-toggle").classList.toggle("primary", paused);
  $("#queue-count").textContent = `${state.queue.items.length} 个待分发任务`;
  const list = $("#queue-list");
  if (!state.queue.items.length) {
    list.innerHTML = '<div class="queue-item"><div class="queue-rank">-</div><div><strong>队列为空</strong><small>等待新任务</small></div></div>';
    return;
  }
  list.innerHTML = state.queue.items.map((job, index) => `<article class="queue-item">
    <div class="queue-rank">${String(index + 1).padStart(2, "0")}</div>
    <div><strong>${escapeHtml(shortId(job.id))} · ${escapeHtml(job.mode.toUpperCase())}</strong><small>${escapeHtml(jobPrompt(job))}</small></div>
    <div><span class="badge warning">P${escapeHtml(job.priority)}</span></div>
    <div>${formatTime(job.created_at)}</div>
    <div class="row-actions">
      <button class="button small" data-job-action="front" data-id="${job.id}">置顶</button>
      <button class="button small" data-job-action="back" data-id="${job.id}">置底</button>
      <button class="button small danger" data-job-action="cancel" data-id="${job.id}">取消</button>
    </div>
  </article>`).join("");
}

function renderUpstreams() {
  const list = $("#upstream-list");
  if (!state.upstreams.length) {
    list.innerHTML = '<div class="upstream-row"><div class="upstream-identity"><strong>尚未配置上游</strong></div></div>';
    return;
  }
  list.innerHTML = state.upstreams.map((item) => {
    const runtime = item.runtime || {};
    const status = !item.enabled ? '<span class="badge neutral">disabled</span>' : runtime.healthy ? '<span class="badge healthy">healthy</span>' : '<span class="badge unhealthy">unhealthy</span>';
    return `<article class="upstream-row">
      <div class="upstream-identity"><header><strong>${escapeHtml(item.name)}</strong>${status}</header><code title="${escapeHtml(item.base_url)}">${escapeHtml(item.base_url)}</code></div>
      <div class="upstream-stat"><span>运行 / 等待</span><strong>${runtime.queue_running || 0} / ${runtime.queue_pending || 0}</strong></div>
      <div class="upstream-stat"><span>最大并发</span><strong>${escapeHtml(item.max_concurrency)}</strong></div>
      <div class="upstream-stat"><span>权重</span><strong>${escapeHtml(item.weight)}</strong></div>
      <div class="upstream-stat"><span>节点能力</span><strong>${runtime.node_count || 0}</strong></div>
      <div class="upstream-actions">
        <button class="button small" data-upstream-action="test" data-id="${item.id}">检测</button>
        <button class="button small" data-upstream-action="edit" data-id="${item.id}">编辑</button>
        <button class="button small" data-upstream-action="ops" data-id="${item.id}">操作</button>
        <button class="button small danger" data-upstream-action="delete" data-id="${item.id}">删除</button>
      </div>
    </article>`;
  }).join("");
}

function renderKeys() {
  const body = $("#keys-body");
  if (!state.keys.length) {
    body.innerHTML = emptyRow(7, "尚未创建数据库 API Key；环境变量中的 bootstrap key 仍可使用");
    return;
  }
  body.innerHTML = state.keys.map((key) => `<tr>
    <td>${escapeHtml(key.name)}</td><td><code>${escapeHtml(key.token_prefix)}...</code></td>
    <td>${key.enabled ? '<span class="badge success">enabled</span>' : '<span class="badge neutral">disabled</span>'}</td>
    <td>${formatTime(key.created_at)}</td><td>${formatTime(key.last_used_at)}</td><td>${formatTime(key.expires_at)}</td>
    <td class="actions-col"><div class="row-actions">
      <button class="button small" data-key-action="toggle" data-id="${key.id}" data-enabled="${key.enabled}">${key.enabled ? "禁用" : "启用"}</button>
      <button class="button small danger" data-key-action="delete" data-id="${key.id}">删除</button>
    </div></td>
  </tr>`).join("");
}

async function handleJobAction(action, id) {
  try {
    if (action === "detail") {
      const result = await api(`/admin/api/jobs/${id}`);
      $("#job-detail").textContent = JSON.stringify(result.job, null, 2);
      $("#detail-dialog").showModal();
      return;
    }
    if (action === "cancel") {
      if (!confirm("取消这个任务？")) return;
      await api(`/admin/api/jobs/${id}/cancel`, { method: "POST" });
      toast("取消请求已提交");
    } else if (action === "retry") {
      await api(`/admin/api/jobs/${id}/retry`, { method: "POST" });
      toast("任务已重新排队");
    } else if (["front", "back"].includes(action)) {
      await api(`/admin/api/jobs/${id}/queue`, { method: "PATCH", body: { action } });
      toast(action === "front" ? "任务已置顶" : "任务已置底");
    }
    await refreshAll(false);
  } catch (error) {
    toast(error.message, true);
  }
}

function openUpstreamDialog(item = null) {
  $("#upstream-dialog-title").textContent = item ? "编辑上游" : "添加上游";
  $("#upstream-id").value = item?.id || "";
  $("#upstream-name").value = item?.name || "";
  $("#upstream-url").value = item?.base_url || "";
  $("#upstream-weight").value = item?.weight ?? 1;
  $("#upstream-concurrency").value = item?.max_concurrency ?? 1;
  $("#upstream-adapter").value = item?.adapter || "minimax-h3-native";
  $("#upstream-token").value = "";
  $("#upstream-token").placeholder = item?.has_auth_token ? "已配置；留空表示不修改" : "可选";
  $("#upstream-conditioning").value = item?.options?.conditioning_node || "";
  $("#upstream-enabled").checked = item?.enabled ?? true;
  $("#upstream-error").textContent = "";
  $("#upstream-dialog").showModal();
}

async function saveUpstream() {
  const id = $("#upstream-id").value;
  const token = $("#upstream-token").value;
  const conditioning = $("#upstream-conditioning").value.trim();
  const body = {
    name: $("#upstream-name").value.trim(),
    base_url: $("#upstream-url").value.trim(),
    weight: Number($("#upstream-weight").value),
    max_concurrency: Number($("#upstream-concurrency").value),
    adapter: $("#upstream-adapter").value,
    enabled: $("#upstream-enabled").checked,
    options: conditioning ? { conditioning_node: conditioning } : {},
  };
  if (token || !id) body.auth_token = token;
  try {
    await api(id ? `/admin/api/upstreams/${id}` : "/admin/api/upstreams", {
      method: id ? "PATCH" : "POST", body,
    });
    $("#upstream-dialog").close();
    toast(id ? "上游已更新" : "上游已添加");
    await refreshAll(false);
  } catch (error) {
    $("#upstream-error").textContent = error.message;
  }
}

async function handleUpstreamAction(action, id) {
  const item = state.upstreams.find((upstream) => upstream.id === id);
  if (!item) return;
  if (action === "edit") {
    openUpstreamDialog(item);
    return;
  }
  if (action === "ops") {
    await openUpstreamOps(item);
    return;
  }
  try {
    if (action === "test") {
      await api(`/admin/api/upstreams/${id}/test`, { method: "POST" });
      toast("健康检查完成");
    } else if (action === "delete") {
      if (!confirm(`删除上游 ${item.name}？`)) return;
      await api(`/admin/api/upstreams/${id}`, { method: "DELETE" });
      toast("上游已删除");
    }
    await refreshAll(false);
  } catch (error) {
    toast(error.message, true);
  }
}

async function refreshUpstreamOps() {
  const id = $("#upstream-ops-dialog").dataset.upstreamId;
  if (!id) return;
  const output = $("#upstream-ops-output");
  output.textContent = "正在读取...";
  try {
    const [queue, prompt, history] = await Promise.all([
      api(`/admin/api/upstreams/${id}/queue`),
      api(`/admin/api/upstreams/${id}/prompt`),
      api(`/admin/api/upstreams/${id}/history?max_items=20`),
    ]);
    output.textContent = JSON.stringify({ prompt: prompt.prompt, queue: queue.queue, history: history.history }, null, 2);
  } catch (error) {
    output.textContent = error.message;
  }
}

async function openUpstreamOps(item) {
  const dialog = $("#upstream-ops-dialog");
  dialog.dataset.upstreamId = item.id;
  $("#upstream-ops-title").textContent = `${item.name} · 原子操作`;
  dialog.showModal();
  await refreshUpstreamOps();
}

async function handleAtomicAction(action) {
  const id = $("#upstream-ops-dialog").dataset.upstreamId;
  if (!id) return;
  if (action === "refresh") {
    await refreshUpstreamOps();
    return;
  }
  const messages = {
    interrupt: "中断该上游当前运行的任务？",
    "clear-queue": "清空该上游所有等待任务？",
    "clear-history": "清空该上游全部历史记录？",
    free: "释放该上游加载的模型与显存？",
  };
  if (!confirm(messages[action])) return;
  try {
    const requests = {
      interrupt: [`/admin/api/upstreams/${id}/interrupt`, {}],
      "clear-queue": [`/admin/api/upstreams/${id}/queue/clear`, {}],
      "clear-history": [`/admin/api/upstreams/${id}/history/clear`, {}],
      free: [`/admin/api/upstreams/${id}/free`, { unload_models: true, free_memory: true }],
    };
    const [path, body] = requests[action];
    await api(path, { method: "POST", body });
    toast("操作已发送");
    await refreshUpstreamOps();
    await refreshAll(false);
  } catch (error) {
    toast(error.message, true);
  }
}

async function saveKey() {
  const name = $("#key-name").value.trim();
  const expiry = $("#key-expiry").value;
  try {
    const result = await api("/admin/api/api-keys", {
      method: "POST",
      body: { name, expires_at: expiry ? new Date(expiry).getTime() / 1000 : null },
    });
    $("#key-dialog").close();
    $("#new-token").textContent = result.token;
    $("#token-dialog").showModal();
    await refreshAll(false);
  } catch (error) {
    $("#key-error").textContent = error.message;
  }
}

async function handleKeyAction(action, id, enabled) {
  try {
    if (action === "toggle") {
      await api(`/admin/api/api-keys/${id}`, { method: "PATCH", body: { enabled: !enabled } });
      toast(enabled ? "API Key 已禁用" : "API Key 已启用");
    } else if (action === "delete") {
      if (!confirm("永久删除这个 API Key？")) return;
      await api(`/admin/api/api-keys/${id}`, { method: "DELETE" });
      toast("API Key 已删除");
    }
    await refreshAll(false);
  } catch (error) {
    toast(error.message, true);
  }
}

function bindEvents() {
  $("#login-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    $("#login-error").textContent = "";
    try {
      const response = await fetch("/admin/api/login", {
        method: "POST", credentials: "same-origin", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ token: $("#admin-token").value }),
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload?.error?.message || "登录失败");
      $("#admin-token").value = "";
      showApp();
      await refreshAll(false);
    } catch (error) {
      $("#login-error").textContent = error.message;
    }
  });

  $("#logout-button").addEventListener("click", logout);
  $("#mobile-logout-button").addEventListener("click", logout);
  $("#refresh-button").addEventListener("click", () => refreshAll(true));
  $("#queue-toggle").addEventListener("click", async () => {
    try {
      await api(`/admin/api/queue/${state.queue.paused ? "resume" : "pause"}`, { method: "POST" });
      await refreshAll(false);
    } catch (error) { toast(error.message, true); }
  });

  $$(".nav-item").forEach((item) => item.addEventListener("click", () => switchView(item.dataset.view)));
  $$('[data-go]').forEach((item) => item.addEventListener("click", () => switchView(item.dataset.go)));
  $("#add-upstream").addEventListener("click", () => openUpstreamDialog());
  $("#save-upstream").addEventListener("click", saveUpstream);
  $("#add-key").addEventListener("click", () => {
    $("#key-name").value = ""; $("#key-expiry").value = ""; $("#key-error").textContent = "";
    $("#key-dialog").showModal();
  });
  $("#save-key").addEventListener("click", saveKey);
  $("#copy-token").addEventListener("click", async () => {
    await navigator.clipboard.writeText($("#new-token").textContent);
    toast("已复制");
  });
  $$('[data-close]').forEach((item) => item.addEventListener("click", () => $(`#${item.dataset.close}`).close()));

  let filterTimer = null;
  [$("#job-search"), $("#job-status-filter"), $("#job-mode-filter")].forEach((element) => {
    element.addEventListener("input", () => {
      clearTimeout(filterTimer);
      filterTimer = setTimeout(async () => { await loadJobs(); renderJobs(); }, 250);
    });
  });

  document.addEventListener("click", (event) => {
    const jobButton = event.target.closest("[data-job-action]");
    if (jobButton) handleJobAction(jobButton.dataset.jobAction, jobButton.dataset.id);
    const upstreamButton = event.target.closest("[data-upstream-action]");
    if (upstreamButton) handleUpstreamAction(upstreamButton.dataset.upstreamAction, upstreamButton.dataset.id);
    const keyButton = event.target.closest("[data-key-action]");
    if (keyButton) handleKeyAction(keyButton.dataset.keyAction, keyButton.dataset.id, keyButton.dataset.enabled === "true");
    const atomicButton = event.target.closest("[data-atomic-action]");
    if (atomicButton) handleAtomicAction(atomicButton.dataset.atomicAction);
  });

  window.addEventListener("hashchange", () => switchView(location.hash.slice(1) || "overview"));
}

async function boot() {
  bindEvents();
  try {
    const session = await api("/admin/api/session");
    if (!session.authenticated) throw new Error("Not authenticated");
    showApp();
    switchView(location.hash.slice(1) || "overview");
    await refreshAll(false);
  } catch {
    showLogin();
  }
}

document.addEventListener("DOMContentLoaded", boot);
