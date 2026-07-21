"""The minimal single-file web chat UI (WS-E).

Served at ``GET /``. Dependency-free vanilla JS: POST a message, then open an SSE
stream of the run's events (assistant text, tool timeline, approvals, errors).
Kept inline so the server needs no static-asset packaging or Node build.
"""

from __future__ import annotations

from html import escape as _esc

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from keel_core.approvals import ApprovalRecord

INDEX_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Keel</title>
<style>
  :root { color-scheme: dark; }
  * { box-sizing: border-box; }
  body { margin: 0; font: 15px/1.5 system-ui, sans-serif; background: #0f1115; color: #e6e6e6; }
  header { padding: 12px 16px; border-bottom: 1px solid #222; display: flex; gap: 12px;
           align-items: center; }
  header b { color: #7aa2f7; }
  #sid { color: #888; font-size: 12px; }
  #log { max-width: 820px; margin: 0 auto; padding: 16px; display: flex; flex-direction: column;
         gap: 10px; }
  .msg { padding: 10px 12px; border-radius: 10px; white-space: pre-wrap; word-wrap: break-word; }
  .user { background: #1f2937; align-self: flex-end; max-width: 80%; }
  .assistant { background: #16213a; align-self: flex-start; max-width: 80%; }
  .meta { color: #9aa; font-size: 13px; font-family: ui-monospace, monospace; align-self: stretch; }
  .meta.error { color: #f77; }
  .approval { background: #2b2411; border: 1px solid #5a4a15; align-self: stretch; }
  .approval button { margin-right: 8px; padding: 4px 12px; border-radius: 6px; border: 0;
                     cursor: pointer; font: inherit; }
  .allow { background: #2e7d32; color: #fff; }
  .deny { background: #7a2222; color: #fff; }
  footer { position: sticky; bottom: 0; background: #0f1115; border-top: 1px solid #222;
           padding: 12px 16px; }
  #bar { max-width: 820px; margin: 0 auto; display: flex; gap: 8px; }
  #input { flex: 1; padding: 10px 12px; border-radius: 8px; border: 1px solid #333;
           background: #1a1d24; color: #e6e6e6; font: inherit; }
  #send { padding: 10px 18px; border-radius: 8px; border: 0; background: #7aa2f7; color: #0f1115;
          font-weight: 600; cursor: pointer; }
  #send:disabled { opacity: .5; cursor: default; }
</style>
</head>
<body>
<header><b>Keel</b> <span id="sid"></span></header>
<div id="log"></div>
<footer><div id="bar">
  <input id="input" placeholder="Message Keel…  (Enter to send)" autofocus />
  <button id="send">Send</button>
</div></footer>
<script>
const sid = crypto.randomUUID();
document.getElementById("sid").textContent = "session " + sid.slice(0, 8);
const log = document.getElementById("log");
const input = document.getElementById("input");
const sendBtn = document.getElementById("send");
let lastSeq = 0, es = null, streamBubble = null;

function el(cls, text) {
  const d = document.createElement("div");
  d.className = cls;
  if (text !== undefined) d.textContent = text;
  log.appendChild(d);
  window.scrollTo(0, document.body.scrollHeight);
  return d;
}

function appendDelta(text) {
  if (!streamBubble) streamBubble = el("msg assistant", "");
  streamBubble.textContent += text;
  window.scrollTo(0, document.body.scrollHeight);
}

function finalizeAssistant(text) {
  if (streamBubble) { streamBubble.textContent = text; streamBubble = null; }
  else el("msg assistant", text);
}

function addApproval(p) {
  const box = el("msg approval");
  box.textContent = `Approve ${p.tool}(${JSON.stringify(p.args)})? `;
  const allow = document.createElement("button");
  allow.className = "allow"; allow.textContent = "Allow";
  const deny = document.createElement("button");
  deny.className = "deny"; deny.textContent = "Deny";
  const resolve = (decision) => {
    allow.disabled = deny.disabled = true;
    fetch(`/v1/approvals/${p.approval_id}`, {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ approval_id: p.approval_id, decision })
    });
  };
  allow.onclick = () => resolve("allow");
  deny.onclick = () => resolve("deny");
  box.append(allow, deny);
}

function handleEvent(ev) {
  if (ev.seq) lastSeq = ev.seq;  // partial deltas (seq 0) don't move the cursor
  const t = ev.type, p = ev.payload || {};
  if (t === "message.token") {
    if (p.role !== "assistant") return;
    if (p.partial) appendDelta(p.text); else finalizeAssistant(p.text);
  }
  else if (t === "tool.call") el("meta", `→ ${p.tool}(${JSON.stringify(p.args)})`);
  else if (t === "tool.result") el("meta", `← ${p.ok ? "ok" : "err"}: ${p.output || ""}`);
  else if (t === "approval.requested") addApproval(p);
  else if (t === "approval.resolved") el("meta", `approval ${p.approved ? "allowed" : "denied"}`);
  else if (t === "error") el("meta error", `! ${p.message}`);
  else if (t === "run.ended") { el("meta", `[${p.reason}]`); closeStream(); setBusy(false); }
}

function openStream() {
  closeStream();
  es = new EventSource(`/v1/sessions/${sid}/events?after=${lastSeq}`);
  es.onmessage = (e) => handleEvent(JSON.parse(e.data));
  es.onerror = () => {};  // browser auto-retries; run.ended closes us cleanly
}
function closeStream() { if (es) { es.close(); es = null; } }
function setBusy(b) { sendBtn.disabled = b; }

function send() {
  const text = input.value.trim();
  if (!text) return;
  el("msg user", text);
  input.value = "";
  setBusy(true);
  fetch(`/v1/sessions/${sid}/messages`, {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ content: text })
  }).then(r => {
    if (!r.ok) { el("meta error", `! HTTP ${r.status}`); setBusy(false); return; }
    openStream();
  }).catch(() => { el("meta error", "! network error"); setBusy(false); });
}

sendBtn.onclick = send;
input.addEventListener("keydown", (e) => { if (e.key === "Enter") send(); });
</script>
</body>
</html>
"""


# --- Legacy server-rendered approvals page --------------------------------------------

pages_router = APIRouter()

_APPROVALS_HTML = """<!doctype html>
<html lang="zh">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Keel — Approvals</title>
<style>
  :root { color-scheme: dark; }
  * { box-sizing: border-box; }
  body { margin: 0; font: 15px/1.5 system-ui, sans-serif; background: #0f1115; color: #e6e6e6; }
  header { padding: 12px 16px; border-bottom: 1px solid #222; }
  header b { color: #7aa2f7; }
  main { max-width: 820px; margin: 0 auto; padding: 16px; }
  .approval { background: #2b2411; border: 1px solid #5a4a15; border-radius: 10px;
              padding: 14px; margin-bottom: 14px; }
  .approval .hd { font-size: 16px; }
  .approval .target { color: #f7768e; font-family: ui-monospace, monospace; }
  .badge { font-size: 12px; color: #e0af68; border: 1px solid #5a4a15; border-radius: 20px;
           padding: 1px 8px; margin-left: 6px; }
  .cd { color: #c0caf5; font-size: 13px; margin: 8px 0; }
  .btns button { margin-right: 8px; padding: 5px 14px; border-radius: 6px; border: 0;
                 cursor: pointer; font: inherit; }
  .allow { background: #2e7d32; color: #fff; } .deny { background: #7a2222; color: #fff; }
  .meta { color: #9aa; }
</style>
</head>
<body>
<header><b>Keel</b> · Approvals — 无人值守运行的挂起审批（G5，超时 fail-closed）</header>
<main>{{ROWS}}</main>
<script>
async function resolve(id, action) {
  await fetch(`/v1/approvals/${id}/${action}`, { method: "POST" });
  location.reload();
}
</script>
</body>
</html>
"""


def _render_row(record: ApprovalRecord) -> str:
    target = str(record.args.get("to", "")) if isinstance(record.args, dict) else ""
    return (
        '<div class="approval">'
        f'<div class="hd">✉️ <b>{_esc(record.tool)}</b> → '
        f'<span class="target">{_esc(target)}</span>'
        f'<span class="badge">⏸ 已挂起 · run {_esc(record.run_id)}</span></div>'
        f'<div class="cd">🛡️ confused-deputy：本次运行读入了 <b>tainted</b> 内容'
        f"（reason={_esc(record.reason)}），外发需你批准。</div>"
        '<div class="btns">'
        f"<button class=\"allow\" onclick=\"resolve('{_esc(record.id)}','approve')\">批准</button>"
        f"<button class=\"deny\" onclick=\"resolve('{_esc(record.id)}','reject')\">拒绝</button>"
        "</div></div>"
    )


def render_approvals_page(records: list[ApprovalRecord]) -> str:
    rows = "\n".join(_render_row(r) for r in records)
    if not rows:
        rows = '<p class="meta">没有待处理的审批。</p>'
    return _APPROVALS_HTML.replace("{{ROWS}}", rows)


@pages_router.get("/approvals", response_class=HTMLResponse, include_in_schema=False)
async def approvals_page(request: Request) -> str:
    """Server-rendered pending durable approvals for the slice scope."""
    store = getattr(request.app.state, "durable_approvals", None)
    scope = getattr(request.app.state, "durable_scope", "web:local")
    records = await store.list_pending(scope) if store is not None else []
    return render_approvals_page(records)
