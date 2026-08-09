#!/usr/bin/env python3
"""Loopback-only editor and monitor for K3 job folders."""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

from k3_jobs import (
    DEFAULT_DATA_DIR,
    DEFAULT_HARNESS,
    DEFAULT_JOBS_DIR,
    JobError,
    active_record,
    atomic_write_text,
    create_job,
    editor_payload,
    list_jobs,
    load_job,
    process_alive,
    resolve_relative,
    run_directory,
    run_snapshot,
    save_editor_payload,
    start_job,
    stop_active_job,
    studio_temperature,
)


MAX_BODY_BYTES = 2 * 1024 * 1024
MAX_VIEW_BYTES = 2 * 1024 * 1024


INDEX_HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>K3 Studio Jobs</title>
<style>
:root {
  color-scheme: dark;
  --ink:#ece8df; --muted:#9d9a92; --line:#343531; --panel:#181916;
  --panel2:#20211d; --ground:#10110f; --warm:#e9a95c; --cool:#78b8c4;
  --good:#91c78e; --bad:#e07a6e; --radius:12px;
}
*{box-sizing:border-box} body{margin:0;background:var(--ground);color:var(--ink);
font:15px/1.45 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}
button,input,textarea{font:inherit} button{cursor:pointer}
.shell{min-height:100vh;display:grid;grid-template-columns:270px 1fr}
aside{border-right:1px solid var(--line);padding:22px 18px;background:#141512;
position:sticky;top:0;height:100vh;overflow:auto}
.brand{font-size:19px;letter-spacing:.04em;margin:0 0 3px}.sub{color:var(--muted);font-size:12px}
.create{margin:24px 0 18px;display:grid;gap:8px}.create input{width:100%}
input,textarea,select{background:#11120f;color:var(--ink);border:1px solid var(--line);
border-radius:8px;padding:9px 10px;outline:none} input:focus,textarea:focus{border-color:var(--cool)}
.primary,.quiet,.danger{border:1px solid var(--line);border-radius:8px;padding:8px 12px;
background:var(--panel2);color:var(--ink)} .primary{background:var(--warm);color:#1b1309;border-color:var(--warm);font-weight:700}
.danger{border-color:#7d3d37;color:#ffc4bc}.quiet:hover,.danger:hover{background:#292a25}
.job-list{display:grid;gap:6px}.job{width:100%;text-align:left;padding:10px;border-radius:8px;
border:1px solid transparent;background:transparent;color:var(--ink)}.job:hover{background:var(--panel2)}
.job.active{border-color:var(--cool);background:#1a2425}.job small{display:block;color:var(--muted);margin-top:3px}
main{padding:26px clamp(18px,4vw,58px) 70px;min-width:0}.empty{max-width:700px;margin:15vh auto;color:var(--muted);text-align:center}
.topline{display:flex;gap:18px;align-items:flex-start;justify-content:space-between}.topline h1{font-size:28px;margin:0 0 5px;letter-spacing:-.03em}
.actions{display:flex;gap:8px;flex-wrap:wrap;justify-content:flex-end}.statusbar{display:flex;gap:14px;align-items:center;
margin:20px 0;padding:11px 14px;background:var(--panel);border:1px solid var(--line);border-radius:var(--radius);color:var(--muted)}
.dot{width:9px;height:9px;border-radius:50%;background:var(--muted)}.dot.running{background:var(--warm);box-shadow:0 0 12px var(--warm)}
.dot.completed{background:var(--good)}.dot.failed,.dot.temperature{background:var(--bad)}
.tabs{display:flex;gap:5px;border-bottom:1px solid var(--line);margin-bottom:22px}.tab{padding:10px 13px;border:0;border-bottom:2px solid transparent;
background:transparent;color:var(--muted)}.tab.selected{color:var(--ink);border-color:var(--warm)}
.panel{display:none}.panel.selected{display:block}.card{background:var(--panel);border:1px solid var(--line);border-radius:var(--radius);padding:18px;margin-bottom:14px}
.card h2,.card h3{margin:0 0 12px}.grid{display:grid;grid-template-columns:repeat(3,minmax(150px,1fr));gap:14px}
label{display:grid;gap:6px;color:var(--muted);font-size:12px}label input,label textarea{color:var(--ink);font-size:14px}
textarea{width:100%;min-height:240px;resize:vertical;line-height:1.5}.small-area{min-height:150px}
.checks{display:flex;gap:22px;margin-top:15px}.checks label{display:flex;align-items:center;gap:8px}.checks input{width:auto}
.stage{border-left:3px solid var(--cool)}.stage-head{display:flex;gap:10px;align-items:center;margin-bottom:12px}.stage-head strong{flex:1}
.stage-grid{display:grid;grid-template-columns:140px 1fr 1fr;gap:10px}.stage textarea{min-height:155px;margin-top:12px}
.stage-controls{display:flex;gap:5px}.stage-controls button{padding:4px 8px}.note{color:var(--muted);font-size:12px}
.run-grid{display:grid;grid-template-columns:minmax(280px,.8fr) minmax(340px,1.2fr);gap:14px}.metrics{display:grid;grid-template-columns:repeat(2,1fr);gap:10px}
.metric{background:#12130f;border:1px solid var(--line);border-radius:8px;padding:10px}.metric b{display:block;font-size:18px;color:var(--ink)}
.outputs{display:grid;gap:6px;max-height:320px;overflow:auto}.output{border:1px solid var(--line);background:#12130f;color:var(--ink);
padding:8px 10px;border-radius:7px;text-align:left}.output:hover{border-color:var(--cool)}
pre{white-space:pre-wrap;word-break:break-word;background:#0b0c0a;border:1px solid var(--line);border-radius:8px;padding:14px;max-height:500px;overflow:auto;color:#d4d2ca}
.toast{position:fixed;right:22px;bottom:22px;background:#292b25;border:1px solid var(--line);border-radius:9px;padding:11px 14px;display:none;max-width:460px;z-index:5}
@media(max-width:850px){.shell{grid-template-columns:1fr}aside{position:static;height:auto;border-right:0;border-bottom:1px solid var(--line)}
.job-list{grid-template-columns:repeat(auto-fit,minmax(180px,1fr))}.grid,.stage-grid,.run-grid{grid-template-columns:1fr}.topline{display:block}.actions{justify-content:flex-start;margin-top:15px}}
</style>
</head>
<body>
<div class="shell">
<aside>
  <h1 class="brand">K3 Studio Jobs</h1>
  <div class="sub">File-backed, local, one run at a time.</div>
  <div class="create">
    <input id="new-name" placeholder="New job name" aria-label="New job name">
    <input id="new-slug" placeholder="job-slug" aria-label="New job slug">
    <button class="quiet" id="create-job">Create job</button>
  </div>
  <div id="jobs" class="job-list"></div>
</aside>
<main>
  <div id="empty" class="empty"><h2>Select or create a job</h2><p>Edit the brief and stage prompts, set hard limits, then let the Spark continue after the browser closes.</p></div>
  <section id="editor" hidden>
    <div class="topline">
      <div><h1 id="job-title"></h1><div class="sub" id="job-description"></div></div>
      <div class="actions">
        <button class="quiet" id="save-job">Save</button>
        <button class="primary" id="start-job">Start run</button>
        <button class="danger" id="stop-job">Stop active run</button>
      </div>
    </div>
    <div class="statusbar"><span id="status-dot" class="dot"></span><span id="status-text">Idle</span><span id="temperature"></span></div>
    <nav class="tabs" aria-label="Job editor sections">
      <button class="tab selected" data-tab="setup">Setup</button>
      <button class="tab" data-tab="instructions">Instructions</button>
      <button class="tab" data-tab="stages">Stages</button>
      <button class="tab" data-tab="run">Run</button>
    </nav>
    <div class="panel selected" data-panel="setup">
      <div class="card">
        <h2>Run limits</h2>
        <div class="grid">
          <label>Name<input id="cfg-name"></label>
          <label>Maximum minutes<input id="cfg-runtime" type="number" min="1"></label>
          <label>Cycles<input id="cfg-cycles" type="number" min="1"></label>
          <label>Temperature cutoff °C<input id="cfg-temp" type="number" min="20" max="120" step="0.5"></label>
          <label>Output token limit<input id="cfg-tokens" type="number" min="1"></label>
          <label>Command rounds<input id="cfg-rounds" type="number" min="0"></label>
        </div>
        <div class="checks">
          <label><input id="cfg-approve" type="checkbox"> Auto-approve bounded ACT commands</label>
          <label><input id="cfg-stop" type="checkbox"> Stop at first failed stage</label>
        </div>
      </div>
      <div class="card"><h2>Brief</h2><p class="note">The brief is prepended to every stage.</p><textarea id="file-brief"></textarea></div>
    </div>
    <div class="panel" data-panel="instructions">
      <div class="card"><h2>System instructions</h2><textarea id="file-system"></textarea></div>
      <div class="card"><h2>Durable state</h2><textarea id="file-state" class="small-area"></textarea></div>
    </div>
    <div class="panel" data-panel="stages">
      <div class="actions" style="margin-bottom:12px"><button class="quiet" id="add-stage">Add stage</button></div>
      <div id="stage-list"></div>
    </div>
    <div class="panel" data-panel="run">
      <div class="run-grid">
        <div>
          <div class="card"><h2>Progress</h2><div id="metrics" class="metrics"></div></div>
          <div class="card"><h2>Outputs</h2><div id="outputs" class="outputs"><span class="note">No outputs yet.</span></div></div>
        </div>
        <div class="card"><h2 id="viewer-title">Recent log</h2><pre id="viewer">No run selected.</pre></div>
      </div>
    </div>
  </section>
</main>
</div>
<div id="toast" class="toast"></div>
<script>
const state={jobs:[],current:null,dirty:false,status:null};
const $=s=>document.querySelector(s), $$=s=>[...document.querySelectorAll(s)];
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
function toast(message,bad=false){const e=$('#toast');e.textContent=message;e.style.display='block';e.style.borderColor=bad?'var(--bad)':'var(--good)';setTimeout(()=>e.style.display='none',3500)}
async function api(path,options={}){const r=await fetch(path,{...options,headers:{'Content-Type':'application/json',...(options.headers||{})}});let body={};try{body=await r.json()}catch{}if(!r.ok)throw new Error(body.error||`${r.status} ${r.statusText}`);return body}
async function loadJobs(){const data=await api('/api/jobs');state.jobs=data.jobs;renderJobs();if(!state.current&&state.jobs.length)await selectJob(state.jobs[0].slug)}
function renderJobs(){$('#jobs').innerHTML=state.jobs.map(j=>`<button class="job ${state.current?.slug===j.slug?'active':''}" data-slug="${esc(j.slug)}"><b>${esc(j.name)}</b><small>${j.valid?esc(j.description||j.slug):'Invalid: '+esc(j.error)}</small></button>`).join('');$$('.job').forEach(b=>b.onclick=()=>selectJob(b.dataset.slug))}
async function selectJob(slug){if(state.dirty&&!confirm('Discard unsaved changes?'))return;state.current=await api(`/api/jobs/${encodeURIComponent(slug)}`);state.dirty=false;renderJobs();renderEditor();await refreshStatus()}
function renderEditor(){const p=state.current,c=p.config;$('#empty').hidden=true;$('#editor').hidden=false;$('#job-title').textContent=c.name;$('#job-description').textContent=c.description||p.slug;
$('#cfg-name').value=c.name;$('#cfg-runtime').value=c.max_runtime_minutes;$('#cfg-cycles').value=c.max_cycles;$('#cfg-temp').value=c.temperature_limit_c??'';$('#cfg-tokens').value=c.max_tokens;$('#cfg-rounds').value=c.max_rounds;$('#cfg-approve').checked=c.auto_approve;$('#cfg-stop').checked=c.stop_on_failure;
$('#file-brief').value=p.files['BRIEF.md'];$('#file-system').value=p.files['SYSTEM.md'];$('#file-state').value=p.files['STATE.md'];renderStages()}
function renderStages(){const p=state.current;$('#stage-list').innerHTML=p.config.stages.map((s,i)=>`<article class="card stage" data-index="${i}" data-prompt="${esc(s.prompt)}">
<div class="stage-head"><input class="st-enabled" type="checkbox" ${s.enabled!==false?'checked':''} aria-label="Stage enabled"><strong>${i+1}. ${esc(s.title)}</strong><div class="stage-controls"><button class="quiet up" title="Move up">↑</button><button class="quiet down" title="Move down">↓</button><button class="danger remove">Remove</button></div></div>
<div class="stage-grid"><label>ID<input class="st-id" value="${esc(s.id)}"></label><label>Title<input class="st-title" value="${esc(s.title)}"></label><label>Expected Markdown file<input class="st-deliverable" value="${esc(s.deliverable)}"></label></div>
<label>Stage prompt<textarea class="st-prompt">${esc(p.files[s.prompt]||'')}</textarea></label></article>`).join('');
$$('.stage .remove').forEach(b=>b.onclick=()=>{const i=+b.closest('.stage').dataset.index;collectEditor();state.current.config.stages.splice(i,1);state.dirty=true;renderStages()});
$$('.stage .up').forEach(b=>b.onclick=()=>moveStage(+b.closest('.stage').dataset.index,-1));$$('.stage .down').forEach(b=>b.onclick=()=>moveStage(+b.closest('.stage').dataset.index,1))}
function moveStage(i,d){collectEditor();const a=state.current.config.stages,j=i+d;if(j<0||j>=a.length)return;[a[i],a[j]]=[a[j],a[i]];state.dirty=true;renderStages()}
function collectEditor(){if(!state.current)return null;const p=state.current,c=p.config;c.name=$('#cfg-name').value;c.max_runtime_minutes=+$('#cfg-runtime').value;c.max_cycles=+$('#cfg-cycles').value;c.temperature_limit_c=$('#cfg-temp').value===''?null:+$('#cfg-temp').value;c.max_tokens=+$('#cfg-tokens').value;c.max_rounds=+$('#cfg-rounds').value;c.auto_approve=$('#cfg-approve').checked;c.stop_on_failure=$('#cfg-stop').checked;p.files['BRIEF.md']=$('#file-brief').value;p.files['SYSTEM.md']=$('#file-system').value;p.files['STATE.md']=$('#file-state').value;
c.stages=$$('.stage').map(card=>{const prompt=card.dataset.prompt;p.files[prompt]=card.querySelector('.st-prompt').value;return{id:card.querySelector('.st-id').value,title:card.querySelector('.st-title').value,enabled:card.querySelector('.st-enabled').checked,prompt,deliverable:card.querySelector('.st-deliverable').value}});return p}
async function saveJob(){const p=collectEditor();state.current=await api(`/api/jobs/${encodeURIComponent(p.slug)}`,{method:'PUT',body:JSON.stringify({config:p.config,files:p.files})});state.dirty=false;renderEditor();await loadJobs();toast('Job saved')}
async function startJob(){await saveJob();const r=await api(`/api/jobs/${encodeURIComponent(state.current.slug)}/start`,{method:'POST',body:'{}'});toast(`Run ${r.run} started`);activateTab('run');setTimeout(refreshStatus,800)}
async function stopJob(){if(!confirm('Stop the active K3 job after its current process receives a termination request?'))return;await api('/api/stop',{method:'POST',body:'{}'});toast('Stop requested');setTimeout(refreshStatus,800)}
function activateTab(name){$$('.tab').forEach(x=>x.classList.toggle('selected',x.dataset.tab===name));$$('.panel').forEach(x=>x.classList.toggle('selected',x.dataset.panel===name))}
async function refreshStatus(){if(!state.current)return;try{state.status=await api(`/api/status?job=${encodeURIComponent(state.current.slug)}`);renderStatus()}catch(e){$('#status-text').textContent=e.message}}
function renderStatus(){const d=state.status,a=d.active,run=d.latest||{},s=run.status||{};const running=!!a?.alive;const mode=running?'running':(s.state||'idle');$('#status-dot').className='dot '+mode;$('#status-text').textContent=running?`${a.job} · ${a.run} · running`:(s.state?`${s.state}: ${s.reason||''}`:'Idle');$('#temperature').textContent=d.temperature_c==null?'':'Spark '+Number(d.temperature_c).toFixed(0)+'°C';
const vals=[['State',s.state||'idle'],['Progress',`${s.completed_stages||0} / ${s.total_stages||0}`],['Cycle',s.cycle||'—'],['Stage',s.current_stage||'—'],['Started',s.started_utc||'—'],['Deadline',s.deadline_utc||'—']];$('#metrics').innerHTML=vals.map(v=>`<div class="metric"><span class="note">${esc(v[0])}</span><b>${esc(v[1])}</b></div>`).join('');
$('#outputs').innerHTML=(run.outputs||[]).length?run.outputs.map(o=>`<button class="output" data-path="${esc(o.path)}">${esc(o.path)} <span class="note">${o.bytes} B</span></button>`).join(''):'<span class="note">No outputs yet.</span>';$$('.output').forEach(b=>b.onclick=()=>viewOutput(run.job,run.run,b.dataset.path));if(!$('#viewer').dataset.file){$('#viewer-title').textContent='Recent log';$('#viewer').textContent=run.log_tail||'No log yet.'}}
async function viewOutput(job,run,path){const d=await api(`/api/output/${encodeURIComponent(job)}/${encodeURIComponent(run)}?path=${encodeURIComponent(path)}`);$('#viewer-title').textContent=path;$('#viewer').dataset.file=path;$('#viewer').textContent=d.content}
function addStage(){collectEditor();const n=state.current.config.stages.length+1,id=`stage-${n}`,prompt=`prompts/${String(n).padStart(2,'0')}-${id}.md`;state.current.config.stages.push({id,title:`Stage ${n}`,enabled:true,prompt,deliverable:`${String(n).padStart(2,'0')}_${id}.md`});state.current.files[prompt]='Reply with `SAVE '+String(n).padStart(2,'0')+'_'+id+'.md` followed by the requested deliverable.\n';state.dirty=true;renderStages()}
async function createJob(){const name=$('#new-name').value.trim(),slug=$('#new-slug').value.trim();if(!name||!slug)return toast('Enter a name and slug',true);const d=await api('/api/jobs',{method:'POST',body:JSON.stringify({name,slug})});$('#new-name').value='';$('#new-slug').value='';await loadJobs();await selectJob(d.slug);toast('Job created')}
$('#create-job').onclick=()=>createJob().catch(e=>toast(e.message,true));$('#save-job').onclick=()=>saveJob().catch(e=>toast(e.message,true));$('#start-job').onclick=()=>startJob().catch(e=>toast(e.message,true));$('#stop-job').onclick=()=>stopJob().catch(e=>toast(e.message,true));$('#add-stage').onclick=addStage;
$$('.tab').forEach(b=>b.onclick=()=>activateTab(b.dataset.tab));document.addEventListener('input',e=>{if(e.target.closest('#editor'))state.dirty=true});window.addEventListener('beforeunload',e=>{if(state.dirty){e.preventDefault();e.returnValue=''}});
loadJobs().catch(e=>toast(e.message,true));setInterval(refreshStatus,5000);
</script>
</body>
</html>"""


class JobHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address, *, jobs_dir: Path, data_dir: Path, harness: Path):
        super().__init__(address, JobHandler)
        self.jobs_dir = jobs_dir
        self.data_dir = data_dir
        self.harness = harness
        self.operation_lock = threading.Lock()


class JobHandler(BaseHTTPRequestHandler):
    server_version = "k3-job-ui/1"

    def log_message(self, fmt, *args):
        sys.stderr.write("%s - %s\n" % (self.address_string(), fmt % args))

    def send_bytes(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.end_headers()
        self.wfile.write(body)

    def send_json(self, status: int, value: Any) -> None:
        self.send_bytes(
            status,
            json.dumps(value, ensure_ascii=False).encode("utf-8"),
            "application/json; charset=utf-8",
        )

    def read_json(self) -> Any:
        if not self.headers.get("Content-Type", "").startswith("application/json"):
            raise JobError("Content-Type must be application/json")
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError as exc:
            raise JobError("invalid Content-Length") from exc
        if not 0 <= length <= MAX_BODY_BYTES:
            raise JobError("request body is too large")
        raw = self.rfile.read(length)
        try:
            return json.loads(raw or b"{}")
        except (json.JSONDecodeError, UnicodeError) as exc:
            raise JobError(f"invalid JSON: {exc}") from exc

    def same_origin(self) -> bool:
        origin = self.headers.get("Origin")
        if not origin:
            return True
        return urlsplit(origin).netloc == self.headers.get("Host")

    def do_GET(self):
        try:
            split = urlsplit(self.path)
            path = split.path.rstrip("/") or "/"
            if path == "/":
                return self.send_bytes(200, INDEX_HTML.encode(), "text/html; charset=utf-8")
            if path == "/api/jobs":
                return self.send_json(200, {"jobs": list_jobs(self.server.jobs_dir)})
            if path.startswith("/api/jobs/"):
                slug = unquote(path[len("/api/jobs/") :])
                return self.send_json(200, editor_payload(self.server.jobs_dir, slug))
            if path == "/api/status":
                query = parse_qs(split.query)
                slug = query.get("job", [None])[0]
                active = active_record(self.server.data_dir)
                latest = None
                if slug:
                    load_job(self.server.jobs_dir, slug)
                    latest = run_snapshot(self.server.data_dir, slug)
                elif active and active.get("job"):
                    latest = run_snapshot(
                        self.server.data_dir, active["job"], active.get("run")
                    )
                return self.send_json(
                    200,
                    {
                        "active": active,
                        "latest": latest,
                        "temperature_c": studio_temperature(),
                    },
                )
            if path.startswith("/api/output/"):
                parts = [unquote(part) for part in path.split("/")[3:]]
                if len(parts) != 2:
                    raise JobError("output URL must include job and run")
                slug, run_id = parts
                query = parse_qs(split.query)
                relative = query.get("path", [""])[0]
                root = run_directory(self.server.data_dir, slug, run_id)
                target = resolve_relative(root, relative, label="output path")
                if target.suffix.lower() != ".md" or not target.is_file():
                    raise JobError("output is not a Markdown deliverable")
                if target.stat().st_size > MAX_VIEW_BYTES:
                    raise JobError("output is too large to display")
                return self.send_json(
                    200, {"path": relative, "content": target.read_text(encoding="utf-8")}
                )
            self.send_json(404, {"error": "not found"})
        except JobError as exc:
            self.send_json(400, {"error": str(exc)})
        except (OSError, UnicodeError) as exc:
            self.send_json(500, {"error": str(exc)})

    def do_POST(self):
        self.mutate("POST")

    def do_PUT(self):
        self.mutate("PUT")

    def mutate(self, method: str):
        try:
            if not self.same_origin():
                return self.send_json(403, {"error": "origin does not match this server"})
            path = urlsplit(self.path).path.rstrip("/") or "/"
            body = self.read_json()
            with self.server.operation_lock:
                if method == "POST" and path == "/api/jobs":
                    if not isinstance(body, dict):
                        raise JobError("job request must be an object")
                    create_job(
                        self.server.jobs_dir, body.get("slug"), body.get("name")
                    )
                    return self.send_json(201, {"slug": body["slug"]})
                if method == "PUT" and path.startswith("/api/jobs/"):
                    slug = unquote(path[len("/api/jobs/") :])
                    value = save_editor_payload(self.server.jobs_dir, slug, body)
                    return self.send_json(200, value)
                if method == "POST" and path.startswith("/api/jobs/") and path.endswith(
                    "/start"
                ):
                    slug = unquote(path[len("/api/jobs/") : -len("/start")])
                    value = start_job(
                        self.server.jobs_dir,
                        self.server.data_dir,
                        self.server.harness,
                        slug,
                    )
                    return self.send_json(202, value)
                if method == "POST" and path == "/api/stop":
                    return self.send_json(202, stop_active_job(self.server.data_dir))
            self.send_json(404, {"error": "not found"})
        except JobError as exc:
            self.send_json(400, {"error": str(exc)})
        except (OSError, UnicodeError) as exc:
            self.send_json(500, {"error": str(exc)})


def parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8042)
    ap.add_argument("--jobs-dir", type=Path, default=DEFAULT_JOBS_DIR)
    ap.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    ap.add_argument("--harness", type=Path, default=DEFAULT_HARNESS)
    ap.add_argument("--pid-file", type=Path, default=None)
    return ap


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if args.host not in {"127.0.0.1", "localhost", "::1"}:
        print("k3-job-ui only binds to a loopback address", file=sys.stderr)
        return 2
    if not 1 <= args.port <= 65535:
        print("port must be between 1 and 65535", file=sys.stderr)
        return 2
    data_dir = args.data_dir.expanduser().resolve()
    pid_file = (
        args.pid_file.expanduser().resolve()
        if args.pid_file is not None
        else data_dir.parent / "job-ui.pid"
    )
    if pid_file.is_file():
        try:
            existing_pid = int(pid_file.read_text().strip())
        except (OSError, ValueError):
            existing_pid = 0
        if process_alive(existing_pid):
            print(f"K3 Studio Jobs is already running as PID {existing_pid}", file=sys.stderr)
            return 2
    server = JobHTTPServer(
        (args.host, args.port),
        jobs_dir=args.jobs_dir.expanduser().resolve(),
        data_dir=data_dir,
        harness=args.harness.expanduser().resolve(),
    )
    atomic_write_text(pid_file, f"{os.getpid()}\n")
    shown = args.host if ":" not in args.host else f"[{args.host}]"
    print(f"K3 Studio Jobs listening on http://{shown}:{server.server_port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        try:
            if pid_file.read_text().strip() == str(os.getpid()):
                pid_file.unlink()
        except OSError:
            pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
