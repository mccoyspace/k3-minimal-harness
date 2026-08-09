#!/usr/bin/env python3
"""File-backed unattended jobs for the minimal K3 harness."""

from __future__ import annotations

import argparse
import json
import os
import re
import secrets
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


PROJECT_DIR = Path(__file__).resolve().parent
DEFAULT_JOBS_DIR = PROJECT_DIR / "jobs"
DEFAULT_DATA_DIR = PROJECT_DIR / ".k3" / "jobs"
DEFAULT_HARNESS = PROJECT_DIR / "bin" / "k3"
JOB_SCHEMA = "k3.job.v1"
RUN_SCHEMA = "k3.job-run.v1"
SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,62}$")
STAGE_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,62}$")
TEMP_RE = re.compile(r"_input:\s*([0-9]+(?:\.[0-9]+)?)")
_BACKGROUND_PROCESSES: dict[int, subprocess.Popen[Any]] = {}
_BACKGROUND_LOCK = threading.Lock()


class JobError(ValueError):
    """A job specification or operation is invalid."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    with temporary.open("w", encoding="utf-8") as handle:
        handle.write(text)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def atomic_write_json(path: Path, value: Any) -> None:
    atomic_write_text(
        path, json.dumps(value, ensure_ascii=False, indent=2, sort_keys=False) + "\n"
    )


def append_jsonl(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(value, ensure_ascii=False, separators=(",", ":")))
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())


def read_json(path: Path) -> Any:
    try:
        with path.open(encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError as exc:
        raise JobError(f"missing {path}") from exc
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise JobError(f"could not read {path}: {exc}") from exc


def safe_slug(value: str, *, label: str = "job slug") -> str:
    if not isinstance(value, str) or not SLUG_RE.fullmatch(value):
        raise JobError(f"{label} must match {SLUG_RE.pattern}")
    return value


def resolve_relative(root: Path, value: str, *, label: str) -> Path:
    if not isinstance(value, str) or not value or "\0" in value:
        raise JobError(f"{label} must be a non-empty relative path")
    relative = Path(value)
    if relative.is_absolute():
        raise JobError(f"{label} must be relative")
    target = (root / relative).resolve()
    try:
        target.relative_to(root.resolve())
    except ValueError as exc:
        raise JobError(f"{label} escapes its job directory") from exc
    return target


def bounded_int(config: dict[str, Any], key: str, lo: int, hi: int) -> int:
    value = config.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or not lo <= value <= hi:
        raise JobError(f"{key} must be an integer from {lo} to {hi}")
    return value


def validate_job_config(
    config: Any,
    job_dir: Path,
    *,
    staged_files: dict[str, str] | None = None,
) -> dict[str, Any]:
    if not isinstance(config, dict):
        raise JobError("job.json must contain an object")
    if config.get("schema") != JOB_SCHEMA:
        raise JobError(f"schema must be {JOB_SCHEMA!r}")
    name = config.get("name")
    if not isinstance(name, str) or not name.strip() or len(name) > 120:
        raise JobError("name must be 1-120 characters")
    description = config.get("description", "")
    if not isinstance(description, str) or len(description) > 500:
        raise JobError("description must be at most 500 characters")

    bounded_int(config, "max_runtime_minutes", 1, 7 * 24 * 60)
    bounded_int(config, "max_cycles", 1, 1000)
    bounded_int(config, "max_tokens", 1, 4096)
    bounded_int(config, "max_rounds", 0, 16)
    bounded_int(config, "max_output_chars", 400, 100_000)
    bounded_int(config, "request_timeout_seconds", 30, 24 * 60 * 60)
    bounded_int(config, "command_timeout_seconds", 1, 24 * 60 * 60)
    for key in ("auto_approve", "stop_on_failure"):
        if not isinstance(config.get(key), bool):
            raise JobError(f"{key} must be true or false")
    temperature = config.get("temperature_limit_c")
    if temperature is not None and (
        isinstance(temperature, bool)
        or not isinstance(temperature, (int, float))
        or not 20 <= float(temperature) <= 120
    ):
        raise JobError("temperature_limit_c must be null or a number from 20 to 120")

    fixed_files = ("SYSTEM.md", "STATE.md", "BRIEF.md")
    for filename in fixed_files:
        if staged_files is not None and filename in staged_files:
            text = staged_files[filename]
            if not isinstance(text, str) or not text.strip():
                raise JobError(f"{filename} must not be empty")
        else:
            path = job_dir / filename
            if not path.is_file() or not path.read_text(encoding="utf-8").strip():
                raise JobError(f"missing or empty {filename}")

    stages = config.get("stages")
    if not isinstance(stages, list) or not 1 <= len(stages) <= 128:
        raise JobError("stages must contain 1-128 entries")
    seen_ids: set[str] = set()
    seen_deliverables: set[str] = set()
    for index, stage in enumerate(stages, 1):
        where = f"stages[{index - 1}]"
        if not isinstance(stage, dict):
            raise JobError(f"{where} must be an object")
        stage_id = stage.get("id")
        if not isinstance(stage_id, str) or not STAGE_RE.fullmatch(stage_id):
            raise JobError(f"{where}.id must match {STAGE_RE.pattern}")
        if stage_id in seen_ids:
            raise JobError(f"duplicate stage id: {stage_id}")
        seen_ids.add(stage_id)
        title = stage.get("title")
        if not isinstance(title, str) or not title.strip() or len(title) > 120:
            raise JobError(f"{where}.title must be 1-120 characters")
        if not isinstance(stage.get("enabled", True), bool):
            raise JobError(f"{where}.enabled must be true or false")

        prompt_name = stage.get("prompt")
        prompt_path = resolve_relative(job_dir, prompt_name, label=f"{where}.prompt")
        if prompt_path.suffix.lower() != ".md" or prompt_path.parent.name != "prompts":
            raise JobError(f"{where}.prompt must be a Markdown file in prompts/")
        if staged_files is not None and prompt_name in staged_files:
            prompt_text = staged_files[prompt_name]
            if not isinstance(prompt_text, str) or not prompt_text.strip():
                raise JobError(f"{where}.prompt must not be empty")
        elif not prompt_path.is_file() or not prompt_path.read_text(
            encoding="utf-8"
        ).strip():
            raise JobError(f"missing or empty prompt: {prompt_name}")

        deliverable = stage.get("deliverable")
        deliverable_path = resolve_relative(
            Path("/job-output"), deliverable, label=f"{where}.deliverable"
        )
        if deliverable_path.suffix.lower() != ".md":
            raise JobError(f"{where}.deliverable must be a Markdown file")
        normalized_deliverable = str(deliverable_path.relative_to("/job-output"))
        if normalized_deliverable in seen_deliverables:
            raise JobError(f"duplicate deliverable: {deliverable}")
        seen_deliverables.add(normalized_deliverable)

    if not any(stage.get("enabled", True) for stage in stages):
        raise JobError("at least one stage must be enabled")
    return config


def load_job(jobs_dir: Path, slug: str) -> tuple[Path, dict[str, Any]]:
    safe_slug(slug)
    job_dir = (jobs_dir / slug).resolve()
    try:
        job_dir.relative_to(jobs_dir.resolve())
    except ValueError as exc:
        raise JobError("job directory escapes jobs root") from exc
    if not job_dir.is_dir():
        raise JobError(f"no such job: {slug}")
    config = read_json(job_dir / "job.json")
    return job_dir, validate_job_config(config, job_dir)


def list_jobs(jobs_dir: Path) -> list[dict[str, Any]]:
    if not jobs_dir.exists():
        return []
    jobs: list[dict[str, Any]] = []
    for path in sorted(jobs_dir.iterdir()):
        if not path.is_dir() or not SLUG_RE.fullmatch(path.name):
            continue
        try:
            _, config = load_job(jobs_dir, path.name)
            jobs.append(
                {
                    "slug": path.name,
                    "name": config["name"],
                    "description": config.get("description", ""),
                    "valid": True,
                }
            )
        except JobError as exc:
            jobs.append(
                {"slug": path.name, "name": path.name, "valid": False, "error": str(exc)}
            )
    return jobs


def default_job_config(name: str) -> dict[str, Any]:
    return {
        "schema": JOB_SCHEMA,
        "name": name,
        "description": "A compact unattended K3 studio job.",
        "max_runtime_minutes": 60,
        "max_cycles": 1,
        "temperature_limit_c": 85,
        "max_tokens": 384,
        "max_rounds": 1,
        "max_output_chars": 6000,
        "request_timeout_seconds": 3600,
        "command_timeout_seconds": 300,
        "auto_approve": False,
        "stop_on_failure": True,
        "stages": [
            {
                "id": "draft",
                "title": "Draft",
                "enabled": True,
                "prompt": "prompts/01-draft.md",
                "deliverable": "01_draft.md",
            }
        ],
    }


def create_job(jobs_dir: Path, slug: str, name: str) -> tuple[Path, dict[str, Any]]:
    safe_slug(slug)
    if not isinstance(name, str) or not name.strip():
        raise JobError("name must not be empty")
    job_dir = jobs_dir / slug
    if job_dir.exists():
        raise JobError(f"job already exists: {slug}")
    (job_dir / "prompts").mkdir(parents=True)
    config = default_job_config(name.strip())
    atomic_write_json(job_dir / "job.json", config)
    atomic_write_text(
        job_dir / "SYSTEM.md",
        """You are K3 acting as a careful unattended studio collaborator.
Reply in exactly one form:

ACT {"commands":["command", "command"]}
SAVE relative/path.md
plain file content
DONE concise result
ASK one necessary question

Use ACT only for bounded inspection. Use SAVE for the requested deliverable.
Never use the network, install software, delete files, spend money, publish,
or contact anyone. Keep every word useful.
""",
    )
    atomic_write_text(
        job_dir / "STATE.md",
        "# Project state\n\nDescribe the durable facts later stages must retain.\n",
    )
    atomic_write_text(
        job_dir / "BRIEF.md",
        "# Job brief\n\nDescribe the question, constraints, audience, and desired outcome.\n",
    )
    atomic_write_text(
        job_dir / "prompts" / "01-draft.md",
        "Reply with `SAVE 01_draft.md` followed by a concise first deliverable.\n",
    )
    return load_job(jobs_dir, slug)


def editor_payload(jobs_dir: Path, slug: str) -> dict[str, Any]:
    job_dir, config = load_job(jobs_dir, slug)
    filenames = ["SYSTEM.md", "STATE.md", "BRIEF.md"]
    filenames.extend(stage["prompt"] for stage in config["stages"])
    files = {
        name: resolve_relative(job_dir, name, label="editor file").read_text(
            encoding="utf-8"
        )
        for name in dict.fromkeys(filenames)
    }
    return {"slug": slug, "config": config, "files": files}


def save_editor_payload(jobs_dir: Path, slug: str, payload: Any) -> dict[str, Any]:
    safe_slug(slug)
    if not isinstance(payload, dict):
        raise JobError("editor payload must be an object")
    config = payload.get("config")
    files = payload.get("files")
    if not isinstance(files, dict):
        raise JobError("files must be an object")
    job_dir = (jobs_dir / slug).resolve()
    if not job_dir.is_dir():
        raise JobError(f"no such job: {slug}")
    allowed = {"SYSTEM.md", "STATE.md", "BRIEF.md"}
    if isinstance(config, dict) and isinstance(config.get("stages"), list):
        allowed.update(
            stage.get("prompt")
            for stage in config["stages"]
            if isinstance(stage, dict) and isinstance(stage.get("prompt"), str)
        )
    unknown = set(files) - allowed
    if unknown:
        raise JobError(f"unexpected editor files: {', '.join(sorted(unknown))}")
    for name, text in files.items():
        if not isinstance(text, str):
            raise JobError(f"{name} content must be text")
        resolve_relative(job_dir, name, label="editor file")
    validate_job_config(config, job_dir, staged_files=files)
    for name, text in files.items():
        atomic_write_text(resolve_relative(job_dir, name, label="editor file"), text)
    atomic_write_json(job_dir / "job.json", config)
    return editor_payload(jobs_dir, slug)


def process_alive(pid: Any) -> bool:
    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 1:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def active_record(data_dir: Path) -> dict[str, Any] | None:
    path = data_dir / "active.json"
    if not path.is_file():
        return None
    try:
        value = read_json(path)
    except JobError:
        return None
    if not isinstance(value, dict):
        return None
    value = dict(value)
    alive = process_alive(value.get("pid")) and value.get("state") not in {
        "completed",
        "deadline",
        "temperature",
        "failed",
        "stopped",
    }
    command_path = Path(f"/proc/{value.get('pid')}/cmdline")
    if alive and command_path.is_file():
        command = command_path.read_bytes().replace(b"\0", b" ").decode(
            "utf-8", errors="replace"
        )
        alive = "k3_jobs.py" in command and value.get("run", "") in command
    value["alive"] = alive
    return value


def run_directory(data_dir: Path, slug: str, run_id: str) -> Path:
    safe_slug(slug)
    if not re.fullmatch(r"[0-9]{8}T[0-9]{6}Z-[a-f0-9]{6}", run_id):
        raise JobError("invalid run id")
    return data_dir / slug / "runs" / run_id


def new_run_id() -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "-" + secrets.token_hex(3)


def latest_run_id(data_dir: Path, slug: str) -> str | None:
    root = data_dir / slug / "runs"
    if not root.is_dir():
        return None
    candidates = sorted(path.name for path in root.iterdir() if path.is_dir())
    return candidates[-1] if candidates else None


def run_snapshot(data_dir: Path, slug: str, run_id: str | None = None) -> dict[str, Any]:
    selected = run_id or latest_run_id(data_dir, slug)
    if selected is None:
        return {"job": slug, "run": None}
    root = run_directory(data_dir, slug, selected)
    status_path = root / "run.json"
    status = read_json(status_path) if status_path.is_file() else {}
    outputs = []
    for cycle in sorted(root.glob("cycle-*")):
        for path in sorted(cycle.rglob("*.md")):
            outputs.append(
                {
                    "path": str(path.relative_to(root)),
                    "bytes": path.stat().st_size,
                    "modified": datetime.fromtimestamp(
                        path.stat().st_mtime, timezone.utc
                    ).isoformat(timespec="seconds"),
                }
            )
    log_path = root / "run.log"
    log_tail = ""
    if log_path.is_file():
        with log_path.open("rb") as handle:
            handle.seek(max(0, log_path.stat().st_size - 24_000))
            log_tail = handle.read().decode("utf-8", errors="replace")
    return {
        "job": slug,
        "run": selected,
        "status": status,
        "outputs": outputs,
        "log_tail": log_tail,
    }


def studio_temperature() -> float | None:
    try:
        result = subprocess.run(
            ["sensors", "-u"],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=5,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None
    values = [float(match.group(1)) for match in TEMP_RE.finditer(result.stdout)]
    values = [value for value in values if 0 < value < 200]
    return max(values) if values else None


def write_run_status(root: Path, status: dict[str, Any]) -> None:
    atomic_write_json(root / "run.json", status)


def terminate_child(child: subprocess.Popen[Any] | None) -> None:
    if child is None or child.poll() is not None:
        return
    try:
        os.killpg(child.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        child.wait(timeout=15)
    except subprocess.TimeoutExpired:
        os.killpg(child.pid, signal.SIGKILL)
        child.wait(timeout=5)


def run_job(
    jobs_dir: Path,
    data_dir: Path,
    harness: Path,
    slug: str,
    *,
    run_id: str | None = None,
    temperature_reader: Callable[[], float | None] = studio_temperature,
) -> int:
    job_dir, config = load_job(jobs_dir, slug)
    if not harness.is_file():
        raise JobError(f"missing harness launcher: {harness}")
    current = active_record(data_dir)
    if current and current.get("alive") and current.get("pid") != os.getpid():
        raise JobError(
            f"job {current.get('job')} run {current.get('run')} is already active"
        )

    selected_run = run_id or new_run_id()
    root = run_directory(data_dir, slug, selected_run)
    root.mkdir(parents=True, exist_ok=True)
    events_path = root / "events.jsonl"
    started_epoch = time.time()
    deadline_epoch = started_epoch + config["max_runtime_minutes"] * 60
    enabled_stages = [stage for stage in config["stages"] if stage.get("enabled", True)]
    total_stages = len(enabled_stages) * config["max_cycles"]
    status: dict[str, Any] = {
        "schema": RUN_SCHEMA,
        "job": slug,
        "name": config["name"],
        "run": selected_run,
        "pid": os.getpid(),
        "state": "running",
        "reason": "",
        "started_utc": utc_now(),
        "deadline_utc": datetime.fromtimestamp(
            deadline_epoch, timezone.utc
        ).isoformat(timespec="seconds"),
        "finished_utc": None,
        "cycle": 0,
        "current_stage": None,
        "completed_stages": 0,
        "total_stages": total_stages,
        "last_temperature_c": None,
    }
    data_dir.mkdir(parents=True, exist_ok=True)
    atomic_write_json(
        data_dir / "active.json",
        {"job": slug, "run": selected_run, "pid": os.getpid(), "started_utc": utc_now()},
    )
    write_run_status(root, status)

    control = {"stop": False, "signal": None}
    child: subprocess.Popen[Any] | None = None

    def request_stop(signum, _frame):
        control["stop"] = True
        control["signal"] = signum
        terminate_child(child)

    previous_term = signal.signal(signal.SIGTERM, request_stop)
    previous_int = signal.signal(signal.SIGINT, request_stop)

    final_state = "completed"
    final_reason = "all configured cycles completed"
    exit_code = 0
    failures: list[str] = []
    try:
        brief = (job_dir / "BRIEF.md").read_text(encoding="utf-8").strip()
        for cycle_number in range(1, config["max_cycles"] + 1):
            cycle_dir = root / f"cycle-{cycle_number:03d}"
            cycle_dir.mkdir(parents=True, exist_ok=True)
            status["cycle"] = cycle_number
            for stage in enabled_stages:
                now = time.time()
                temperature = temperature_reader()
                status["last_temperature_c"] = temperature
                if control["stop"]:
                    final_state, final_reason, exit_code = "stopped", "stop requested", 130
                    break
                if now >= deadline_epoch:
                    final_state, final_reason, exit_code = (
                        "deadline",
                        "maximum runtime reached",
                        124,
                    )
                    break
                limit = config.get("temperature_limit_c")
                if limit is not None and temperature is not None and temperature >= limit:
                    final_state, final_reason, exit_code = (
                        "temperature",
                        f"temperature reached {temperature:.1f}C",
                        125,
                    )
                    break

                stage_started = time.time()
                status["current_stage"] = stage["id"]
                write_run_status(root, status)
                event = {
                    "event": "stage_started",
                    "time": utc_now(),
                    "cycle": cycle_number,
                    "stage": stage["id"],
                    "deliverable": stage["deliverable"],
                }
                append_jsonl(events_path, event)
                prompt = resolve_relative(
                    job_dir, stage["prompt"], label="stage prompt"
                ).read_text(encoding="utf-8").strip()
                task = f"PROJECT BRIEF\n{brief}\n\nSTAGE\n{prompt}"
                session = f"{slug}-{selected_run}-c{cycle_number:03d}-{stage['id']}"
                command = [
                    str(harness),
                    "--session",
                    session,
                    "--workspace",
                    str(cycle_dir),
                    "--system",
                    str(job_dir / "SYSTEM.md"),
                    "--state",
                    str(job_dir / "STATE.md"),
                    "--data-dir",
                    str(root / "harness-data"),
                    "--max-rounds",
                    str(config["max_rounds"]),
                    "--max-output-chars",
                    str(config["max_output_chars"]),
                    "--max-tokens",
                    str(config["max_tokens"]),
                    "--request-timeout",
                    str(config["request_timeout_seconds"]),
                    "--command-timeout",
                    str(config["command_timeout_seconds"]),
                ]
                if config["auto_approve"]:
                    command.append("--yes")
                command.append(task)
                print(
                    f"[{utc_now()}] cycle {cycle_number}/{config['max_cycles']} "
                    f"stage {stage['id']} started",
                    flush=True,
                )
                child = subprocess.Popen(
                    command,
                    cwd=cycle_dir,
                    stdin=subprocess.DEVNULL,
                    stdout=sys.stdout,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
                interrupted_reason: str | None = None
                while child.poll() is None:
                    if control["stop"]:
                        interrupted_reason = "stop requested"
                    elif time.time() >= deadline_epoch:
                        interrupted_reason = "maximum runtime reached"
                    else:
                        temperature = temperature_reader()
                        status["last_temperature_c"] = temperature
                        if (
                            limit is not None
                            and temperature is not None
                            and temperature >= limit
                        ):
                            interrupted_reason = f"temperature reached {temperature:.1f}C"
                    if interrupted_reason:
                        terminate_child(child)
                        break
                    time.sleep(2)
                result = child.wait()
                child = None
                if interrupted_reason is None and control["stop"]:
                    interrupted_reason = "stop requested"
                deliverable = resolve_relative(
                    cycle_dir, stage["deliverable"], label="stage deliverable"
                )
                if interrupted_reason:
                    if interrupted_reason == "maximum runtime reached":
                        final_state, exit_code = "deadline", 124
                    elif interrupted_reason.startswith("temperature"):
                        final_state, exit_code = "temperature", 125
                    else:
                        final_state, exit_code = "stopped", 130
                    final_reason = interrupted_reason
                    stage_ok = False
                else:
                    stage_ok = result == 0 and deliverable.is_file()
                    if result != 0:
                        failure_reason = f"stage {stage['id']} exited {result}"
                    elif not deliverable.is_file():
                        failure_reason = (
                            f"stage {stage['id']} did not create {stage['deliverable']}"
                        )
                    if not stage_ok:
                        failures.append(failure_reason)
                        if config["stop_on_failure"]:
                            final_state, final_reason, exit_code = (
                                "failed",
                                failure_reason,
                                result or 66,
                            )

                append_jsonl(
                    events_path,
                    {
                        "event": "stage_finished",
                        "time": utc_now(),
                        "cycle": cycle_number,
                        "stage": stage["id"],
                        "deliverable": stage["deliverable"],
                        "exit": result,
                        "ok": stage_ok,
                        "elapsed_s": round(time.time() - stage_started, 3),
                    },
                )
                if stage_ok:
                    status["completed_stages"] += 1
                    print(f"[{utc_now()}] stage {stage['id']} completed", flush=True)
                elif config["stop_on_failure"] or interrupted_reason:
                    break
            if final_state != "completed":
                break
        if final_state == "completed" and failures:
            final_state = "failed"
            final_reason = f"{len(failures)} stage(s) failed; last: {failures[-1]}"
            exit_code = 1
    except Exception as exc:
        final_state, final_reason, exit_code = "failed", str(exc), 1
        print(f"job runner error: {exc}", file=sys.stderr, flush=True)
    finally:
        terminate_child(child)
        signal.signal(signal.SIGTERM, previous_term)
        signal.signal(signal.SIGINT, previous_int)
        status["state"] = final_state
        status["reason"] = final_reason
        status["finished_utc"] = utc_now()
        status["current_stage"] = None
        write_run_status(root, status)
        atomic_write_json(
            data_dir / "active.json",
            {
                "job": slug,
                "run": selected_run,
                "pid": os.getpid(),
                "started_utc": status["started_utc"],
                "finished_utc": status["finished_utc"],
                "state": final_state,
            },
        )
    return exit_code


def start_job(
    jobs_dir: Path, data_dir: Path, harness: Path, slug: str
) -> dict[str, Any]:
    load_job(jobs_dir, slug)
    current = active_record(data_dir)
    if current and current.get("alive"):
        raise JobError(
            f"job {current.get('job')} run {current.get('run')} is already active"
        )
    run_id = new_run_id()
    root = run_directory(data_dir, slug, run_id)
    root.mkdir(parents=True, exist_ok=False)
    log_path = root / "run.log"
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--jobs-dir",
        str(jobs_dir),
        "--data-dir",
        str(data_dir),
        "--harness",
        str(harness),
        "run",
        slug,
        "--run-id",
        run_id,
    ]
    with log_path.open("ab", buffering=0) as log:
        process = subprocess.Popen(
            command,
            cwd=PROJECT_DIR,
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    data_dir.mkdir(parents=True, exist_ok=True)
    atomic_write_json(
        data_dir / "active.json",
        {
            "job": slug,
            "run": run_id,
            "pid": process.pid,
            "started_utc": utc_now(),
            "state": "launching",
        },
    )
    status_path = root / "run.json"
    for _ in range(100):
        if status_path.is_file():
            break
        if process.poll() is not None:
            raise JobError(f"job runner exited {process.returncode} during startup")
        time.sleep(0.05)
    else:
        terminate_child(process)
        raise JobError("job runner did not initialize within five seconds")

    def reap() -> None:
        process.wait()
        with _BACKGROUND_LOCK:
            _BACKGROUND_PROCESSES.pop(process.pid, None)

    with _BACKGROUND_LOCK:
        _BACKGROUND_PROCESSES[process.pid] = process
    threading.Thread(target=reap, daemon=True).start()
    return {"job": slug, "run": run_id, "pid": process.pid, "log": str(log_path)}


def stop_active_job(data_dir: Path) -> dict[str, Any]:
    current = active_record(data_dir)
    if not current or not current.get("alive"):
        raise JobError("no job is currently running")
    pid = current.get("pid")
    command_path = Path(f"/proc/{pid}/cmdline")
    if command_path.is_file():
        command = command_path.read_bytes().replace(b"\0", b" ").decode(
            "utf-8", errors="replace"
        )
        if "k3_jobs.py" not in command or current.get("run", "") not in command:
            raise JobError(f"refusing to stop unrecognized process {pid}")
    os.kill(pid, signal.SIGTERM)
    return {"stopping": True, "pid": pid, "job": current.get("job"), "run": current.get("run")}


def parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--jobs-dir", type=Path, default=DEFAULT_JOBS_DIR)
    ap.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    ap.add_argument("--harness", type=Path, default=DEFAULT_HARNESS)
    sub = ap.add_subparsers(dest="command", required=True)
    sub.add_parser("list")
    create = sub.add_parser("create")
    create.add_argument("slug")
    create.add_argument("--name", required=True)
    validate = sub.add_parser("validate")
    validate.add_argument("slug")
    run = sub.add_parser("run")
    run.add_argument("slug")
    run.add_argument("--run-id")
    start = sub.add_parser("start")
    start.add_argument("slug")
    status = sub.add_parser("status")
    status.add_argument("slug", nargs="?")
    sub.add_parser("stop")
    return ap


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    jobs_dir = args.jobs_dir.expanduser().resolve()
    data_dir = args.data_dir.expanduser().resolve()
    harness = args.harness.expanduser().resolve()
    try:
        if args.command == "list":
            result: Any = list_jobs(jobs_dir)
        elif args.command == "create":
            _, config = create_job(jobs_dir, args.slug, args.name)
            result = {"created": args.slug, "config": config}
        elif args.command == "validate":
            _, config = load_job(jobs_dir, args.slug)
            result = {"valid": True, "job": args.slug, "name": config["name"]}
        elif args.command == "run":
            return run_job(
                jobs_dir, data_dir, harness, args.slug, run_id=args.run_id
            )
        elif args.command == "start":
            result = start_job(jobs_dir, data_dir, harness, args.slug)
        elif args.command == "status":
            active = active_record(data_dir)
            result = {"active": active}
            if args.slug:
                result["latest"] = run_snapshot(data_dir, args.slug)
            elif active and active.get("job"):
                result["latest"] = run_snapshot(
                    data_dir, active["job"], active.get("run")
                )
        elif args.command == "stop":
            result = stop_active_job(data_dir)
        else:
            raise AssertionError(args.command)
    except JobError as exc:
        print(f"k3-job: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
