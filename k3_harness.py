#!/usr/bin/env python3
"""Minimal approved-command harness for K3 served by WASTE."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


PROJECT_DIR = Path(__file__).resolve().parent
DEFAULT_SYSTEM = PROJECT_DIR / "K3_SYSTEM.md"
DEFAULT_STATE = PROJECT_DIR / "PROJECT_STATE.md"
DEFAULT_DATA = PROJECT_DIR / ".k3"
DEFAULT_USAGE_HOTLIST = DEFAULT_DATA / "learning" / "studio-usage.waste"
COMMAND_SHELL = "/bin/zsh" if Path("/bin/zsh").exists() else "/bin/bash"


def safe_name(value: str) -> str:
    name = re.sub(r"[^A-Za-z0-9_.-]+", "-", value).strip(".-")
    if not name:
        raise ValueError("session name must contain a letter or number")
    return name


def load_messages(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    messages: list[dict[str, str]] = []
    with path.open(encoding="utf-8") as handle:
        for number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            item = json.loads(line)
            if item.get("role") not in {"user", "assistant"}:
                raise ValueError(f"invalid role on {path}:{number}")
            if not isinstance(item.get("content"), str):
                raise ValueError(f"invalid content on {path}:{number}")
            messages.append({"role": item["role"], "content": item["content"]})
    return messages


def append_message(path: Path, message: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(message, ensure_ascii=False, separators=(",", ":")))
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())


def snapshot_usage_hotlist(
    usage_path: Path,
    data_dir: Path,
    session: str,
    task: str,
    elapsed: float,
) -> Path | None:
    """Keep one private hotlist checkpoint after a completed harness task."""
    if not usage_path.is_file():
        return None
    digest = hashlib.sha256(usage_path.read_bytes()).hexdigest()
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    snapshot_dir = data_dir / "learning" / "snapshots"
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    target = snapshot_dir / f"{stamp}-{session}-{digest[:12]}.waste"
    if not target.exists():
        temporary = target.with_name(f".{target.name}.tmp-{os.getpid()}")
        shutil.copyfile(usage_path, temporary)
        os.replace(temporary, target)

    record = {
        "schema": "k3.harness.usage-snapshot.v1",
        "completed_utc": stamp,
        "session": session,
        "elapsed_s": round(elapsed, 3),
        "task_sha256": hashlib.sha256(task.encode("utf-8")).hexdigest(),
        "task_chars": len(task),
        "usage_sha256": digest,
        "usage_bytes": target.stat().st_size,
        "snapshot": str(target.relative_to(data_dir)),
    }
    index = data_dir / "learning" / "snapshots.jsonl"
    index.parent.mkdir(parents=True, exist_ok=True)
    with index.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, separators=(",", ":")) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    return target


def parse_reply(text: str) -> tuple[str, Any]:
    stripped = text.strip()
    first_line, separator, remainder = stripped.partition("\n")
    if first_line.startswith("SAVE "):
        filename = first_line[5:].strip()
        if not filename or not separator or not remainder.strip():
            raise ValueError("SAVE requires a relative filename and non-empty content")
        return "save", (filename, remainder.rstrip() + "\n")
    if stripped.startswith("ACT "):
        try:
            payload = json.loads(stripped[4:].strip())
        except json.JSONDecodeError as exc:
            raise ValueError(f"K3 returned invalid ACT JSON: {exc}") from exc
        commands = payload.get("commands") if isinstance(payload, dict) else None
        if not isinstance(commands, list) or not commands:
            raise ValueError("ACT must contain a non-empty commands list")
        if len(commands) > 16 or not all(
            isinstance(command, str) and command.strip() and len(command) <= 4096
            for command in commands
        ):
            raise ValueError("ACT commands must be 1-16 non-empty strings")
        return "act", [command.strip() for command in commands]
    if stripped.startswith("ASK "):
        return "ask", stripped[4:].strip()
    if stripped.startswith("DONE "):
        return "done", stripped[5:].strip()
    return "done", stripped


def clipped(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    marker = f"\n... {len(text) - limit:,} characters omitted; see full log ...\n"
    room = max(0, limit - len(marker))
    head = room * 2 // 3
    return text[:head] + marker + text[-(room - head):]


class WasteClient:
    def __init__(self, url: str, model: str, api_key: str, timeout: int, max_tokens: int):
        self.url = url.rstrip("/") + "/chat/completions"
        self.model = model
        self.api_key = api_key
        self.timeout = timeout
        self.max_tokens = max_tokens

    def complete(self, messages: list[dict[str, str]]) -> tuple[str, dict[str, Any]]:
        payload = {
            "model": self.model,
            "messages": messages,
            "temperature": 0,
            "top_p": 1,
            "max_completion_tokens": self.max_tokens,
            "reasoning_effort": "off",
            "stream": False,
        }
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        request = urllib.request.Request(
            self.url,
            data=json.dumps(payload).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        started = time.monotonic()
        print("K3 is processing the request...", file=sys.stderr, flush=True)
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                result = json.load(response)
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"WASTE returned HTTP {exc.code}: {body}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"could not reach WASTE at {self.url}: {exc.reason}") from exc

        elapsed = time.monotonic() - started
        try:
            text = result["choices"][0]["message"]["content"] or ""
        except (KeyError, IndexError, TypeError) as exc:
            raise RuntimeError(f"unexpected WASTE response: {result}") from exc

        usage = result.get("usage", {})
        cache = result.get("waste", {}).get("prefix_cache", {})
        details = [f"elapsed={elapsed:.1f}s"]
        if usage:
            details.append(
                f"tokens={usage.get('prompt_tokens', '?')}+{usage.get('completion_tokens', '?')}"
            )
        if cache:
            details.append(
                "cache="
                f"{cache.get('status', '?')} "
                f"restored={cache.get('restored_tokens', 0)} "
                f"replayed={cache.get('replayed_tokens', 0)}"
            )
        print("K3 response: " + ", ".join(details), file=sys.stderr, flush=True)
        return text, result


def approve(commands: list[str], assume_yes: bool) -> bool:
    print("\nProposed command batch:")
    for index, command in enumerate(commands, 1):
        print(f"  {index}. {command}")
    if assume_yes:
        print("Approved by --yes.")
        return True
    try:
        answer = input("Run this batch? [y/N] ").strip().lower()
    except EOFError:
        return False
    return answer in {"y", "yes"}


def approve_save(filename: str, content: str, assume_yes: bool) -> bool:
    print(f"\nProposed save: {filename} ({len(content.encode('utf-8')):,} bytes)")
    print(clipped(content, 600))
    if assume_yes:
        print("Approved by --yes.")
        return True
    try:
        answer = input("Write this file? [y/N] ").strip().lower()
    except EOFError:
        return False
    return answer in {"y", "yes"}


def save_output(workspace: Path, filename: str, content: str) -> Path:
    relative = Path(filename)
    if relative.is_absolute() or not relative.parts or "\0" in filename:
        raise ValueError("SAVE filename must be a relative path")
    target = (workspace / relative).resolve()
    try:
        target.relative_to(workspace)
    except ValueError as exc:
        raise ValueError("SAVE filename escapes the workspace") from exc
    if target == workspace:
        raise ValueError("SAVE filename must name a file")
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.tmp-{os.getpid()}")
    temporary.write_text(content, encoding="utf-8")
    os.replace(temporary, target)
    return target


def execute_batch(
    commands: list[str],
    workspace: Path,
    run_dir: Path,
    output_limit: int,
    command_timeout: int,
) -> str:
    run_dir.mkdir(parents=True, exist_ok=True)
    records: list[tuple[str, int, float, Path, str]] = []
    for index, command in enumerate(commands, 1):
        print(f"Running {index}/{len(commands)}...", file=sys.stderr, flush=True)
        started = time.monotonic()
        try:
            process = subprocess.run(
                "set -o pipefail\n" + command,
                cwd=workspace,
                shell=True,
                executable=COMMAND_SHELL,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                timeout=command_timeout,
                env=os.environ.copy(),
            )
            output = process.stdout or ""
            status = process.returncode
        except subprocess.TimeoutExpired as exc:
            partial = exc.stdout or ""
            output = partial if isinstance(partial, str) else partial.decode(errors="replace")
            output += f"\nCommand timed out after {command_timeout} seconds.\n"
            status = 124
        elapsed = time.monotonic() - started
        log_path = run_dir / f"command-{index}.log"
        log_path.write_text(output, encoding="utf-8")
        records.append((command, status, elapsed, log_path, output))

    per_command = max(400, output_limit // len(records))
    sections = ["RESULTS"]
    for index, (command, status, elapsed, log_path, output) in enumerate(records, 1):
        sections.extend(
            [
                "",
                f"[{index}] exit={status}",
                clipped(output, per_command) if output else "(no output)",
            ]
        )
    return clipped("\n".join(sections), output_limit)


def first_task(task: str, state_file: Path, has_history: bool) -> str:
    if has_history or not state_file.exists():
        return task
    state = state_file.read_text(encoding="utf-8").strip()
    if not state:
        return task
    return f"PROJECT STATE\n{state}\n\nTASK\n{task}"


def run_task(
    task: str,
    messages: list[dict[str, str]],
    session_path: Path,
    system_prompt: str,
    state_file: Path,
    client: WasteClient,
    workspace: Path,
    runs_root: Path,
    assume_yes: bool,
    max_rounds: int,
    output_limit: int,
    command_timeout: int,
) -> None:
    content = first_task(task, state_file, bool(messages))
    user_message = {"role": "user", "content": content}
    messages.append(user_message)
    append_message(session_path, user_message)

    for round_number in range(max_rounds + 1):
        reply, response = client.complete(
            [{"role": "system", "content": system_prompt}, *messages]
        )
        assistant_message = {"role": "assistant", "content": reply}
        messages.append(assistant_message)
        append_message(session_path, assistant_message)
        print(f"\nK3> {reply}\n", flush=True)

        finish_reason = response.get("choices", [{}])[0].get("finish_reason")
        if finish_reason == "length":
            raise ValueError(
                "K3 response reached the output-token limit; nothing was executed or saved"
            )

        kind, payload = parse_reply(reply)
        if kind == "save":
            filename, file_content = payload
            if not approve_save(filename, file_content, assume_yes):
                print("File not written.")
                return
            target = save_output(workspace, filename, file_content)
            print(f"Saved {target} ({len(file_content.encode('utf-8')):,} bytes).")
            return
        if kind != "act":
            return
        if round_number >= max_rounds:
            print(f"Stopped after {max_rounds} command rounds.", file=sys.stderr)
            return
        if not approve(payload, assume_yes):
            rejection = {
                "role": "user",
                "content": "The proposed command batch was not approved. Wait for user direction.",
            }
            messages.append(rejection)
            append_message(session_path, rejection)
            print("Batch not run.")
            return

        stamp = time.strftime("%Y%m%d-%H%M%S")
        result_text = execute_batch(
            payload,
            workspace,
            runs_root / f"{stamp}-round-{round_number + 1}",
            output_limit,
            command_timeout,
        )
        result_message = {"role": "user", "content": result_text}
        messages.append(result_message)
        append_message(session_path, result_message)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("task", nargs="?", help="run one task and exit")
    parser.add_argument("--session", default="studio", help="persistent session name")
    parser.add_argument("--workspace", type=Path, default=PROJECT_DIR)
    parser.add_argument("--system", type=Path, default=DEFAULT_SYSTEM)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA)
    parser.add_argument(
        "--usage-hotlist",
        type=Path,
        default=Path(os.getenv("K3_USAGE_PATH", str(DEFAULT_USAGE_HOTLIST))),
        help="live WASTE usage file to checkpoint after each completed task",
    )
    parser.add_argument("--yes", action="store_true", help="approve proposed command batches")
    parser.add_argument("--max-rounds", type=int, default=2)
    parser.add_argument("--max-output-chars", type=int, default=6000)
    parser.add_argument("--command-timeout", type=int, default=300)
    parser.add_argument("--request-timeout", type=int, default=3600)
    parser.add_argument("--max-tokens", type=int, default=384)
    parser.add_argument("--url", default=os.getenv("K3_URL", "http://127.0.0.1:8000/v1"))
    parser.add_argument("--model", default=os.getenv("K3_MODEL", "k3"))
    parser.add_argument("--api-key", default=os.getenv("K3_API_KEY", ""))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.max_rounds < 0 or args.max_output_chars < 400:
        raise SystemExit("max-rounds must be non-negative and max-output-chars at least 400")
    workspace = args.workspace.expanduser().resolve()
    if not workspace.is_dir():
        raise SystemExit(f"workspace is not a directory: {workspace}")
    system_file = args.system.expanduser().resolve()
    state_file = args.state.expanduser().resolve()
    data_dir = args.data_dir.expanduser().resolve()
    usage_hotlist = args.usage_hotlist.expanduser().resolve()
    session = safe_name(args.session)
    session_path = data_dir / "sessions" / f"{session}.jsonl"
    messages = load_messages(session_path)
    system_prompt = system_file.read_text(encoding="utf-8").strip()
    client = WasteClient(
        args.url, args.model, args.api_key, args.request_timeout, args.max_tokens
    )
    print(f"Session: {session} ({len(messages)} saved messages)")
    print(f"Workspace: {workspace}")

    if args.task:
        started = time.monotonic()
        run_task(
            args.task,
            messages,
            session_path,
            system_prompt,
            state_file,
            client,
            workspace,
            data_dir / "runs" / session,
            args.yes,
            args.max_rounds,
            args.max_output_chars,
            args.command_timeout,
        )
        snapshot = snapshot_usage_hotlist(
            usage_hotlist, data_dir, session, args.task,
            time.monotonic() - started)
        if snapshot is not None:
            print(f"Learning snapshot: {snapshot}")
        return 0

    print("Enter a task, or /quit to leave the warm session.")
    while True:
        try:
            task = input("\nyou> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return 0
        if not task:
            continue
        if task in {"/quit", "/exit"}:
            return 0
        started = time.monotonic()
        run_task(
            task,
            messages,
            session_path,
            system_prompt,
            state_file,
            client,
            workspace,
            data_dir / "runs" / session,
            args.yes,
            args.max_rounds,
            args.max_output_chars,
            args.command_timeout,
        )
        snapshot = snapshot_usage_hotlist(
            usage_hotlist, data_dir, session, task,
            time.monotonic() - started)
        if snapshot is not None:
            print(f"Learning snapshot: {snapshot}")


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1)
