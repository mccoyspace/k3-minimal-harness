from __future__ import annotations

import copy
import json
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path

from k3_job_ui import JobHTTPServer
from k3_jobs import (
    JobError,
    active_record,
    create_job,
    editor_payload,
    load_job,
    run_job,
    run_snapshot,
    save_editor_payload,
    start_job,
    stop_active_job,
)


FAKE_HARNESS = r"""#!/usr/bin/env python3
import re
import sys
from pathlib import Path

workspace = Path(sys.argv[sys.argv.index("--workspace") + 1])
task = sys.argv[-1]
match = re.search(r"SAVE\s+`?([A-Za-z0-9_./-]+\.md)`?", task)
if not match:
    raise SystemExit(64)
target = (workspace / match.group(1)).resolve()
target.parent.mkdir(parents=True, exist_ok=True)
target.write_text("# Fake K3 deliverable\n", encoding="utf-8")
"""

SLOW_HARNESS = r"""#!/usr/bin/env python3
import time
time.sleep(60)
"""


class JobFixture(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.jobs = self.root / "jobs"
        self.data = self.root / "data"
        self.harness = self.root / "fake-k3"
        self.harness.write_text(FAKE_HARNESS, encoding="utf-8")
        self.harness.chmod(0o755)

    def tearDown(self):
        self.temporary.cleanup()


class TestJobFiles(JobFixture):
    def test_create_validate_edit_and_reject_escape(self):
        create_job(self.jobs, "sample", "Sample Job")
        _, config = load_job(self.jobs, "sample")
        self.assertEqual(config["schema"], "k3.job.v1")
        payload = editor_payload(self.jobs, "sample")
        payload["config"]["name"] = "Edited Job"
        payload["files"]["BRIEF.md"] = "# Edited brief\n"
        saved = save_editor_payload(self.jobs, "sample", payload)
        self.assertEqual(saved["config"]["name"], "Edited Job")
        self.assertEqual(saved["files"]["BRIEF.md"], "# Edited brief\n")

        invalid = copy.deepcopy(saved)
        invalid["config"]["stages"][0]["prompt"] = "../outside.md"
        invalid["files"].pop("prompts/01-draft.md")
        with self.assertRaisesRegex(JobError, "escapes"):
            save_editor_payload(self.jobs, "sample", invalid)

    def test_runner_honors_cycles_and_records_outputs(self):
        job_dir, config = create_job(self.jobs, "sample", "Sample Job")
        config["max_cycles"] = 2
        config["temperature_limit_c"] = None
        (job_dir / "job.json").write_text(
            json.dumps(config), encoding="utf-8"
        )
        run_id = "20260808T000000Z-abcdef"
        status = run_job(
            self.jobs,
            self.data,
            self.harness,
            "sample",
            run_id=run_id,
            temperature_reader=lambda: None,
        )
        self.assertEqual(status, 0)
        snapshot = run_snapshot(self.jobs, self.data, "sample", run_id)
        self.assertEqual(snapshot["status"]["state"], "completed")
        self.assertEqual(snapshot["status"]["completed_stages"], 2)
        self.assertEqual(
            [item["path"] for item in snapshot["outputs"]],
            ["cycle-001/01_draft.md", "cycle-002/01_draft.md"],
        )
        output_set = job_dir / "outputs" / run_id
        self.assertTrue((output_set / "inputs" / "job.json").is_file())
        self.assertEqual(
            (output_set / "inputs" / "BRIEF.md").read_text(encoding="utf-8"),
            (job_dir / "BRIEF.md").read_text(encoding="utf-8"),
        )
        self.assertFalse((self.data / "sample" / "runs" / run_id / "cycle-001").exists())
        self.assertFalse(active_record(self.data)["alive"])

    def test_temperature_guard_stops_before_first_stage(self):
        create_job(self.jobs, "sample", "Sample Job")
        run_id = "20260808T000000Z-fedcba"
        status = run_job(
            self.jobs,
            self.data,
            self.harness,
            "sample",
            run_id=run_id,
            temperature_reader=lambda: 90.0,
        )
        self.assertEqual(status, 125)
        snapshot = run_snapshot(self.jobs, self.data, "sample", run_id)
        self.assertEqual(snapshot["status"]["state"], "temperature")
        self.assertEqual(snapshot["outputs"], [])

    def test_background_run_can_be_stopped(self):
        create_job(self.jobs, "sample", "Sample Job")
        self.harness.write_text(SLOW_HARNESS, encoding="utf-8")
        started = start_job(self.jobs, self.data, self.harness, "sample")
        active = None
        for _ in range(50):
            active = active_record(self.data)
            if active and active.get("alive"):
                break
            time.sleep(0.1)
        self.assertTrue(active and active["alive"])
        stopped = stop_active_job(self.data)
        self.assertEqual(stopped["run"], started["run"])
        snapshot = None
        for _ in range(100):
            snapshot = run_snapshot(
                self.jobs, self.data, "sample", started["run"]
            )
            if snapshot.get("status", {}).get("state") == "stopped":
                break
            time.sleep(0.1)
        self.assertEqual(snapshot["status"]["state"], "stopped")


class TestJobHTTP(JobFixture):
    def setUp(self):
        super().setUp()
        self.server = JobHTTPServer(
            ("127.0.0.1", 0),
            jobs_dir=self.jobs,
            data_dir=self.data,
            harness=self.harness,
        )
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        super().tearDown()

    def request(self, path: str, *, method: str = "GET", value=None):
        data = None if value is None else json.dumps(value).encode()
        request = urllib.request.Request(
            self.base + path,
            data=data,
            method=method,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.load(response)
        except urllib.error.HTTPError as exc:
            return exc.code, json.load(exc)

    def test_create_get_and_edit_job(self):
        status, created = self.request(
            "/api/jobs",
            method="POST",
            value={"slug": "web-job", "name": "Web Job"},
        )
        self.assertEqual(status, 201)
        self.assertEqual(created["slug"], "web-job")
        status, payload = self.request("/api/jobs/web-job")
        self.assertEqual(status, 200)
        payload["config"]["max_cycles"] = 3
        status, saved = self.request(
            "/api/jobs/web-job", method="PUT", value=payload
        )
        self.assertEqual(status, 200)
        self.assertEqual(saved["config"]["max_cycles"], 3)

    def test_loopback_ui_and_status(self):
        create_job(self.jobs, "sample", "Sample Job")
        with urllib.request.urlopen(self.base + "/", timeout=5) as response:
            html = response.read().decode()
        self.assertIn("K3 Studio Jobs", html)
        status, payload = self.request("/api/status?job=sample")
        self.assertEqual(status, 200)
        self.assertIsNone(payload["active"])
        self.assertIsNone(payload["latest"]["run"])

    def test_status_and_output_api_use_job_output_set(self):
        create_job(self.jobs, "sample", "Sample Job")
        run_id = "20260809T000000Z-abcdef"
        self.assertEqual(
            run_job(
                self.jobs,
                self.data,
                self.harness,
                "sample",
                run_id=run_id,
                temperature_reader=lambda: None,
            ),
            0,
        )
        status, payload = self.request("/api/status?job=sample")
        self.assertEqual(status, 200)
        self.assertEqual(payload["latest"]["output_set"], f"outputs/{run_id}")
        self.assertEqual(
            payload["latest"]["outputs"][0]["path"],
            "cycle-001/01_draft.md",
        )
        status, payload = self.request(
            f"/api/output/sample/{run_id}?path=cycle-001%2F01_draft.md"
        )
        self.assertEqual(status, 200)
        self.assertEqual(payload["content"], "# Fake K3 deliverable\n")


if __name__ == "__main__":
    unittest.main(verbosity=2)
