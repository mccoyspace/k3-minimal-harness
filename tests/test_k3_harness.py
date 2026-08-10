from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from k3_harness import WasteClient


class FakeResponse:
    def __init__(self, lines: list[bytes]):
        self.lines = lines

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def __iter__(self):
        return iter(self.lines)


def event(value) -> bytes:
    if value == "[DONE]":
        return b"data: [DONE]\n\n"
    return ("data: " + json.dumps(value) + "\n\n").encode()


class TestWasteClientMetrics(unittest.TestCase):
    def test_streaming_response_records_ttft_and_effective_rate(self):
        lines = [
            event({"choices": [{"delta": {"role": "assistant", "content": ""}}]}),
            event({"choices": [{"delta": {"content": "SAVE result.md\nhello\n"}}]}),
            event(
                {
                    "choices": [
                        {"delta": {}, "finish_reason": "stop"}
                    ]
                }
            ),
            event(
                {
                    "choices": [],
                    "usage": {
                        "prompt_tokens": 12,
                        "completion_tokens": 10,
                        "total_tokens": 22,
                    },
                }
            ),
            event({"choices": [], "waste": {"prefix_cache": {"status": "hit"}}}),
            event("[DONE]"),
        ]
        with tempfile.TemporaryDirectory() as temporary:
            metrics_path = Path(temporary) / "metrics.json"
            client = WasteClient(
                "http://127.0.0.1:8000/v1",
                "k3",
                "test-key",
                30,
                64,
                metrics_path,
            )
            with mock.patch(
                "k3_harness.urllib.request.urlopen",
                return_value=FakeResponse(lines),
            ), mock.patch(
                "k3_harness.time.monotonic", side_effect=[100.0, 102.0, 105.0]
            ):
                text, response = client.complete([{"role": "user", "content": "hi"}])

            self.assertEqual(text, "SAVE result.md\nhello\n")
            self.assertEqual(response["choices"][0]["finish_reason"], "stop")
            metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
            self.assertEqual(metrics["summary"]["first_response_ttft_s"], 2.0)
            self.assertEqual(metrics["summary"]["model_elapsed_s"], 5.0)
            self.assertEqual(metrics["summary"]["completion_tokens"], 10)
            self.assertEqual(metrics["summary"]["effective_tok_s"], 2.0)


if __name__ == "__main__":
    unittest.main()
