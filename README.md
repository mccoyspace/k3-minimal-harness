# K3 Minimal Harness

A deliberately small, dependency-free agent loop for Kimi K3 served by
[WARP](https://github.com/sqliteai/warp). It is designed for a model whose
tokens are valuable and slow: keep the server warm, maintain compact session
state, approve shell work explicitly, and let the model save a deliverable
without squeezing it through JSON.

This repository is the sanitized companion to the measured NVIDIA DGX Spark
work in [warp-spark](https://github.com/mccoyspace/warp-spark). It contains no
model weights, private prompts, transcripts, outputs, machine-specific paths,
or learned usage files. The prompts under `jobs/studio-study/` are generic,
editable examples.

## What it does

K3 must reply in one of four forms:

```text
ACT {"commands":["command", "command"]}
SAVE relative/path.md
plain file content
DONE concise answer
ASK one necessary question
```

- `ACT` batches a small set of shell inspections or computations and asks for
  approval before running them.
- `SAVE` validates a workspace-relative path and writes the content atomically.
- Responses cut off at the token limit are recorded but never executed or
  saved.
- JSONL session transcripts, command logs, and expert-usage learning remain in
  the ignored `.k3/` directory.

This is not a general-purpose autonomous coding agent. It is intentionally an
"A+B" harness: a warm WASTE server plus one auditable Python file.

## Requirements

- Python 3.10 or newer; the harness itself uses only the standard library.
- A converted Kimi K3 WARP container. Kimi K2 is optional.
- Linux for the profile launchers; their defaults are qualified on DGX Spark.
- The `exp/k2-cuda-compat` branch of
  [mccoyspace/warp-spark](https://github.com/mccoyspace/warp-spark/tree/exp/k2-cuda-compat)
  for the K2 CUDA profile and automatic expert-hotlist learning.
- Passwordless access to the narrowly scoped PM-QoS launcher if using Q0.

The model conversion and measured engine work are documented in
[GN100.md](https://github.com/mccoyspace/warp-spark/blob/spark/integration/docs/GN100.md).

## Start an optimized Spark profile

Build `libwaste.so` in the WARP checkout, then identify that checkout and the
converted models without putting their paths in this repository:

```bash
export WASTE_REPO="$HOME/src/warp-spark"
export WASTE_K3_MODEL="$HOME/models/k3.waste"
export WASTE_K2_MODEL="$HOME/models/kimi-k2.waste"

./bin/restart-waste-profile k3
./bin/check-waste k3
```

The K3 defaults reproduce the qualified single-user Spark arm: CUDA
KDA/dense/VQ, Q0, ten selected CPU cores, two readers at depth two, and router
lookahead six. Override machine-specific values as needed:

```bash
export WASTE_CPU_LIST="5-9,15-19"
export WASTE_BUDGET_BYTES="95172120576"
export WASTE_PREFIX_CACHE="2G"
export WASTE_PREFIX_ENTRIES="4"
```

Q0 holds a low-latency PM-QoS constraint while the server child is alive. It
can increase system power and affects idle behavior system-wide during that
time, so the launcher is intended for a dedicated experimental machine.

In a second shell:

```bash
./bin/k3 --session studio \
  "Inspect this project and identify the next useful step."
```

`bin/check-waste [k3|k2]` fails unless the live process matches the selected
qualified profile, including CUDA paths, loaded CUDA runtime, CPU placement,
Q0, lookahead, and I/O configuration. `bin/restart-waste-fast` remains a
backward-compatible alias for selecting K3.

The experimental K2 profile uses a 72 GiB expert cache, lookahead six, two
readers at depth two, eight compute-only CPU threads, CUDA dense scope three,
and CUDA VQ3R mode two/group one. It averaged 2.75 tok/s across the final
three-family qualification and reached 3.00 tok/s in its best run, versus a
1.22 tok/s CPU fallback. Start it with:

```bash
./bin/restart-waste-profile k2
./bin/check-waste k2
```

The qualified K2 container currently uses raw `/v1/completions`; it does not
have WARP's supported `chat.json`, so this K3 chat harness is not silently
applied to it. The K2 launcher and switch are operational helpers for direct
completion clients until an exact K2 chat format is established.

Use `--workspace /path/to/project` to work somewhere other than this checkout.
Use a short, purpose-specific session name so warm conversations do not become
an ever-growing context.

## Studio jobs

Reusable unattended jobs live in `jobs/`. Each job is an ordinary folder with
`job.json`, `SYSTEM.md`, `STATE.md`, `BRIEF.md`, and Markdown stage prompts.
Generated work is stored beside those instructions under
`jobs/<job>/outputs/<run-id>/`. Each uniquely named output set includes an
`inputs/` snapshot of the exact configuration and Markdown instructions used,
plus one result directory per cycle. Operational status, events, transcripts,
and logs remain private under `.k3/jobs/`.

The command-line surface and browser UI use the same validator and runner:

```bash
./bin/k3-job list
./bin/k3-job validate studio-study
./bin/k3-job start studio-study
./bin/k3-job status studio-study
./bin/k3-job stop

./bin/start-k3-job-ui
```

The UI binds only to `127.0.0.1:8042`. From another computer, tunnel it rather
than exposing it to the network:

```bash
ssh -N -L 8042:127.0.0.1:8042 your-spark-host
```

Then open `http://127.0.0.1:8042`. The browser may close after a run starts;
the Spark runner continues independently. Runtime and temperature limits are
enforced during model requests, not just between stages. Only one job runs at
a time. See `UI_HELP.md` for a short explanation of every field and the on-disk
layout.

The Run tab records TTFT, completion-token counts, model time, stage wall time,
and effective output tok/s for each stage. The run summary uses total output
tokens divided by total model-request time, so long and short stages are
weighted honestly. Detailed request metrics stay with private runtime records
under `.k3/jobs/`; generated studio content is not added to Git.

`request_inactivity_timeout_seconds` limits silence between streamed response
events; it does not cap total generation time. `max_runtime_minutes` remains
the hard wall-clock limit for the complete job. Existing job files using the
older `request_timeout_seconds` name continue to load and are migrated when
saved through the UI.

## Private adaptive hotlist

The launcher learns into `.k3/learning/studio-usage.waste`, separate from the
container's known-good default `usage.waste`. WASTE saves the live expert-cache
ranking after each successful request and loads it on the next server start.
After each completed harness task, the harness checkpoints the candidate and
adds a provenance row containing only the session name, timings, sizes, and
cryptographic hashes—not the task text.

No cron job is needed. The request-completion hook captures the signal when it
exists, and task completion preserves checkpoints before later work can evict
different experts. Promotion is intentionally manual: compare a candidate
against the baseline before treating an in-use studio hotlist as a general
performance result.

All learning data, sessions, and logs are under `.k3/` and ignored by Git. Do
not force-add that directory to a public repository.

## Safety model

The system prompt forbids deletion, publishing, installation, spending, or
external contact without explicit permission. Shell batches and file saves
still require approval unless `--yes` is supplied. Reserve `--yes` for bounded,
isolated experiments with a hard deadline and a temperature supervisor.

## License

Apache-2.0. See `LICENSE`.
