# K3 Minimal Harness

A deliberately small, dependency-free agent loop for Kimi K3 served by
[WASTE](https://github.com/sqliteai/waste). It is designed for a model whose
tokens are valuable and slow: keep the server warm, maintain compact session
state, approve shell work explicitly, and let the model save a deliverable
without squeezing it through JSON.

This repository is the sanitized companion to the measured NVIDIA DGX Spark
work in [waste-spark](https://github.com/mccoyspace/waste-spark). It contains no
model weights, prompts, transcripts, outputs, machine-specific paths, or
learned usage files.

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
- A converted Kimi K3 WASTE container.
- Linux for `bin/start-waste-fast`; its defaults are the qualified DGX Spark
  profile.
- The `exp/studio-usage-learning` branch of
  [mccoyspace/waste-spark](https://github.com/mccoyspace/waste-spark/tree/exp/studio-usage-learning)
  for automatic expert-hotlist learning.
- Passwordless access to the narrowly scoped PM-QoS launcher if using Q0.

The model conversion and measured engine work are documented in
[GN100.md](https://github.com/mccoyspace/waste-spark/blob/spark/integration/docs/GN100.md).

## Start the optimized Spark server

Build `libwaste.so` in the WASTE checkout, then identify that checkout and the
converted model without putting either path in this repository:

```bash
export WASTE_REPO="$HOME/src/waste-spark"
export WASTE_MODEL="$HOME/models/k3.waste"
./bin/start-waste-fast
```

The defaults reproduce the qualified single-user Spark arm: CUDA KDA/dense/VQ,
Q0, ten selected CPU cores, two readers at depth two, and router lookahead six.
Override machine-specific values as needed:

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
./bin/check-waste
./bin/k3 --session studio \
  "Inspect this project and identify the next useful step."
```

Use `--workspace /path/to/project` to work somewhere other than this checkout.
Use a short, purpose-specific session name so warm conversations do not become
an ever-growing context.

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
