# K3 Studio Jobs UI help

The UI is an editor and controller for small file-backed jobs. It does not use
a database. A job definition and all of its generated output sets live together
under one named directory in `jobs/`. Only transient status, events, session
transcripts, and logs live under `.k3/`.

## UI elements

| UI element | Stored as | Purpose |
|---|---|---|
| New job name | `job.json` | Human-readable job title. |
| Job slug | `jobs/<slug>/` | Stable directory name for the complete job bundle. |
| Maximum minutes | `job.json` | Hard wall-clock limit for a run. |
| Cycles | `job.json` | Number of times to repeat the enabled stage sequence. |
| Temperature cutoff | `job.json` | Stops the run if the Spark reaches this temperature. |
| Output token limit | `job.json` | Maximum K3 response length for each request. |
| Command rounds | `job.json` | Maximum number of bounded `ACT` exchanges per stage. |
| Auto-approve | `job.json` | Allows bounded shell actions without interactive approval. |
| Stop at first failed stage | `job.json` | Prevents later stages from running after a failure. |
| Brief | `BRIEF.md` | Project, desired outcome, audience, and constraints; sent with every stage. |
| System instructions | `SYSTEM.md` | K3's role, response protocol, and safety boundaries. |
| Durable state | `STATE.md` | Compact facts and decisions shared with every stage. |
| Stage order and enabled state | `job.json` | Determines which steps run and in what order. |
| Stage ID and title | `job.json` | Stable machine ID and readable label for a step. |
| Expected Markdown file | `job.json` | Deliverable path that must exist before the stage succeeds. |
| Stage prompt | `prompts/*.md` | Instructions specific to that step. |
| Start/stop/status | `.k3/jobs/` | Controls and reports the live process; not part of the reusable job. |
| Outputs | `jobs/<slug>/outputs/<run-id>/` | Uniquely identified result set for one run. |
| Recent log | `.k3/jobs/<slug>/runs/<run-id>/run.log` | Operational diagnostics kept outside the job bundle. |

## Directory layout

```text
jobs/studio-study/
├── job.json
├── BRIEF.md
├── SYSTEM.md
├── STATE.md
├── prompts/
│   ├── 01-ideas.md
│   ├── 02-critique.md
│   └── 03-synthesis.md
└── outputs/
    ├── 20260809T120000Z-a1b2c3/
    │   ├── inputs/
    │   │   ├── job.json
    │   │   ├── BRIEF.md
    │   │   ├── SYSTEM.md
    │   │   ├── STATE.md
    │   │   └── prompts/
    │   └── cycle-001/
    │       ├── 01_ideas.md
    │       ├── 02_critique.md
    │       └── 03_proposal.md
    └── 20260809T180000Z-d4e5f6/
        ├── inputs/
        └── cycle-001/
```

The timestamp-plus-random-suffix directory is the output-set ID. Running the
same instructions again creates another output set instead of overwriting the
first. The `inputs/` snapshot preserves the exact files used for that run, so a
result remains understandable even if the editable job definition changes
later. The harness also executes from this snapshot, so editing the reusable job
while a run is in progress cannot change that run halfway through.

## What the harness does

When a run starts, the harness validates the job, creates the uniquely named
output set, and snapshots its inputs. It then sends the shared system
instructions, durable state, brief, and each enabled stage prompt to K3 in
sequence. All stages in a cycle share the same cycle directory, so later stages
can inspect earlier deliverables. A stage succeeds only when its expected
Markdown file exists. The harness records operational status and logs under
`.k3/`, while the generated Markdown remains in the job's output set.

The `outputs/` directories are ignored by Git because studio results may be
private. Copy or publish an output set deliberately when you want to share it.
