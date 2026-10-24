# ETL Agent Pipeline

A production-grade event streaming pipeline with LLM-powered schema repair and a human-in-the-loop approval system. Kafka ingests raw signup events, Great Expectations validates them, and a LangGraph agent powered by Claude proposes targeted repairs for any violations. A FastAPI service surfaces those proposals to a dashboard where a human — or an optional autopilot — decides whether to apply or reject each fix. Every decision is persisted in PostgreSQL and overridable at any time.

---

## Architecture

```
Kafka Topic (user-signups)
        │
        ▼
  src/consumer.py
  ┌─────────────────────────────────────────┐
  │  1. Deserialize message                 │
  │  2. Validate with Great Expectations    │
  │  3. Persist PipelineEvent to Postgres   │
  │  4. On failure → invoke LangGraph agent │
  └─────────────────────────────────────────┘
        │ (failed events only)
        ▼
  src/agent.py  (LangGraph StateGraph)
  ┌──────────────────────────────────────────────────────┐
  │                                                      │
  │  analyze_and_propose                                 │
  │    └─ Claude (forced tool use) → RepairProposal      │
  │         persisted to Postgres                        │
  │                ↓                                     │
  │  await_approval                                      │
  │    └─ polls Postgres every N seconds                 │
  │         until status ≠ pending, or timeout           │
  │                ↓                                     │
  │  [conditional route on approval_status]              │
  │    approved → execute_fix → apply repair to event    │
  │    rejected/timeout → log_rejection                  │
  │                                                      │
  └──────────────────────────────────────────────────────┘
        │ (RepairProposal written to Postgres)
        ▼
  src/approval_api.py  (FastAPI)
  ┌──────────────────────────────────────────────────────┐
  │  Dashboard UI    → GET  /dashboard                   │
  │  Manual approve  → POST /approve                     │
  │  Event override  → POST /events/{id}/override        │
  │  Validate edit   → POST /validate                    │
  │  Autopilot ctrl  → GET/POST /autopilot               │
  │                                                      │
  │  Background task (asyncio):                          │
  │    every 5s → query pending proposals                │
  │             → Claude decides approve/reject          │
  │             → write decision to Postgres             │
  │             → agent's poll loop picks it up          │
  └──────────────────────────────────────────────────────┘
        │
        ▼
  static/index.html  (Bloomberg terminal UI)
```

---

## Stack

| Layer | Technology | Why |
|---|---|---|
| Streaming | Kafka + kafka-python | Industry-standard for event ingestion at scale |
| Validation | Great Expectations 0.18.x | Declarative schema contracts, structured violation output |
| Agent | LangGraph + Claude | Stateful multi-step agent with explicit graph topology |
| LLM | Anthropic claude-sonnet-4-6 | Forced tool use gives structured, parseable outputs |
| API | FastAPI | Async-native, automatic OpenAPI docs, lifespan context |
| Persistence | PostgreSQL + SQLAlchemy | Relational integrity between events and proposals |
| UI | Vanilla JS + JetBrains Mono | Zero dependencies, full control, ships as a single file |
| Tests | pytest + FastAPI TestClient | Real database, no mocks — catches integration failures |

---

## Key Design Decisions

### 1. The agent polls the database, not the API

When `await_approval` needs to know whether a proposal was approved or rejected, it queries PostgreSQL directly rather than calling the approval API over HTTP. This was a deliberate choice that paid off immediately during testing: the test suite can use FastAPI's `TestClient` to approve proposals without running a live server, eliminating a whole class of integration test complexity. It also means the agent has no network dependency on the API service and can run in a completely separate process.

### 2. Forced tool use for structured LLM outputs

Both the repair proposal agent and the autopilot evaluator use `tool_choice: {"type": "any"}`, which forces Claude to always respond through a defined tool schema rather than free text. This guarantees that every LLM response is machine-parseable and conforms to the expected shape — no output parsing, no regex, no prompt asking Claude to "respond in JSON." The schema itself is the contract.

### 3. Autopilot as an asyncio background task

The autopilot runs as a persistent `asyncio.create_task` launched in FastAPI's lifespan context, not as a cron job or a separate worker process. It wakes every 5 seconds, queries for pending proposals, and calls Claude synchronously via `asyncio.to_thread` to avoid blocking the event loop. The enabled flag is checked both before and inside each tick so toggling autopilot off takes effect immediately, even mid-run.

### 4. Validation before manual repair submission

When a human edits event data through the dashboard, the form hits `/validate` before it hits `/events/{id}/override`. Great Expectations runs against the edited payload and returns any remaining violations inline. The repair cannot be submitted until the data passes — preventing a human from accidentally introducing a different schema violation while fixing the original one.

### 5. Event-level override, not proposal-level

Early versions of the dashboard only allowed overriding autopilot decisions on existing proposals. The final design operates at the event level: any event in the log can be opened, edited field-by-field, and saved as repaired — regardless of whether an agent proposal exists. Missing fields (identified from the violations list) are surfaced as editable inputs automatically, since the event data itself doesn't contain keys for fields that were never present.

### 6. Great Expectations column guard

Great Expectations raises an internal exception if you run a value-level expectation (e.g., `expect_column_values_to_not_be_null`) on a column that doesn't exist in the DataFrame. The validator guards against this by checking `set(df.columns)` before registering any value-level expectations — only column existence checks run unconditionally. This keeps the violation output clean and prevents the pipeline from crashing on the most common class of bad data.

### 7. Integration tests without a running server

The test suite starts no external processes. `TestClient` wraps the FastAPI app in-process, the agent runs in a background `threading.Thread`, and `_wait_for_proposal` polls PostgreSQL directly until the agent writes its proposal. The test then approves or rejects via `TestClient`, and the agent's own DB poll loop picks up the decision. The only external dependency is a live PostgreSQL instance — no mocks, no in-memory databases, no monkey-patching.

### 8. Autopilot race condition in tests

An early test failure exposed a subtle race: a pre-created `RepairProposal` in `pending` status would be grabbed by the running server's autopilot background loop before the test could decide it. The fix was two-pronged: disable autopilot at the start of each test that needs manual control, and let the agent create its own proposal rather than pre-inserting one. This way the test always decides the proposal the agent actually generated, and there's no window for the background task to interfere.

---

## Data Models

**PipelineEvent**
- `id` — UUID primary key
- `event_data` — raw JSON payload
- `validation_status` — `passed | failed | repaired | rejected`
- `violations` — JSON array of Great Expectations results
- `created_at`

**RepairProposal**
- `id` — UUID primary key
- `event_id` — FK → PipelineEvent
- `violation_type` — e.g. `missing_field`, `type_mismatch`
- `field_name` — the field that violated the schema
- `repair_action` — `add_nullable_field | coerce_to_type | reject_event`
- `target_type` — Python type name, for coercions
- `reasoning` — Claude's one-sentence justification
- `status` — `pending | approved | rejected`
- `reviewer_note` — human or autopilot note
- `fixed_event` — JSON of the repaired payload
- `decided_at`

---

## Running Locally

**Prerequisites:** Docker, Python 3.11+, an `ANTHROPIC_API_KEY` in `.env`

```bash
# Infrastructure
docker compose up -d

# Dependencies
pip install -r requirements.txt

# Terminal 1 — approval dashboard
uvicorn src.approval_api:app --port 8003

# Terminal 2 — Kafka consumer + agent
python3 -m src.consumer

# Send test events (2 valid, 2 malformed, 1 valid)
python3 scripts/produce_events.py
```

Open `http://localhost:8003` to see the dashboard.

---

## Test Suite

```bash
python3 -m pytest tests/ -v
```

**`tests/test_integration.py`** — validator unit tests, API contract tests, end-to-end repair flow for a missing field.

**`tests/test_advanced.py`** — rejection flow with double-decision guards, type coercion end-to-end (string age coerced to int), autopilot endpoint contracts, dashboard shape assertions, and direct Claude evaluation tests verifying that the LLM makes correct decisions on unambiguous vs. genuinely ambiguous inputs.

All tests run against a real PostgreSQL instance. No mocks.

---

## Evaluation

`scripts/eval_repair_accuracy.py` measures repair reliability on a labeled set of
24 malformed events spanning four defect classes (cleanly-coercible types,
uncoercible types, missing optional fields, missing identity fields). It exercises
the real validator, repair proposer (`src.agent.propose_repair`), and autopilot
reviewer (`src.approval_api.evaluate_repair`) — only the Postgres persistence hop
is skipped, so the scores reflect the shipped decision logic. No Kafka or Postgres
needed; just an `ANTHROPIC_API_KEY`.

```bash
python3 scripts/eval_repair_accuracy.py --runs 3
```

Ground truth is deterministic: a value that converts cleanly is repaired, anything
ambiguous (a non-numeric age, a missing `user_id`) is dropped. Latest result —
**100% end-to-end keep/drop accuracy across 3 runs**, up from 79% before the agent
was taught to choose `reject_event` instead of always coercing and leaning on the
reviewer to catch unsafe fixes.

---

## Project Structure

```
.
├── docker-compose.yml          # Kafka, Zookeeper, Postgres
├── requirements.txt
├── scripts/
│   ├── produce_events.py       # sends 5 test events to Kafka
│   └── eval_repair_accuracy.py # labeled reliability eval (no infra needed)
├── src/
│   ├── config.py               # pydantic-settings, loads .env
│   ├── db.py                   # SQLAlchemy models + session
│   ├── validator.py            # Great Expectations validation
│   ├── agent.py                # LangGraph agent (4-node graph)
│   ├── consumer.py             # Kafka consumer loop
│   ├── approval_api.py         # FastAPI service + autopilot
│   └── models.py               # Pydantic request models
├── static/
│   └── index.html              # Dashboard UI
└── tests/
    ├── test_integration.py
    └── test_advanced.py
```

## Privacy and licensing

See [PRIVACY.md](PRIVACY.md) and [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md). The software license does not relicense third-party data or publications.
