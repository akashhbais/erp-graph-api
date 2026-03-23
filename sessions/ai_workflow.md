# AI Coding Session Workflow

## Overview
This document captures the AI-assisted development workflow used to build the ERP Graph Intelligence Platform.

## Session Logs

### Session 1: Architecture Design (30 min)
**Goal**: Design canonical schema and graph structure

**AI Prompts Used**:
- "Design a normalized ERP schema for order-to-cash flow"
- "What are the key entities and relationships in SAP data?"
- "How should I model a sales order → delivery → billing flow as a graph?"

**Decisions**:
- ✅ 11 entity tables (customer, product, sales_order, delivery, billing_document, journal_entry, etc.)
- ✅ DuckDB for canonical storage (OLAP-friendly)
- ✅ Graph edges for traceability (source → target → relationship)

**Output**: `schema.sql`, entity definitions

---

### Session 2: Data Ingestion Pipeline (45 min)
**Goal**: Build JSONL → DuckDB normalizer

**AI Prompts Used**:
- "How to parse nested JSONL files into flat tables?"
- "Handle missing/null values in ERP datasets"
- "Bulk insert optimization for DuckDB"

**Decisions**:
- ✅ Pandas for data transformation
- ✅ Automatic type inference + explicit casting
- ✅ Batch inserts for performance

**Output**: `ingest_dataset.py`

---

### Session 3: Graph Service Layer (1 hour)
**Goal**: Implement node lookup, neighbors, flow trace

**AI Prompts Used**:
- "Query graph neighbors recursively in SQL"
- "Order-to-cash path tracing algorithm"
- "Handle cycles and missing relationships gracefully"

**Decisions**:
- ✅ Recursive CTEs for neighbor traversal
- ✅ LEFT JOINs to tolerate missing intermediates
- ✅ Graph edge table for flexible relationships

**Output**: `graph_service.py`

---

### Session 4: NL-to-SQL Query Engine (1.5 hours)
**Goal**: Translate natural language to SQL safely

**AI Prompts Used**:
- "Prompt engineering for SQL generation"
- "How to constrain LLM to specific tables?"
- "Validate generated SQL for injection attacks"

**Decisions**:
- ✅ Structured prompt with table/column context
- ✅ Whitelist-based SQL validator
- ✅ SELECT-only enforcement
- ✅ LIMIT 200 rows max

**Output**: `sql_generator.py`, `sql_validator.py`

---

### Session 5: Guardrails (45 min)
**Goal**: Restrict off-topic queries

**AI Prompts Used**:
- "Keyword-based domain filtering for ERP"
- "What are common out-of-domain prompts?"
- "Strategy to detect adversarial inputs"

**Decisions**:
- ✅ Domain guard: whitelist ~50 ERP keywords
- ✅ Reject non-SELECT statements
- ✅ Block forbidden keywords (DROP, DELETE, etc.)

**Output**: `domain_guard.py`, `sql_validator.py`

---

### Session 6: FastAPI Integration (1 hour)
**Goal**: Build REST API with proper error handling

**AI Prompts Used**:
- "FastAPI dependency injection pattern"
- "How to structure routes across files?"
- "Proper HTTP status codes for errors"

**Decisions**:
- ✅ Routes split by domain (health, graph, chat, ui)
- ✅ 404 for not found, 400 for validation, 500 for server errors
- ✅ Singleton DB connection pattern

**Output**: `routes_*.py`, `main.py`

---

### Session 7: Graph UI - Vis.js (1.5 hours)
**Goal**: Interactive node exploration with metadata

**AI Prompts Used**:
- "Vis.js network layout and physics"
- "Fetch neighbors on node double-click"
- "Display node metadata on click"

**Decisions**:
- ✅ Vis.js for force-directed layout
- ✅ Button + double-click to expand
- ✅ Pre-populated metadata panel

**Output**: `graph.html`

---

### Session 8: Chat UI Integration (1 hour)
**Goal**: Build split-pane UI with graph + chat

**AI Prompts Used**:
- "CSS flexbox for split panels"
- "Append messages dynamically to chat"
- "Handle API responses and errors gracefully"

**Decisions**:
- ✅ HTML + vanilla JS (no framework overhead)
- ✅ Async/await for API calls
- ✅ User messages blue, assistant messages gray
- ✅ Auto-scroll on new messages

**Output**: `index.html`

---

### Session 9: Render Deployment (1.5 hours)
**Goal**: Deploy to free tier with Python runtime

**AI Prompts Used**:
- "Python 3.11 vs 3.14 compatibility on Render"
- "Why pydantic-core fails on free tier?"
- "DuckDB in /tmp vs persistent disk tradeoffs"

**Decisions**:
- ✅ Pin Python 3.11.9 (avoid 3.14 Rust build)
- ✅ Use /tmp for free tier (ephemeral)
- ✅ Startup script for ingestion on first run
- ✅ render.yaml for Blueprint deployment

**Output**: `render.yaml`, `start.sh`, `.python-version`

---

## Iteration Patterns

### Pattern 1: Test → Fail → Prompt → Fix
1. Local test: `curl http://127.0.0.1:8000/chat/query`
2. Observe error (e.g., SQL parsing bug)
3. Prompt: "Why does this SQL fail with DuckDB?"
4. Implement fix
5. Re-test

### Pattern 2: Expand Scope Incrementally
1. Start with `/health` endpoint
2. Add `/graph/stats`
3. Add `/graph/node/{id}`
4. Add `/graph/neighbors/{id}`
5. Add `/chat/query`
6. Add UI

### Pattern 3: Safety-First Guardrails
- Each feature first validates input
- Then executes
- Then returns or errors
- Failures never expose internals

---

## AI Tools Used

| Tool | Purpose | Sessions |
|------|---------|----------|
| **Claude** | Architecture, SQL, prompt engineering | 1-6, 9 |
| **GitHub Copilot** | Code completion, boilerplate | All |
| **Cursor** | IDE with AI chat, refactoring | All |

---

## Key Insights

1. **Prompt Clarity Matters**: Detailed prompts yield better SQL generation
2. **Guardrails First**: Safety must be baked in, not added after
3. **Iterative UI**: UI evolved from `/graph.html` → `/index.html` (split pane)
4. **Deployment Challenges**: Free tier constraints (no Rust, no persistent disk) drove decisions
5. **Type Safety**: Pydantic + DuckDB schema validation catches most bugs early

---

## Time Breakdown

| Component | Hours |
|-----------|-------|
| Architecture | 0.5 |
| Data Pipeline | 0.75 |
| Graph Service | 1.0 |
| NL Query Engine | 1.5 |
| Guardrails | 0.75 |
| API | 1.0 |
| Graph UI | 1.5 |
| Chat UI | 1.0 |
| Deployment | 1.5 |
| **Total** | **~9.5 hours** |

---

## Lessons for LLM-Assisted Development

✅ **Do**:
- Ask for algorithm explanations before implementation
- Test edge cases after AI suggests code
- Validate security-critical code manually
- Use AI for boilerplate, not just logic

❌ **Don't**:
- Copy-paste without understanding
- Skip error handling
- Trust generated SQL without validation
- Deploy without local testing

---

## Future Improvements (Out of Scope)

- [ ] Streaming responses from LLM
- [ ] Conversation memory across sessions
- [ ] Graph clustering/community detection
- [ ] Semantic search over entities
- [ ] Real-time data sync
- [ ] GraphQL endpoint

---

**Last Updated**: 26 March 2026