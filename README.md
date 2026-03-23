# ERP Graph Intelligence Platform

FastAPI-based backend for ERP order-to-cash analytics using DuckDB, graph-style traversal, and safe natural-language-to-SQL querying.

---

## Features

- Ingests SAP-style JSONL dataset into a canonical DuckDB schema
- Order-to-cash graph traversal:
  - Sales Order → Delivery → Billing → Journal Entry
- REST APIs for:
  - health
  - graph stats
  - node lookup
  - neighbors
  - flow trace
  - NL query (`/chat/query`)
- Guardrails:
  - domain filtering (ERP-only questions)
  - SQL validation (SELECT-only, allowed tables, no dangerous keywords)

---

## Repository Structure

```text
.
├── backend/
│   ├── app/
│   │   ├── main.py
│   │   ├── api/
│   │   ├── core/
│   │   ├── graph/
│   │   ├── query/
│   │   └── guardrails/
│   └── deploy/
│       └── start.sh
├── scripts/
│   ├── ingest_dataset.py
│   └── profile_raw_tables.py
├── sessions/
│   └── ai_workflow.md
├── data/
│   ├── duckdb/
│   └── raw/
├── render.yaml
├── requirements.txt
├── .env.example
└── README.md
```

> If evaluator requires `/src`, add a shim:
> - `src/main.py` → `from backend.app.main import app`

---

## Local Setup (Windows PowerShell)

1. Create and activate venv
```powershell
cd C:\GitHub\Task
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

2. Install dependencies
```powershell
pip install -r requirements.txt
```

3. Ingest dataset
```powershell
py .\scripts\ingest_dataset.py --raw-dir .\sap-order-to-cash-dataset --db-path .\data\duckdb\app.duckdb
```

4. Run API
```powershell
python -m uvicorn backend.app.main:app --reload
```

5. Open docs
- http://127.0.0.1:8000/docs

---

## API Endpoints

- `GET /health`
- `GET /graph/stats`
- `GET /graph/node/{node_id}`
- `GET /graph/neighbors/{node_id}`
- `GET /graph/flow/{billing_document_id}`
- `GET /graph/sample-flow`
- `POST /chat/query`

---

## Example Requests (PowerShell)

### Health
```powershell
Invoke-WebRequest http://127.0.0.1:8000/health | Select-Object -ExpandProperty Content
```

### Graph stats
```powershell
Invoke-WebRequest http://127.0.0.1:8000/graph/stats | Select-Object -ExpandProperty Content
```

### NL query
```powershell
$body = '{"question":"Which customers generated the highest billing value?"}'
Invoke-WebRequest http://127.0.0.1:8000/chat/query -Method POST -ContentType "application/json" -Body $body |
  Select-Object -ExpandProperty Content
```
## User Interface

### Graph + Chat (Combined)
```
GET /
```
- Left panel: Interactive graph explorer (expand nodes, inspect metadata)
- Right panel: Conversational query assistant
- Open: `https://<your-service>.onrender.com/`

### API Documentation
```
GET /docs
```

---

## Example Queries

**Q: Which customers generated the highest billing value?**
```
A: Nelson, Fitzpatrick and Jordan ($55,337.76), Nguyen-Davis ($4,769.30), ...
```

**Q: Trace order 90504248**
```
A: Sales Order → Delivery → Billing Document → Journal Entry
```

**Q: Write a poem**
```
A: This system only answers questions about the ERP dataset.
```

---

## Architecture

- **Frontend**: Vanilla JS + Vis.js (graph) + HTML (chat)
- **Backend**: FastAPI + DuckDB + Claude API
- **Database**: DuckDB (11 tables, order-to-cash schema)
- **Deployment**: Render (Python 3.11, free tier)
- **Guardrails**: SQL whitelist + domain filter

### Domain guard test
```powershell
$body = '{"question":"Write a poem"}'
Invoke-WebRequest http://127.0.0.1:8000/chat/query -Method POST -ContentType "application/json" -Body $body
```

---

## Render Deployment (Python Runtime)

### Required files
- `render.yaml`
- `backend/deploy/start.sh`
- `requirements.txt`
- `.python-version` (recommended: `3.11.9`)

### Render config (free tier)
- Runtime: Python
- Build: `pip install -r requirements.txt`
- Start: `bash backend/deploy/start.sh`
- Env vars:
  - `PYTHON_VERSION=3.11.9`
  - `DEBUG=false`
  - `APP_DB_PATH=/tmp/app.duckdb` (free-tier compatible)

> `/tmp` is ephemeral on free tier. Data may reset on restart/redeploy.

---

## Submission Checklist

- Public repository link
- `README.md` with setup steps
- `sessions/ai_workflow.md`
- Working API endpoints
- Guardrails enabled

---

## License

For assessment/demo use.