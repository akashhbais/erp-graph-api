# AI-Assisted Development Workflow

## Project: ERP Graph Intelligence Platform

---

## Phase 1: Requirements & Design

### Initial Brief
- Build production-grade ERP graph API
- Ingest SAP order-to-cash dataset
- Implement natural language query engine with guardrails
- Demonstrate data traceability

### AI Collaboration Strategy
Used Claude to:
1. **Review** requirements and suggest architectural patterns
2. **Design** service layers and dependency injection
3. **Identify** missing safety guardrails (SQL injection, domain filtering)

### Key Design Decisions
- **Graph model**: Entities and edges stored in canonical DuckDB schema
- **Layering**: Service → Route → FastAPI (clean separation of concerns)
- **Safety first**: Domain guard + SQL validator before execution

---

## Phase 2: Data Pipeline Implementation

### Problem Statement
- Dataset: 49 JSONL files in nested folders (order-to-cash entities)
- Challenge: Map SAP source fields to canonical schema

### AI-Assisted Process
1. Created profiling script to inspect raw tables
2. Used AI to generate field mappings (business_partner → customer, etc.)
3. Implemented lossy normalization (IDs, dates, decimals, currency codes)
4. Validated edge types post-ingestion

### Debugging Approach
- Ran count queries after each module:
  - `SELECT COUNT(*) FROM customer` → 8 ✓
  - `SELECT COUNT(*) FROM graph_edge` → 123 ✓
  - Verified edge_type distribution

### Outcome
- Full order-to-cash pipeline: Sales Order → Delivery → Billing → Journal Entry
- Graph edges properly reflect relationships

---

## Phase 3: API Layer & Guardrails

### Architecture
```
POST /chat/query
  ├─ DomainGuard       (reject politics, weather, jokes)
  ├─ SQLGenerator      (question → SQL)
  ├─ SQLValidator      (block DELETE, DROP, check tables)
  ├─ DuckDB execution  (run with LIMIT 200)
  └─ Return results
```

### Safety Features Implemented
1. **Domain Guard**
   - Whitelist: ERP keywords (customer, order, billing, etc.)
   - Blacklist: (politics, poem, weather, etc.)
   - Prevents LLM prompt injection

2. **SQL Validator**
   - Forbids: DROP, DELETE, INSERT, UPDATE, ALTER
   - Forbids: SELECT * (data minimization)
   - Enforces max LIMIT 200 per row
   - Whitelists allowed tables only

3. **Error Handling**
   - Malicious queries rejected with HTTP 403
   - Out-of-domain questions rejected with HTTP 400
   - Database errors logged but not exposed

### AI Prompting Approach
Tested with Claude:
- "Which customers generated the highest billing value?"
- "Which products appear most frequently in billing documents?"
- "What is total revenue by customer?"

---

## Phase 4: Production Hardening

### Improvements Applied
1. **Configuration management**
   - Centralized settings in `core/config.py`
   - Environment variables for DB path, LLM model, debug mode

2. **Database singleton**
   - Single connection reused across requests
   - Connection pooling via DuckDB's built-in mechanism

3. **OpenAPI metadata**
   - Added descriptions to all endpoints
   - `/docs` now comprehensive and self-documenting

4. **Comprehensive health checks**
   - Return DB path, version, status
   - Useful for deployment diagnostics

5. **Graph statistics**
   - New `GET /graph/stats` endpoint
   - Shows data completeness

---

## Phase 5: Testing & Validation

### Manual Testing Performed
```bash
# 1. Health
curl http://127.0.0.1:8000/health
# Expected: status=ok, database=connected

# 2. Node lookup
curl http://127.0.0.1:8000/graph/node/740506
# Expected: node metadata with edges

# 3. Neighbors
curl http://127.0.0.1:8000/graph/neighbors/740506
# Expected: center node + neighbors + edge list

# 4. Flow tracing
curl http://127.0.0.1:8000/graph/flow/BILLING-001
# Expected: full sales→delivery→billing→journal path

# 5. NL query
curl -X POST http://127.0.0.1:8000/chat/query \
  -d '{"question": "Which customers have highest billing?"}'
# Expected: generated SQL + results

# 6. Domain guard
curl -X POST http://127.0.0.1:8000/chat/query \
  -d '{"question": "Write a poem"}'
# Expected: HTTP 400, "This system only answers questions..."
```

### Edge Cases Tested
- Query with SELECT * → rejected ✓
- Query with DROP TABLE → rejected ✓
- Query accessing unauthorized table → rejected ✓
- Out-of-domain question → rejected ✓
- Nonexistent node → 404 ✓
- Database down → 500 with error ✓

---

## Key Learnings

### What Worked Well
1. **Layered architecture** made changes isolated and testable
2. **Guardrails-first mindset** prevented security issues early
3. **DuckDB** lightweight but powerful for analytics
4. **FastAPI** type hints and auto-documentation saved time

### Challenges & Resolutions
| Challenge | Resolution |
|-----------|-----------|
| SAP schema mapping unclear | Created profiling script to inspect raw tables |
| SQL injection risk | Implemented whitelist + forbid list validator |
| LLM prompt injection | Added domain guard before SQL generation |
| Route path duplication | Fixed prefix + route path separation |
| SELECT * data leaks | Enforced explicit column selection + LIMIT |

### AI Tool Usage
- **Claude**: Architecture design, code generation, guardrail logic
- **Strategy**: Ask for design first, then implementation, then validation
- **Verification**: Always test generated code with actual data

---

## Deployment Readiness

### Before Production
- [ ] Set `DEBUG=false`
- [ ] Use read-only database connection when possible
- [ ] Add authentication (JWT / API keys)
- [ ] Enable rate limiting
- [ ] Add structured logging
- [ ] Set up monitoring & alerts
- [ ] Use Gunicorn instead of uvicorn
- [ ] Add reverse proxy (nginx)

### Instrumentation
```python
# Example: structured logging
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@router.post("/chat/query")
def query(...):
    logger.info(f"Query: {question}", extra={"user_id": user_id})
    result = svc.execute_question(question)
    logger.info(f"Rows returned: {len(result.get('rows', []))}")
```

---

## Future Improvements

1. **LLM Integration**
   - Replace SQLGenerator placeholders with actual Claude/GPT calls
   - Few-shot examples for complex queries

2. **Caching**
   - Cache frequent queries (customers, top products)
   - Reduce DB load

3. **Pagination**
   - Return results in pages instead of hard limit

4. **Advanced Auth**
   - Row-level security based on user role
   - Customer-specific data filtering

5. **Analytics Dashboard**
   - Real-time metrics on query performance
   - Popular queries report

---

## Conclusion

This project demonstrates:
- **Data engineering**: JSONL → DuckDB normalization
- **Backend architecture**: Layered services, dependency injection
- **Safety-first mindset**: Guardrails at every layer
- **Production readiness**: Config management, health checks, docs

The AI assistance was most valuable in:
1. Rapid design iteration (days → hours)
2. Catching security issues early (guardrails)
3. Code generation + validation
4. Documentation & examples

Total time: ~6 hours of focused work + AI collaboration