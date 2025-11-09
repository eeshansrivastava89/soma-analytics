# SOMA Analytics

FastAPI backend that queries Supabase and returns JSON for real-time analytics dashboards.

## Architecture

```
Python analysis functions (analysis/ab_test.py)
         ↓
FastAPI endpoints (api.py) - return JSON
         ↓
Vanilla JS in Astro site - fetch + render with Plotly.js
```

## Files

- `analysis/ab_test.py` - All analysis logic (pure Python functions)
- `api.py` - FastAPI endpoints (returns JSON)
- `requirements.txt` - Dependencies
- `.env` - DATABASE_URL (not in git)
- `Dockerfile` + `fly.toml` - Deployment config

## Local Development

```bash
# Install
pip install -r requirements.txt

# Test analysis functions
python3 analysis/ab_test.py

# Run API server
python api.py
# or: uvicorn api:app --reload

# Test endpoints
curl http://localhost:8000/api/variant-stats
```

## Add New Analysis

Edit `analysis/ab_test.py`:

```python
def get_my_new_metric():
    """Your analysis"""
    engine = get_db_connection()
    df = pd.read_sql("SELECT * FROM my_table", engine)
    return df.to_dict(orient='records')
```

Add endpoint in `api.py`:

```python
@app.get("/api/my-metric")
def my_metric():
    return get_my_new_metric()
```

That's it.

## Deploy

```bash
fly deploy
```

## Environment Variables

`.env` (local):
```
DATABASE_URL=postgresql://postgres:password@host:5432/postgres
```

Fly.io (production):
```bash
fly secrets set DATABASE_URL="postgresql://..."
```
