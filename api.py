"""
SOMA Analytics - FastAPI Backend

Simple JSON API that returns analysis results.
All analysis logic lives in analysis/ab_test.py
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime
import sys
import os

# Add analysis module to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'analysis'))

# Import analysis functions
from ab_test import (
    get_variant_stats,
    get_conversion_funnel,
    get_recent_completions,
    get_comparison_metrics
)

app = FastAPI(
    title="SOMA Analytics API",
    description="Real-time analytics for SOMA projects",
    version="1.0.0"
)

# Enable CORS for Astro site to fetch data
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict to your domain
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.get("/")
def root():
    """API info"""
    return {
        "service": "SOMA Analytics API",
        "version": "1.0.0",
        "endpoints": [
            "/api/variant-stats",
            "/api/conversion-funnel",
            "/api/recent-completions",
            "/api/comparison"
        ],
        "docs": "/docs"
    }


@app.get("/health")
def health():
    """Health check for monitoring"""
    return {
        "status": "ok",
        "timestamp": datetime.utcnow().isoformat()
    }


@app.get("/api/variant-stats")
def variant_stats():
    """
    Get aggregated statistics for each variant (A/B).

    Returns list of variant stats with completion times, user counts, percentiles.
    """
    try:
        return get_variant_stats()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/conversion-funnel")
def conversion_funnel():
    """
    Get conversion funnel data (Started → Completed → Repeated).

    Returns list of funnel stages with event counts and user counts.
    """
    try:
        return get_conversion_funnel()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/recent-completions")
def recent_completions(limit: int = 100):
    """
    Get recent puzzle completions.

    Query params:
        limit: Number of recent completions to return (default: 100, max: 500)
    """
    try:
        if limit > 500:
            limit = 500
        return get_recent_completions(limit=limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/comparison")
def comparison():
    """
    Get comparison metrics between variant A and B.

    Returns time difference, percentage difference, and interpretation.
    """
    try:
        return get_comparison_metrics()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Run with: python api.py
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
