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
import time
from functools import wraps

# Add analysis module to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'analysis'))

# Import analysis functions
from ab_test import (
    get_variant_stats,
    get_conversion_funnel,
    get_recent_completions,
    get_comparison_metrics,
    get_completion_time_distribution,
    get_leaderboard
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


def retry_on_failure(retries=2, delay=0.5):
    """Retry decorator for transient database failures"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < retries - 1:
                        time.sleep(delay)
                    continue
            raise last_exception
        return wrapper
    return decorator


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
            "/api/comparison",
            "/api/leaderboard"
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
@retry_on_failure(retries=2, delay=0.5)
def variant_stats():
    """
    Get aggregated statistics for each variant (A/B).
    Real-time data, no caching.
    """
    return get_variant_stats()


@app.get("/api/conversion-funnel")
@retry_on_failure(retries=2, delay=0.5)
def conversion_funnel():
    """
    Get conversion funnel data (Started → Completed → Repeated).

    Returns list of funnel stages with event counts and user counts.
    """
    return get_conversion_funnel()


@app.get("/api/recent-completions")
@retry_on_failure(retries=2, delay=0.5)
def recent_completions(limit: int = 100):
    """
    Get recent puzzle completions.

    Query params:
        limit: Number of recent completions to return (default: 100, max: 500)
    """
    if limit > 500:
        limit = 500
    return get_recent_completions(limit=limit)


@app.get("/api/comparison")
@retry_on_failure(retries=2, delay=0.5)
def comparison():
    """
    Get comparison metrics between variant A and B.
    Real-time data, no caching.
    """
    return get_comparison_metrics()


@app.get("/api/time-distribution")
@retry_on_failure(retries=2, delay=0.5)
def time_distribution():
    """
    Get raw completion times for KDE/histogram visualization.
    Returns all completion times for both variants.
    """
    return get_completion_time_distribution()


@app.get("/api/leaderboard")
@retry_on_failure(retries=2, delay=0.5)
def leaderboard(variant: str = 'A', limit: int = 10):
    """
    Get global leaderboard of top players by best completion time.
    
    Query params:
        variant: 'A' or 'B' (default: 'A')
        limit: Number of top players (default: 10, max: 50)
    """
    if variant not in ['A', 'B']:
        raise HTTPException(status_code=400, detail="variant must be 'A' or 'B'")
    if limit > 50:
        limit = 50
    if limit < 1:
        limit = 10
    
    return get_leaderboard(variant=variant, limit=limit)


# Run with: python api.py
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
