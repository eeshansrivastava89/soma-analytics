"""
A/B Test Analysis Module

This is your data science workspace. All analysis logic lives here.
Edit these functions to change what data/metrics are returned.

Think of this like notebook cells converted to functions.
"""

import os
import pandas as pd
from sqlalchemy import create_engine, pool
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Create a single engine instance with connection pooling
_engine = None


def get_db_connection():
    """
    Get database connection with connection pooling.

    Returns:
        SQLAlchemy engine connected to Supabase PostgreSQL
    """
    global _engine
    
    if _engine is None:
        db_url = os.getenv("DATABASE_URL")
        if not db_url:
            raise ValueError("DATABASE_URL environment variable not set in .env")
        
        # Use connection pooling to handle concurrent requests
        # pool_size: number of connections to keep in pool
        # max_overflow: additional connections beyond pool_size
        # pool_recycle: recycle connections after 3600 seconds (Supabase default)
        _engine = create_engine(
            db_url,
            poolclass=pool.QueuePool,
            pool_size=5,
            max_overflow=10,
            pool_recycle=3600,
            pool_pre_ping=True  # Test connections before using them
        )
    
    return _engine


def get_variant_stats():
    """
    Load aggregated variant statistics from Supabase v_variant_stats view.

    This view contains pre-computed metrics for each variant (A/B).

    Returns:
        list of dict: Variant statistics with keys:
            - variant: 'A' or 'B'
            - total_completions: number of puzzle completions
            - unique_users: number of unique users
            - avg_completion_time: average time in seconds
            - median_completion_time: median time in seconds
            - min/max/p25/p75/p90/p95_completion_time: percentiles
    """
    engine = get_db_connection()
    query = "SELECT * FROM v_variant_stats ORDER BY variant"
    df = pd.read_sql(query, engine)

    # Convert to list of dicts for JSON serialization
    return df.to_dict(orient='records')


def get_conversion_funnel():
    """
    Load event-based conversion funnel from Supabase v_conversion_funnel view.

    Shows progression: Started → Completed → Repeated

    Returns:
        list of dict: Funnel data with keys:
            - variant: 'A' or 'B'
            - stage: 'Started', 'Completed', or 'Repeated'
            - stage_order: 1, 2, or 3
            - event_count: number of events
            - unique_users: number of unique users
    """
    engine = get_db_connection()
    query = "SELECT * FROM v_conversion_funnel ORDER BY variant, stage_order"
    df = pd.read_sql(query, engine)

    return df.to_dict(orient='records')


def get_recent_completions(limit=100):
    """
    Load recent puzzle completions from PostHog events table.

    Args:
        limit: Maximum number of recent completions to return (default 100)

    Returns:
        list of dict: Recent completions with keys:
            - variant: 'A' or 'B'
            - completion_time_seconds: time to complete puzzle
            - correct_words_count: number of words found
            - total_guesses_count: number of guesses made
            - timestamp: when puzzle was completed (ISO format)
            - user_id: anonymous user identifier
    """
    engine = get_db_connection()
    query = f"""
        SELECT
            variant as "Variant",
            completion_time_seconds as "Time to Complete",
            correct_words_count as "Correct Words",
            total_guesses_count as "Total Guesses",
            TO_CHAR(timestamp AT TIME ZONE 'America/Los_Angeles', 'YYYY-MM-DD HH24:MI:SS') AS When,
            properties ->> '$geoip_city_name' as City, 
            properties ->> '$geoip_country_name' as Country
        FROM posthog_events
        WHERE event = 'puzzle_completed'
          AND completion_time_seconds IS NOT NULL
        ORDER BY timestamp DESC
        LIMIT {limit}
    """
    df = pd.read_sql(query, engine)

    # Convert timestamp to string for JSON serialization
    if not df.empty and 'timestamp' in df.columns:
        df['timestamp'] = df['timestamp'].astype(str)

    return df.to_dict(orient='records')


def get_completion_time_distribution():
    """
    Load all completion times for KDE/histogram visualization.

    Returns:
        dict: Distribution data with keys:
            - variant_a_times: list of completion times for variant A
            - variant_b_times: list of completion times for variant B
    """
    engine = get_db_connection()
    query = """
        SELECT
            variant,
            completion_time_seconds
        FROM posthog_events
        WHERE event = 'puzzle_completed'
          AND completion_time_seconds IS NOT NULL
        ORDER BY variant, completion_time_seconds
    """
    df = pd.read_sql(query, engine)

    variant_a_times = df[df['variant'] == 'A']['completion_time_seconds'].tolist()
    variant_b_times = df[df['variant'] == 'B']['completion_time_seconds'].tolist()

    return {
        "variant_a_times": variant_a_times,
        "variant_b_times": variant_b_times
    }


def get_comparison_metrics():
    """
    Compute comparison between variant A and B.

    Returns:
        dict: Comparison metrics with keys:
            - time_difference_seconds: B avg - A avg
            - percentage_difference: % change from A to B
            - interpretation: human-readable interpretation
            - variant_a_avg: average time for variant A
            - variant_b_avg: average time for variant B
            - variant_a_completions: completion count for A
            - variant_b_completions: completion count for B
    """
    stats = get_variant_stats()

    if len(stats) < 2:
        return {
            "error": "Need data from both variants for comparison"
        }

    # Find variant A and B
    variant_a = next((v for v in stats if v['variant'] == 'A'), None)
    variant_b = next((v for v in stats if v['variant'] == 'B'), None)

    if not variant_a or not variant_b:
        return {
            "error": "Missing data for variant A or B"
        }

    # Calculate differences
    time_diff = variant_b['avg_completion_time'] - variant_a['avg_completion_time']
    pct_diff = (time_diff / variant_a['avg_completion_time'] * 100) if variant_a['avg_completion_time'] > 0 else 0

    return {
        "time_difference_seconds": round(time_diff, 2),
        "percentage_difference": round(pct_diff, 1),
        "variant_a_avg": round(variant_a['avg_completion_time'], 2),
        "variant_b_avg": round(variant_b['avg_completion_time'], 2),
        "variant_a_completions": int(variant_a['total_completions']),
        "variant_b_completions": int(variant_b['total_completions'])
    }


# For testing: Run this file directly to see sample output
if __name__ == "__main__":
    print("Testing analysis functions...\n")

    print("=== Variant Stats ===")
    print(get_variant_stats())
    print()

    print("=== Conversion Funnel ===")
    print(get_conversion_funnel())
    print()

    print("=== Comparison Metrics ===")
    print(get_comparison_metrics())
