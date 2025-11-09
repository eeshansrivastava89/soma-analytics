"""
A/B Test Analysis Module

This is your data science workspace. All analysis logic lives here.
Edit these functions to change what data/metrics are returned.

Think of this like notebook cells converted to functions.
"""

import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def get_db_connection():
    """
    Create database connection from environment variable.

    Returns:
        SQLAlchemy engine connected to Supabase PostgreSQL
    """
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("DATABASE_URL environment variable not set in .env")
    return create_engine(db_url)


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
            variant,
            completion_time_seconds,
            correct_words_count,
            total_guesses_count,
            timestamp,
            user_id
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

    # Interpret the difference
    if abs(pct_diff) > 20:
        if pct_diff > 0:
            interpretation = "🔴 Variant B is significantly harder (+20% time)"
        else:
            interpretation = "🟢 Variant B is surprisingly easier (-20% time)"
    elif abs(pct_diff) > 10:
        interpretation = "🟡 Moderate difficulty difference (10-20%)"
    else:
        interpretation = "⚪ Similar difficulty (<10% difference)"

    return {
        "time_difference_seconds": round(time_diff, 2),
        "percentage_difference": round(pct_diff, 1),
        "interpretation": interpretation,
        "variant_a_avg": round(variant_a['avg_completion_time'], 2),
        "variant_b_avg": round(variant_b['avg_completion_time'], 2),
        "variant_a_median": round(variant_a['median_completion_time'], 2),
        "variant_b_median": round(variant_b['median_completion_time'], 2),
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
