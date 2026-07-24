"""
anomaly.py — statistical anomaly detection on the DQ scorecard.

This is deliberately NOT a trained ML model: with only a handful of
pipeline runs so far, there isn't enough history to train or validate
one honestly, and a black-box model nobody can explain is worse than a
simple one everybody can. A z-score against the rolling window of past
runs is the standard first step before reaching for anything heavier —
it's explainable, needs no training data, and catches the thing that
actually matters here: "this run's DQ score is unusual compared to
recent history", which a static `< 0.85` threshold alone can't see
(a score that's technically "fine" but dropped sharply from 0.98 to
0.87 deserves a look even though it wouldn't fail dbt test).

Next step if/when more history piles up: swap `detect` for an actual
seasonality-aware model (e.g. Prophet, given Bryan's existing forecasting
background) — the call site in the Spark job doesn't need to change.
"""
import statistics


def detect(history: list[float], latest: float, z_threshold: float = 2.0) -> tuple[bool, float]:
    """
    history: overall_score from the last N runs, oldest first, NOT
             including `latest`.
    Returns (is_anomaly, z_score). Needs at least 5 prior runs to say
    anything meaningful — returns (False, 0.0) before that.
    """
    if len(history) < 5:
        return False, 0.0

    mean = statistics.mean(history)
    stdev = statistics.stdev(history)
    if stdev == 0:
        return (latest != mean), 0.0

    z_score = (latest - mean) / stdev
    is_anomaly = abs(z_score) >= z_threshold
    return is_anomaly, z_score
