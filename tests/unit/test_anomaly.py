"""test_anomaly.py — unit tests for include/anomaly.py's z-score detector."""
from anomaly import detect


def test_detect_returns_false_with_fewer_than_five_history_points():
    is_anomaly, z_score = detect(history=[0.9, 0.91, 0.92], latest=0.5)

    assert is_anomaly is False
    assert z_score == 0.0


def test_detect_flags_sharp_drop_below_threshold():
    stable_history = [0.97, 0.98, 0.97, 0.99, 0.98, 0.97]

    is_anomaly, z_score = detect(stable_history, latest=0.80)

    assert is_anomaly is True
    assert z_score < 0


def test_detect_does_not_flag_score_within_normal_range():
    stable_history = [0.97, 0.98, 0.97, 0.99, 0.98, 0.97]

    is_anomaly, z_score = detect(stable_history, latest=0.975)

    assert is_anomaly is False


def test_detect_respects_custom_z_threshold():
    stable_history = [0.97, 0.98, 0.97, 0.99, 0.98, 0.97]

    # A mild dip (z ≈ -1.06) that wouldn't clear the default threshold
    # (2.0) should clear a much more sensitive one (0.5).
    is_anomaly_default, _ = detect(stable_history, latest=0.968, z_threshold=2.0)
    is_anomaly_sensitive, _ = detect(stable_history, latest=0.968, z_threshold=0.5)

    assert is_anomaly_default is False
    assert is_anomaly_sensitive is True


def test_detect_zero_stdev_identical_score_is_not_anomaly():
    identical_history = [0.95] * 6

    is_anomaly, z_score = detect(identical_history, latest=0.95)

    assert is_anomaly is False
    assert z_score == 0.0


def test_detect_zero_stdev_any_deviation_is_anomaly():
    """
    When every prior run scored exactly the same, stdev is 0 and a
    z-score can't be computed (division by zero) — detect() falls back
    to "any deviation from the flat history is an anomaly" instead of
    crashing.
    """
    identical_history = [0.95] * 6

    is_anomaly, z_score = detect(identical_history, latest=0.80)

    assert is_anomaly is True
    assert z_score == 0.0
