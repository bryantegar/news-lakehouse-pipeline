"""test_utils.py — unit tests for include/utils.py's shared transform helpers."""
import pytest
from utils import (
    age_group,
    channel_to_category,
    device_class,
    edu_level,
    follower_tier,
)


@pytest.mark.parametrize(
    "age,expected",
    [
        (None, "Unknown"),
        (0, "<18"),
        (17, "<18"),
        (18, "18-24"),   # lower boundary
        (24, "18-24"),   # upper boundary
        (25, "25-34"),
        (34, "25-34"),
        (35, "35-44"),
        (44, "35-44"),
        (45, "45-54"),
        (54, "45-54"),
        (55, "55+"),
        (100, "55+"),
    ],
)
def test_age_group_boundaries(age, expected):
    assert age_group(age) == expected


@pytest.mark.parametrize(
    "followers,expected",
    [
        (None, "nano"),
        (0, "nano"),
        (9_999, "nano"),
        (10_000, "micro"),   # lower boundary
        (99_999, "micro"),
        (100_000, "mid"),    # lower boundary
        (499_999, "mid"),
        (500_000, "macro"),  # lower boundary
        (999_999, "macro"),
        (1_000_000, "mega"),  # lower boundary
        (10_000_000, "mega"),
    ],
)
def test_follower_tier_boundaries(followers, expected):
    assert follower_tier(followers) == expected


@pytest.mark.parametrize(
    "edu,expected",
    [
        (None, 0),
        ("", 0),
        ("SD", 0),  # not in the mapping — falls back to 0, not a KeyError
        ("SMA", 1),
        ("D3", 2),
        ("S1", 3),
        ("S2", 4),
        ("S3", 5),
    ],
)
def test_edu_level_mapping(edu, expected):
    assert edu_level(edu) == expected


@pytest.mark.parametrize(
    "device,expected",
    [
        (None, "unknown"),
        ("", "unknown"),
        ("Mozilla/5.0 (Mobile)", "mobile"),
        ("Mozilla/5.0 (iPad; Tablet)", "tablet"),
        ("Mozilla/5.0 (Windows NT 10.0)", "desktop"),
        # case-insensitivity
        ("MOBILE Safari", "mobile"),
    ],
)
def test_device_class(device, expected):
    assert device_class(device) == expected


def test_channel_to_category_prefers_explicit_slug_over_id_lookup():
    assert channel_to_category("1", "some-custom-slug") == "some-custom-slug"


def test_channel_to_category_falls_back_to_known_id_mapping():
    assert channel_to_category("5", None) == "bola-sports"


def test_channel_to_category_unknown_id_falls_back_to_other():
    assert channel_to_category("999", None) == "other"


def test_channel_to_category_neither_id_nor_slug_falls_back_to_other():
    assert channel_to_category(None, None) == "other"
