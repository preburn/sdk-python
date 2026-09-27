import pytest

from preburn._fallback import FALLBACK_CACHE_MAXIMUM_ENTRIES, FallbackCache


def test_unknown_customer_and_feature_default_to_allow() -> None:
    cache = FallbackCache()
    outcome = cache.outcome("customer_1", "text_to_video")
    if outcome != "allow":
        pytest.fail(f"outcome={outcome}")


def test_customer_and_feature_entry_wins_over_feature_entry() -> None:
    cache = FallbackCache()
    cache.record("customer_1", "text_to_video", "deny")
    cache.record("customer_2", "text_to_video", "allow")
    first_outcome = cache.outcome("customer_1", "text_to_video")
    if first_outcome != "deny":
        pytest.fail(f"customer=customer_1 outcome={first_outcome}")
    second_outcome = cache.outcome("customer_2", "text_to_video")
    if second_outcome != "allow":
        pytest.fail(f"customer=customer_2 outcome={second_outcome}")


def test_feature_entry_covers_customers_without_their_own() -> None:
    cache = FallbackCache()
    cache.record("customer_1", "text_to_video", "deny")
    outcome = cache.outcome("customer_3", "text_to_video")
    if outcome != "deny":
        pytest.fail(f"outcome={outcome}")
    other_feature_outcome = cache.outcome("customer_1", "speech_to_text")
    if other_feature_outcome != "allow":
        pytest.fail(f"other_feature_outcome={other_feature_outcome}")


def test_later_outcome_replaces_earlier() -> None:
    cache = FallbackCache()
    cache.record("customer_1", "text_to_video", "deny")
    cache.record("customer_1", "text_to_video", "allow")
    outcome = cache.outcome("customer_1", "text_to_video")
    if outcome != "allow":
        pytest.fail(f"outcome={outcome}")


def test_least_recently_used_entry_is_evicted() -> None:
    cache = FallbackCache()
    cache.record("customer_0", "text_to_video", "deny")
    cache.record("customer_1", "text_to_video", "allow")
    for index in range(FALLBACK_CACHE_MAXIMUM_ENTRIES - 3):
        cache.record(f"filler_{index}", "speech_to_text", "allow")
    if len(cache) != FALLBACK_CACHE_MAXIMUM_ENTRIES:
        pytest.fail(f"entries={len(cache)}")
    evicted_outcome = cache.outcome("customer_0", "text_to_video")
    if evicted_outcome != "allow":
        pytest.fail(f"customer entry kept evicted_outcome={evicted_outcome}")


def test_reading_an_entry_protects_it_from_eviction() -> None:
    cache = FallbackCache()
    cache.record("customer_0", "text_to_video", "deny")
    cache.record("customer_1", "text_to_video", "deny")
    cache.record("customer_2", "text_to_video", "allow")
    cache.outcome("customer_0", "text_to_video")
    for index in range(FALLBACK_CACHE_MAXIMUM_ENTRIES - 4):
        cache.record(f"filler_{index}", "speech_to_text", "allow")
    if len(cache) != FALLBACK_CACHE_MAXIMUM_ENTRIES:
        pytest.fail(f"entries={len(cache)}")
    read_outcome = cache.outcome("customer_0", "text_to_video")
    if read_outcome != "deny":
        pytest.fail(f"read entry evicted read_outcome={read_outcome}")
    unread_outcome = cache.outcome("customer_1", "text_to_video")
    if unread_outcome != "allow":
        pytest.fail(f"unread entry kept unread_outcome={unread_outcome}")
