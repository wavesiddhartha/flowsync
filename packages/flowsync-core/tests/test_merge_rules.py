import pytest
import time
from flowsync.stream import Stream, MergeRuleRegistry

def test_unknown_rule_name():
    with pytest.raises(ValueError):
        Stream(name="invalid:rule", merge_rule="unknown-rule")

@pytest.mark.asyncio
async def test_min_and_max_merge_rules():
    # Min merge rule
    s_min = Stream(name="score:min", merge_rule="min", initial=10)
    await s_min.push(5, {"ts": 1.0})
    assert await s_min.get() == 5
    await s_min.push(8, {"ts": 2.0})
    assert await s_min.get() == 5  # stays 5 since 5 is min

    # Max merge rule
    s_max = Stream(name="score:max", merge_rule="max", initial=10)
    await s_max.push(15, {"ts": 1.0})
    assert await s_max.get() == 15
    await s_max.push(12, {"ts": 2.0})
    assert await s_max.get() == 15  # stays 15 since 15 is max

@pytest.mark.asyncio
async def test_append_merge_rule():
    s_app = Stream(name="logs", merge_rule="append", initial=[])
    await s_app.push("event-1", {"ts": 1.0})
    await s_app.push("event-2", {"ts": 2.0})
    assert await s_app.get() == ["event-1", "event-2"]

@pytest.mark.asyncio
async def test_custom_merge_fn():
    # Custom merge logic: take string concatenation
    def concat_merge(existing, incoming, meta_exist, meta_in):
        merged_val = f"{existing}-{incoming}"
        return merged_val, meta_in

    s_custom = Stream(name="concat", merge_rule="custom", initial="A", merge_fn=concat_merge)
    await s_custom.push("B")
    assert await s_custom.get() == "A-B"
    await s_custom.push("C")
    assert await s_custom.get() == "A-B-C"

@pytest.mark.asyncio
async def test_registry_get():
    # Verify that registry maps correctly
    assert MergeRuleRegistry.get("lww") is not None
    assert MergeRuleRegistry.get("crdt-counter") is not None
    assert MergeRuleRegistry.get("crdt-set") is not None
    assert MergeRuleRegistry.get("crdt-text") is not None
    assert MergeRuleRegistry.get("min") is not None
    assert MergeRuleRegistry.get("max") is not None
    assert MergeRuleRegistry.get("append") is not None

    with pytest.raises(ValueError):
        MergeRuleRegistry.get("non-existent")
