"""
Unit tests for the producer's filtering logic.

We import producer.py from the producer/ directory by manipulating sys.path.
This keeps the producer free of any test-specific imports.
"""
from __future__ import annotations

import importlib.util
import os

import pytest

# Load producer/producer.py by explicit file path under a unique module name.
# We can't simply `import producer`: the producer/ directory itself is an
# importable (namespace) package that shadows the producer.py module on
# sys.path, so a plain import resolves to the wrong thing. Loading by path
# under the name "wmf_producer" sidesteps the collision entirely.
_PRODUCER_PY = os.path.join(os.path.dirname(__file__), "..", "producer", "producer.py")
_spec = importlib.util.spec_from_file_location("wmf_producer", _PRODUCER_PY)
_producer = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_producer)

should_keep = _producer.should_keep


def make_event(**overrides) -> dict:
    """Build a baseline Wikimedia 'edit' event, with optional field overrides."""
    base = {
        "type": "edit",
        "bot": False,
        "server_name": "en.wikipedia.org",
        "title": "Test Article",
        "user": "TestUser",
        "namespace": 0,
        "timestamp": 1718287321,
        "meta": {
            "dt": "2024-06-13T14:22:01Z",
            "domain": "en.wikipedia.org",
        },
    }
    base.update(overrides)
    return base


class TestShouldKeep:
    """Filtering logic in producer.should_keep()."""

    def test_keeps_human_edit(self):
        assert should_keep(make_event()) is True

    def test_drops_bot_edit(self):
        assert should_keep(make_event(bot=True)) is False

    def test_drops_new_page_creation(self):
        assert should_keep(make_event(type="new")) is False

    def test_drops_log_event(self):
        assert should_keep(make_event(type="log")) is False

    def test_drops_categorize_event(self):
        assert should_keep(make_event(type="categorize")) is False

    def test_handles_missing_type_field(self):
        event = make_event()
        del event["type"]
        assert should_keep(event) is False

    def test_treats_missing_bot_field_as_human(self):
        # If bot field is absent, .get("bot") returns None which is falsy
        # → event is treated as human and kept.
        event = make_event()
        del event["bot"]
        assert should_keep(event) is True


@pytest.mark.parametrize("event_type", ["edit", "Edit", "EDIT"])
def test_keep_is_case_sensitive_on_type(event_type):
    """Sanity check: filter is case-sensitive — only lowercase 'edit' passes."""
    # Wikimedia uses lowercase 'edit' consistently. If the format ever changes,
    # this test surfaces it.
    result = should_keep(make_event(type=event_type))
    assert result is (event_type == "edit")
