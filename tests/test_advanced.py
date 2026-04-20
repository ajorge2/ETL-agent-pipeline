"""
Advanced integration tests.

Requires:
  - PostgreSQL running at localhost:5433
  - ANTHROPIC_API_KEY in environment or .env
"""

import threading
import time
import uuid

import pytest
from fastapi.testclient import TestClient

from src.approval_api import app, _evaluate_proposal_sync
from src.db import PipelineEvent, RepairProposal, SessionLocal, init_db
from src.validator import validate_user_signup
from src.agent import agent

TYPE_MISMATCH_EVENT = {
    "user_id": "adv-u1",
    "email": "coerce@example.com",
    "name": "Coerce Test",
    "age": "25",
    "signup_timestamp": "2024-06-01T00:00:00Z",
}

MISSING_EMAIL_EVENT = {
    "user_id": "adv-u2",
    "name": "Missing Email",
    "age": 31,
    "signup_timestamp": "2024-06-01T00:00:00Z",
}

AMBIGUOUS_AGE_EVENT = {
    "user_id": "adv-u3",
    "email": "ambiguous@example.com",
    "name": "Ambiguous Age",
    "age": "twenty-five",
    "signup_timestamp": "2024-06-01T00:00:00Z",
}


@pytest.fixture(scope="module", autouse=True)
def setup_db():
    init_db()


@pytest.fixture()
def api():
    return TestClient(app)


def _store_event(event: dict, status: str, violations: list | None = None) -> str:
    db = SessionLocal()
    try:
        rec = PipelineEvent(event_data=event, validation_status=status, violations=violations)
        db.add(rec)
        db.commit()
        return str(rec.id)
    finally:
        db.close()


def _store_proposal(event_id: str, **kwargs) -> str:
    db = SessionLocal()
    try:
        rec = RepairProposal(
            event_id=uuid.UUID(event_id),
            violation_type=kwargs.get("violation_type", "missing_field"),
            field_name=kwargs.get("field_name", "email"),
            repair_action=kwargs.get("repair_action", "add_nullable_field"),
            target_type=kwargs.get("target_type"),
            reasoning=kwargs.get("reasoning", "test reasoning"),
        )
        db.add(rec)
        db.commit()
        return str(rec.id)
    finally:
        db.close()


def _wait_for_proposal(event_id: str, timeout: int = 20) -> str | None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        db = SessionLocal()
        try:
            row = db.query(RepairProposal).filter_by(event_id=uuid.UUID(event_id)).first()
            if row:
                return str(row.id)
        finally:
            db.close()
        time.sleep(1)
    return None


# ── Rejection flow ────────────────────────────────────────────────────────────

class TestRejectionFlow:
    def test_rejection_marks_event_rejected(self, api):
        # Disable autopilot so the server's background task doesn't race us
        api.post("/autopilot", json={"enabled": False})

        violations = [{"expectation_type": "expect_column_to_exist", "column": "email", "observed_value": ""}]
        event_id = _store_event(MISSING_EMAIL_EVENT, "failed", violations)

        result: dict = {}
        def run():
            result.update(agent.invoke({
                "event": MISSING_EMAIL_EVENT,
                "event_id": event_id,
                "violations": violations,
                "proposal_id": None,
                "approval_status": None,
                "fixed_event": None,
            }))

        t = threading.Thread(target=run, daemon=True)
        t.start()

        proposal_id = _wait_for_proposal(event_id, timeout=20)
        assert proposal_id is not None

        r = api.post("/approve", json={"proposal_id": proposal_id, "decision": "rejected", "reviewer_note": "drop it"})
        assert r.status_code == 200

        t.join(timeout=10)
        assert result.get("approval_status") == "rejected"

        db = SessionLocal()
        try:
            ev = db.query(PipelineEvent).filter_by(id=uuid.UUID(event_id)).first()
            assert ev.validation_status == "rejected"
        finally:
            db.close()

    def test_rejected_proposal_cannot_be_approved(self, api):
        event_id  = _store_event({}, "failed")
        proposal_id = _store_proposal(event_id)

        api.post("/approve", json={"proposal_id": proposal_id, "decision": "rejected"})
        r = api.post("/approve", json={"proposal_id": proposal_id, "decision": "approved"})
        assert r.status_code == 400
        assert "rejected" in r.json()["detail"]

    def test_approved_proposal_cannot_be_rejected(self, api):
        event_id  = _store_event({}, "failed")
        proposal_id = _store_proposal(event_id)

        api.post("/approve", json={"proposal_id": proposal_id, "decision": "approved"})
        r = api.post("/approve", json={"proposal_id": proposal_id, "decision": "rejected"})
        assert r.status_code == 400


# ── Type coercion end-to-end ──────────────────────────────────────────────────

class TestTypeCoercionEndToEnd:
    def test_string_age_coerced_to_int(self, api):
        passed, violations = validate_user_signup(TYPE_MISMATCH_EVENT)
        assert not passed
        assert any(v["column"] == "age" for v in violations)

        event_id = _store_event(TYPE_MISMATCH_EVENT, "failed", violations)

        result: dict = {}
        def run():
            result.update(agent.invoke({
                "event": TYPE_MISMATCH_EVENT,
                "event_id": event_id,
                "violations": violations,
                "proposal_id": None,
                "approval_status": None,
                "fixed_event": None,
            }))

        t = threading.Thread(target=run, daemon=True)
        t.start()

        proposal_id = _wait_for_proposal(event_id, timeout=20)
        assert proposal_id is not None

        r = api.post("/approve", json={"proposal_id": proposal_id, "decision": "approved"})
        assert r.status_code == 200

        t.join(timeout=15)

        db = SessionLocal()
        try:
            prop = db.query(RepairProposal).filter_by(id=uuid.UUID(proposal_id)).first()
            assert prop.fixed_event is not None
            if prop.repair_action == "coerce_to_type":
                assert prop.fixed_event.get("age") == 25
        finally:
            db.close()


# ── Autopilot endpoints ───────────────────────────────────────────────────────

class TestAutopilotEndpoints:
    def test_get_default_state(self, api):
        r = api.get("/autopilot")
        assert r.status_code == 200
        body = r.json()
        assert "enabled" in body

    def test_set_enabled(self, api):
        r = api.post("/autopilot", json={"enabled": True})
        assert r.status_code == 200
        assert r.json()["enabled"] is True
        api.post("/autopilot", json={"enabled": False})

    def test_dashboard_includes_autopilot(self, api):
        r = api.get("/dashboard")
        assert r.status_code == 200
        body = r.json()
        assert "autopilot" in body
        assert "pending_proposals" in body
        assert "recent_events" in body
        assert "stats" in body

    def test_stats_keys_present(self, api):
        r = api.get("/dashboard")
        stats = r.json()["stats"]
        for key in ("total", "passed", "repaired", "failed", "rejected"):
            assert key in stats


# ── Autopilot Claude evaluation ───────────────────────────────────────────────

class TestAutopilotEvaluation:
    def _make_proposal(self, repair_action: str, field: str = "email",
                       violation: str = "missing_field", target_type: str | None = None,
                       event: dict | None = None) -> RepairProposal:
        ev = event or MISSING_EMAIL_EVENT
        event_id = _store_event(ev, "failed")

        db = SessionLocal()
        try:
            prop = RepairProposal(
                event_id=uuid.UUID(event_id),
                violation_type=violation,
                field_name=field,
                repair_action=repair_action,
                target_type=target_type,
                reasoning="Auto-generated for test.",
            )
            db.add(prop)
            db.commit()
            db.refresh(prop)
            return prop
        finally:
            db.close()

    def test_approves_safe_repair(self):
        prop = self._make_proposal("add_nullable_field", field="email")
        decision, reasoning = _evaluate_proposal_sync(prop)
        assert decision == "approved"
        assert len(reasoning) > 0

    def test_rejects_ambiguous_coercion(self):
        prop = self._make_proposal(
            "coerce_to_type",
            field="age",
            violation="type_mismatch",
            target_type="int",
            event=AMBIGUOUS_AGE_EVENT,
        )
        decision, reasoning = _evaluate_proposal_sync(prop)
        assert decision == "rejected"
        assert len(reasoning) > 0

    def test_decision_has_reasoning(self):
        prop = self._make_proposal("add_nullable_field")
        decision, reasoning = _evaluate_proposal_sync(prop)
        assert decision in ("approved", "rejected")
        assert isinstance(reasoning, str) and len(reasoning) > 10
