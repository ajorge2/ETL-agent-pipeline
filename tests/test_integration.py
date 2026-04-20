"""
End-to-end integration test.

Requires:
  - PostgreSQL running at localhost:5432 (docker-compose up postgres)
  - ANTHROPIC_API_KEY set in environment or .env

Does NOT require Kafka — it exercises the pipeline components directly.
"""

import threading
import time
import uuid

import pytest
from fastapi.testclient import TestClient

from src.approval_api import app
from src.db import PipelineEvent, RepairProposal, SessionLocal, init_db
from src.validator import validate_user_signup
from src.agent import agent

MISSING_EMAIL_EVENT = {
    "user_id": "test-u1",
    "name": "Test User",
    "age": 29,
    "signup_timestamp": "2024-06-01T00:00:00Z",
}

TYPE_MISMATCH_EVENT = {
    "user_id": "test-u2",
    "email": "x@example.com",
    "name": "Test User 2",
    "age": "25",
    "signup_timestamp": "2024-06-01T00:00:00Z",
}


@pytest.fixture(scope="module", autouse=True)
def setup_db():
    init_db()


@pytest.fixture()
def api():
    return TestClient(app)


def _wait_for_proposal(event_id: str, timeout: int = 15) -> str | None:
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


def _store_failed_event(event: dict, violations: list[dict]) -> str:
    db = SessionLocal()
    try:
        record = PipelineEvent(
            event_data=event,
            validation_status="failed",
            violations=violations,
        )
        db.add(record)
        db.commit()
        return str(record.id)
    finally:
        db.close()


class TestValidation:
    def test_valid_event_passes(self):
        passed, violations = validate_user_signup(
            {"user_id": "v1", "email": "ok@example.com", "name": "OK", "age": 20, "signup_timestamp": "2024-01-01T00:00:00Z"}
        )
        assert passed is True
        assert violations == []

    def test_missing_email_fails(self):
        passed, violations = validate_user_signup(MISSING_EMAIL_EVENT)
        assert passed is False
        assert any(v["column"] == "email" for v in violations)

    def test_wrong_age_type_fails(self):
        passed, violations = validate_user_signup(TYPE_MISMATCH_EVENT)
        assert passed is False
        assert any(v["column"] == "age" for v in violations)


class TestApprovalAPI:
    def test_health(self, api):
        r = api.get("/health")
        assert r.status_code == 200
        assert r.json()["status"] == "ok"

    def test_approve_unknown_proposal_returns_404(self, api):
        r = api.post("/approve", json={"proposal_id": str(uuid.uuid4()), "decision": "approved"})
        assert r.status_code == 404

    def test_invalid_decision_returns_422(self, api):
        db = SessionLocal()
        try:
            event_record = PipelineEvent(event_data={}, validation_status="failed")
            db.add(event_record)
            db.commit()
            proposal = RepairProposal(
                event_id=event_record.id,
                violation_type="missing_field",
                field_name="email",
                repair_action="add_nullable_field",
                reasoning="test",
            )
            db.add(proposal)
            db.commit()
            proposal_id = str(proposal.id)
        finally:
            db.close()

        r = api.post("/approve", json={"proposal_id": proposal_id, "decision": "maybe"})
        assert r.status_code == 422


class TestEndToEnd:
    def test_repair_flow_missing_field(self, api):
        passed, violations = validate_user_signup(MISSING_EMAIL_EVENT)
        assert not passed

        event_id = _store_failed_event(MISSING_EMAIL_EVENT, violations)

        agent_result: dict = {}
        error_holder: list = []

        def run_agent():
            try:
                result = agent.invoke(
                    {
                        "event": MISSING_EMAIL_EVENT,
                        "event_id": event_id,
                        "violations": violations,
                        "proposal_id": None,
                        "approval_status": None,
                        "fixed_event": None,
                    }
                )
                agent_result.update(result)
            except Exception as exc:
                error_holder.append(exc)

        thread = threading.Thread(target=run_agent, daemon=True)
        thread.start()

        proposal_id = _wait_for_proposal(event_id, timeout=20)
        assert proposal_id is not None, "Agent did not create a repair proposal within 20 s"

        r = api.get(f"/proposals/{proposal_id}")
        assert r.status_code == 200
        assert r.json()["status"] == "pending"

        r = api.post(
            "/approve",
            json={
                "proposal_id": proposal_id,
                "decision": "approved",
                "reviewer_note": "Safe to add nullable email field",
            },
        )
        assert r.status_code == 200
        assert r.json()["status"] == "approved"

        thread.join(timeout=15)

        if error_holder:
            raise error_holder[0]

        assert not thread.is_alive(), "Agent thread did not finish"
        assert agent_result.get("approval_status") == "approved"
        assert agent_result.get("fixed_event") is not None

        db = SessionLocal()
        try:
            ev = db.query(PipelineEvent).filter_by(id=uuid.UUID(event_id)).first()
            prop = db.query(RepairProposal).filter_by(id=uuid.UUID(proposal_id)).first()
        finally:
            db.close()

        assert ev.validation_status == "repaired"
        assert prop.status == "approved"
        assert prop.fixed_event is not None
