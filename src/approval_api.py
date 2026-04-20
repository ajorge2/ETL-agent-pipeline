import asyncio
import json
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime

from anthropic import Anthropic
from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sqlalchemy.orm import Session

from .config import settings
from .db import PipelineEvent, RepairProposal, SessionLocal, get_db, init_db
from .models import ApprovalRequest
from .validator import validate_user_signup

_static_dir = os.path.join(os.path.dirname(__file__), "..", "static")

_autopilot: dict = {"enabled": False}

_DECISION_TOOL = {
    "name": "submit_decision",
    "description": "Submit an approval decision for a repair proposal.",
    "input_schema": {
        "type": "object",
        "properties": {
            "decision": {
                "type": "string",
                "enum": ["approved", "rejected"],
            },
            "reasoning": {
                "type": "string",
                "description": "One sentence explaining the decision.",
            },
        },
        "required": ["decision", "reasoning"],
    },
}


def _evaluate_proposal_sync(proposal: RepairProposal) -> tuple[str, str]:
    db = SessionLocal()
    try:
        event = db.query(PipelineEvent).filter_by(id=proposal.event_id).first()
        event_data = event.event_data if event else {}
    finally:
        db.close()

    client = Anthropic(api_key=settings.anthropic_api_key)
    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=256,
        tools=[_DECISION_TOOL],
        tool_choice={"type": "any"},
        messages=[
            {
                "role": "user",
                "content": (
                    "You are an autopilot agent for an ETL pipeline. "
                    "Approve the repair if it is safe and technically valid. "
                    "Reject if the correct value cannot be determined or data integrity is at risk.\n\n"
                    f"Repair proposal:\n"
                    f"  field:   {proposal.field_name}\n"
                    f"  action:  {proposal.repair_action}"
                    + (f" → {proposal.target_type}" if proposal.target_type else "")
                    + f"\n  reason:  {proposal.reasoning}\n\n"
                    f"Original event: {json.dumps(event_data)}\n\n"
                    "Make a decision."
                ),
            }
        ],
    )

    tool_block = next(b for b in response.content if b.type == "tool_use")
    result = tool_block.input
    return result["decision"], result["reasoning"]


async def _autopilot_loop() -> None:
    while True:
        await asyncio.sleep(5)
        if not _autopilot["enabled"]:
            continue
        try:
            await asyncio.to_thread(_run_autopilot_tick)
        except Exception as exc:
            print(f"[autopilot] error: {exc}")


def _run_autopilot_tick() -> None:
    db = SessionLocal()
    try:
        pending = db.query(RepairProposal).filter_by(status="pending").all()
        for proposal in pending:
            if not _autopilot["enabled"]:
                break
            decision, reasoning = _evaluate_proposal_sync(proposal)
            proposal.status = decision
            proposal.reviewer_note = f"autopilot: {reasoning}"
            proposal.decided_at = datetime.utcnow()
            db.commit()
            print(f"[autopilot] {decision} proposal {proposal.id} ({proposal.field_name})")
    finally:
        db.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    task = asyncio.create_task(_autopilot_loop())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(title="ETL Approval Service", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=_static_dir), name="static")


@app.get("/", include_in_schema=False)
def serve_ui():
    return FileResponse(os.path.join(_static_dir, "index.html"))


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/autopilot")
def get_autopilot():
    return _autopilot


class AutopilotUpdate(BaseModel):
    enabled: bool


@app.post("/autopilot")
def set_autopilot(body: AutopilotUpdate):
    _autopilot["enabled"] = body.enabled
    return _autopilot


@app.get("/dashboard")
def get_dashboard(db: Session = Depends(get_db)):
    pending_rows = (
        db.query(RepairProposal)
        .filter_by(status="pending")
        .order_by(RepairProposal.created_at.asc())
        .all()
    )

    pending = []
    for p in pending_rows:
        event = db.query(PipelineEvent).filter_by(id=p.event_id).first()
        pending.append(
            {
                "id": str(p.id),
                "violation_type": p.violation_type,
                "field_name": p.field_name,
                "repair_action": p.repair_action,
                "target_type": p.target_type,
                "reasoning": p.reasoning,
                "created_at": p.created_at.isoformat(),
                "event": event.event_data if event else {},
                "violations": event.violations if event else [],
            }
        )

    event_rows = (
        db.query(PipelineEvent)
        .order_by(PipelineEvent.created_at.desc())
        .limit(30)
        .all()
    )

    event_ids = [e.id for e in event_rows]
    proposals_by_event: dict = {}
    if event_ids:
        for p in db.query(RepairProposal).filter(RepairProposal.event_id.in_(event_ids)).all():
            proposals_by_event[str(p.event_id)] = p

    all_statuses = [e.validation_status for e in event_rows]
    stats = {
        "total":    len(event_rows),
        "passed":   all_statuses.count("passed"),
        "repaired": all_statuses.count("repaired"),
        "rejected": all_statuses.count("rejected"),
        "failed":   all_statuses.count("failed"),
    }

    def _serialize_event(e):
        prop = proposals_by_event.get(str(e.id))
        return {
            "id": str(e.id),
            "validation_status": e.validation_status,
            "event_data": e.event_data,
            "violations": e.violations,
            "created_at": e.created_at.isoformat(),
            "proposal": {
                "id": str(prop.id),
                "field_name": prop.field_name,
                "repair_action": prop.repair_action,
                "target_type": prop.target_type,
                "reasoning": prop.reasoning,
                "status": prop.status,
                "reviewer_note": prop.reviewer_note,
            } if prop else None,
        }

    return {
        "pending_proposals": pending,
        "recent_events": [_serialize_event(e) for e in event_rows],
        "stats": stats,
        "autopilot": dict(_autopilot),
    }


@app.get("/proposals")
def list_proposals(db: Session = Depends(get_db)):
    rows = db.query(RepairProposal).order_by(RepairProposal.created_at.desc()).limit(20).all()
    return [
        {
            "id": str(r.id),
            "event_id": str(r.event_id),
            "field_name": r.field_name,
            "repair_action": r.repair_action,
            "reasoning": r.reasoning,
            "status": r.status,
            "created_at": r.created_at.isoformat(),
        }
        for r in rows
    ]


@app.get("/proposals/{proposal_id}")
def get_proposal(proposal_id: str, db: Session = Depends(get_db)):
    row = db.query(RepairProposal).filter_by(id=uuid.UUID(proposal_id)).first()
    if not row:
        raise HTTPException(status_code=404, detail="Proposal not found")
    return {
        "id": str(row.id),
        "event_id": str(row.event_id),
        "violation_type": row.violation_type,
        "field_name": row.field_name,
        "repair_action": row.repair_action,
        "target_type": row.target_type,
        "reasoning": row.reasoning,
        "status": row.status,
        "reviewer_note": row.reviewer_note,
        "fixed_event": row.fixed_event,
        "created_at": row.created_at.isoformat(),
        "decided_at": row.decided_at.isoformat() if row.decided_at else None,
    }


@app.post("/approve")
def approve_proposal(request: ApprovalRequest, db: Session = Depends(get_db)):
    row = db.query(RepairProposal).filter_by(id=uuid.UUID(request.proposal_id)).first()
    if not row:
        raise HTTPException(status_code=404, detail="Proposal not found")
    if row.status != "pending":
        raise HTTPException(status_code=400, detail=f"Proposal is already '{row.status}'")
    if request.decision not in ("approved", "rejected"):
        raise HTTPException(status_code=422, detail="decision must be 'approved' or 'rejected'")

    row.status = request.decision
    row.reviewer_note = request.reviewer_note
    row.decided_at = datetime.utcnow()
    db.commit()

    return {"proposal_id": str(row.id), "status": row.status}


@app.post("/override")
def override_proposal(request: ApprovalRequest, db: Session = Depends(get_db)):
    row = db.query(RepairProposal).filter_by(id=uuid.UUID(request.proposal_id)).first()
    if not row:
        raise HTTPException(status_code=404, detail="Proposal not found")
    if request.decision not in ("approved", "rejected"):
        raise HTTPException(status_code=422, detail="decision must be 'approved' or 'rejected'")

    row.status = request.decision
    row.reviewer_note = f"human override: {request.reviewer_note or ''}"
    row.decided_at = datetime.utcnow()

    event = db.query(PipelineEvent).filter_by(id=row.event_id).first()
    if event:
        event.validation_status = "repaired" if request.decision == "approved" else "rejected"

    db.commit()
    return {"proposal_id": str(row.id), "status": row.status}


@app.post("/validate")
def validate_event(event: dict):
    passed, violations = validate_user_signup(event)
    return {"passed": passed, "violations": violations}


class EventOverride(BaseModel):
    action: str
    event_data: dict | None = None


@app.post("/events/{event_id}/override")
def override_event(event_id: str, body: EventOverride, db: Session = Depends(get_db)):
    if body.action not in ("repair", "reject"):
        raise HTTPException(status_code=422, detail="action must be 'repair' or 'reject'")
    event = db.query(PipelineEvent).filter_by(id=uuid.UUID(event_id)).first()
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")
    if body.action == "repair":
        if body.event_data is not None:
            event.event_data = body.event_data
        event.validation_status = "repaired"
    else:
        event.validation_status = "rejected"
    db.commit()
    return {"event_id": event_id, "status": event.validation_status}


@app.get("/events")
def list_events(db: Session = Depends(get_db)):
    rows = db.query(PipelineEvent).order_by(PipelineEvent.created_at.desc()).limit(20).all()
    return [
        {
            "id": str(r.id),
            "validation_status": r.validation_status,
            "event_data": r.event_data,
            "violations": r.violations,
            "created_at": r.created_at.isoformat(),
        }
        for r in rows
    ]
