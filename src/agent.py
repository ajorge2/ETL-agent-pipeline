import json
import time
import uuid
from typing import Optional, TypedDict

from anthropic import Anthropic
from langgraph.graph import END, StateGraph

from .config import settings
from .db import PipelineEvent, RepairProposal, SessionLocal


class AgentState(TypedDict):
    event: dict
    event_id: str
    violations: list[dict]
    proposal_id: Optional[str]
    approval_status: Optional[str]
    fixed_event: Optional[dict]


_PROPOSE_REPAIR_TOOL = {
    "name": "propose_repair",
    "description": "Propose a targeted repair for a schema or type violation in a user signup event.",
    "input_schema": {
        "type": "object",
        "properties": {
            "violation_type": {
                "type": "string",
                "description": "Short label for the class of violation, e.g. 'missing_field', 'type_mismatch', 'null_value'.",
            },
            "field_name": {
                "type": "string",
                "description": "The event field that violates the schema.",
            },
            "repair_action": {
                "type": "string",
                "enum": ["add_nullable_field", "coerce_to_type", "reject_event"],
                "description": "Repair strategy: add missing field as null, coerce value to target type, or reject the event entirely.",
            },
            "target_type": {
                "type": "string",
                "description": "Required when repair_action is coerce_to_type. Python type name: 'int', 'str', 'float'.",
            },
            "reasoning": {
                "type": "string",
                "description": "One-sentence justification for this repair choice.",
            },
        },
        "required": ["violation_type", "field_name", "repair_action", "reasoning"],
    },
}


def propose_repair(event: dict, violations: list[dict]) -> dict:
    """Ask Claude to propose one targeted repair for the first violation.

    Pure LLM call with forced tool use and no persistence, so the same logic is
    shared by the pipeline (analyze_and_propose) and the reliability eval in
    scripts/eval_repair_accuracy.py.
    """
    client = Anthropic(api_key=settings.anthropic_api_key)

    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        tools=[_PROPOSE_REPAIR_TOOL],
        tool_choice={"type": "any"},
        messages=[
            {
                "role": "user",
                "content": (
                    "You are an ETL schema-repair agent. Analyze the first violation only and propose one repair.\n\n"
                    "Pick the repair_action deliberately:\n"
                    "- coerce_to_type: ONLY when the value converts cleanly and unambiguously to the target "
                    'type (e.g. "25" -> int). Set target_type accordingly.\n'
                    "- reject_event: when the correct value cannot be determined -- a non-numeric string for a "
                    'numeric field (e.g. "twenty-five" for age), or a missing identity field such as user_id '
                    "that must not be fabricated.\n"
                    "- add_nullable_field: when a genuinely optional field is absent and filling it with null "
                    "is safe.\n\n"
                    f"Event:\n{json.dumps(event, indent=2)}\n\n"
                    f"Violations:\n{json.dumps(violations, indent=2)}"
                ),
            }
        ],
    )

    tool_block = next(b for b in response.content if b.type == "tool_use")
    return tool_block.input


def analyze_and_propose(state: AgentState) -> AgentState:
    proposal = propose_repair(state["event"], state["violations"])

    db = SessionLocal()
    try:
        record = RepairProposal(
            event_id=uuid.UUID(state["event_id"]),
            violation_type=proposal["violation_type"],
            field_name=proposal["field_name"],
            repair_action=proposal["repair_action"],
            target_type=proposal.get("target_type"),
            reasoning=proposal["reasoning"],
        )
        db.add(record)
        db.commit()
        proposal_id = str(record.id)
    finally:
        db.close()

    return {**state, "proposal_id": proposal_id}


def await_approval(state: AgentState) -> AgentState:
    deadline = time.time() + settings.approval_timeout_seconds

    while time.time() < deadline:
        db = SessionLocal()
        try:
            record = db.query(RepairProposal).filter_by(id=uuid.UUID(state["proposal_id"])).first()
            if record and record.status in ("approved", "rejected"):
                return {**state, "approval_status": record.status}
        finally:
            db.close()

        time.sleep(settings.poll_interval_seconds)

    return {**state, "approval_status": "timeout"}


def execute_fix(state: AgentState) -> AgentState:
    db = SessionLocal()
    try:
        proposal = db.query(RepairProposal).filter_by(id=uuid.UUID(state["proposal_id"])).first()
        event_record = db.query(PipelineEvent).filter_by(id=proposal.event_id).first()

        if proposal.repair_action == "reject_event":
            # An approved reject_event means drop the event, not synthesize a fix.
            event_record.validation_status = "rejected"
            db.commit()
            return {**state, "fixed_event": None}

        fixed = dict(state["event"])

        if proposal.repair_action == "add_nullable_field":
            fixed[proposal.field_name] = None
        elif proposal.repair_action == "coerce_to_type":
            raw = fixed.get(proposal.field_name)
            type_map = {"int": int, "str": str, "float": float}
            coerce = type_map.get(proposal.target_type or "", str)
            try:
                fixed[proposal.field_name] = coerce(raw)
            except (ValueError, TypeError):
                fixed[proposal.field_name] = None

        proposal.fixed_event = fixed
        event_record.validation_status = "repaired"

        db.commit()
        return {**state, "fixed_event": fixed}
    finally:
        db.close()


def log_rejection(state: AgentState) -> AgentState:
    db = SessionLocal()
    try:
        event_record = db.query(PipelineEvent).filter_by(id=uuid.UUID(state["event_id"])).first()
        event_record.validation_status = "rejected"
        db.commit()
    finally:
        db.close()
    return state


def _route(state: AgentState) -> str:
    return "execute_fix" if state["approval_status"] == "approved" else "log_rejection"


def _build_agent():
    g = StateGraph(AgentState)
    g.add_node("analyze_and_propose", analyze_and_propose)
    g.add_node("await_approval", await_approval)
    g.add_node("execute_fix", execute_fix)
    g.add_node("log_rejection", log_rejection)

    g.set_entry_point("analyze_and_propose")
    g.add_edge("analyze_and_propose", "await_approval")
    g.add_conditional_edges(
        "await_approval",
        _route,
        {"execute_fix": "execute_fix", "log_rejection": "log_rejection"},
    )
    g.add_edge("execute_fix", END)
    g.add_edge("log_rejection", END)

    return g.compile()


agent = _build_agent()
