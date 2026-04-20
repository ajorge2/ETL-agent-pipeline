from pydantic import BaseModel
from typing import Optional
from enum import Enum


class RepairAction(str, Enum):
    ADD_NULLABLE_FIELD = "add_nullable_field"
    COERCE_TO_TYPE = "coerce_to_type"
    REJECT_EVENT = "reject_event"


class UserSignupEvent(BaseModel):
    user_id: str
    email: Optional[str] = None
    name: str
    age: Optional[int] = None
    signup_timestamp: str


class ViolationDetail(BaseModel):
    expectation_type: str
    column: str
    observed_value: Optional[str] = None


class ApprovalRequest(BaseModel):
    proposal_id: str
    decision: str
    reviewer_note: Optional[str] = None
