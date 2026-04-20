import uuid
from datetime import datetime

from sqlalchemy import Column, DateTime, ForeignKey, JSON, String, Text, create_engine
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import declarative_base, sessionmaker

from .config import settings

engine = create_engine(settings.postgres_dsn)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()


class PipelineEvent(Base):
    __tablename__ = "pipeline_events"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    event_data = Column(JSON, nullable=False)
    validation_status = Column(String(20), nullable=False)
    violations = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)


class RepairProposal(Base):
    __tablename__ = "repair_proposals"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    event_id = Column(UUID(as_uuid=True), ForeignKey("pipeline_events.id"), nullable=False)
    violation_type = Column(String(100), nullable=False)
    field_name = Column(String(100), nullable=False)
    repair_action = Column(String(50), nullable=False)
    target_type = Column(String(50))
    reasoning = Column(Text, nullable=False)
    status = Column(String(20), default="pending")
    reviewer_note = Column(Text)
    fixed_event = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
    decided_at = Column(DateTime)


def init_db() -> None:
    Base.metadata.create_all(engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
