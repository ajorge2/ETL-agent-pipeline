import json
import logging

from kafka import KafkaConsumer

from .agent import agent
from .config import settings
from .db import PipelineEvent, SessionLocal, init_db
from .validator import validate_user_signup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def process_event(event: dict) -> None:
    passed, violations = validate_user_signup(event)

    db = SessionLocal()
    try:
        record = PipelineEvent(
            event_data=event,
            validation_status="passed" if passed else "failed",
            violations=violations if not passed else None,
        )
        db.add(record)
        db.commit()
        event_id = str(record.id)
    finally:
        db.close()

    if passed:
        logger.info("Event %s passed validation", event_id)
        return

    logger.info("Event %s failed validation (%d violations); invoking repair agent", event_id, len(violations))
    result = agent.invoke(
        {
            "event": event,
            "event_id": event_id,
            "violations": violations,
            "proposal_id": None,
            "approval_status": None,
            "fixed_event": None,
        }
    )
    logger.info(
        "Agent finished for event %s — approval=%s fixed=%s",
        event_id,
        result.get("approval_status"),
        result.get("fixed_event") is not None,
    )


def run() -> None:
    init_db()

    consumer = KafkaConsumer(
        settings.kafka_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_group_id,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
    )

    logger.info("Listening on topic '%s'", settings.kafka_topic)
    for message in consumer:
        try:
            process_event(message.value)
        except Exception:
            logger.exception("Unhandled error processing message offset=%s", message.offset)


if __name__ == "__main__":
    run()
