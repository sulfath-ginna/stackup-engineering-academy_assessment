"""
=============================================================
StackUp Engineering Academy — Data Engineering Assessment
Pillar 3 — Big Data Processing
Task 3.2 — Apache Kafka Real-Time Streaming

Trainee: sulfath_sahib
=============================================================

Topics:
    presight.project.events
        - 3 partitions

    presight.escalations.critical
        - 1 partition

Input:
    datasets/events_stream/events_2025_01.jsonl

Output:
    outputs/kafka/summary.json
=============================================================
"""

import argparse
import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timezone

from kafka import (
    KafkaProducer,
    KafkaConsumer,
    KafkaAdminClient,
)
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError


# ==============================================================================
# LOGGING
# ==============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


# ==============================================================================
# PATHS
# ==============================================================================

BASE_DIR = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../../../.."
    )
)

EVENTS_FILE = os.path.join(
    BASE_DIR,
    "datasets",
    "events_stream",
    "events_2025_01.jsonl"
)

OUTPUT_DIR = os.path.join(
    BASE_DIR,
    "outputs",
    "kafka"
)

os.makedirs(
    OUTPUT_DIR,
    exist_ok=True
)


# ==============================================================================
# KAFKA CONFIGURATION
# ==============================================================================

KAFKA_BOOTSTRAP = "localhost:9092"

TOPIC_EVENTS = (
    "presight.project.events"
)

TOPIC_ESCALATIONS = (
    "presight.escalations.critical"
)

CONSUMER_GROUP = (
    "presight-assessment-consumer"
)


# ==============================================================================
# 3.2a — CREATE TOPICS
# ==============================================================================

def create_topics():
    """
    Create assessment Kafka topics if they do not already exist.

    Required:
      presight.project.events
          partitions = 3

      presight.escalations.critical
          partitions = 1
    """

    logger.info(
        "Checking Kafka topics..."
    )

    admin = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        client_id="presight-assessment-admin"
    )

    existing_topics = set(
        admin.list_topics()
    )

    topics_to_create = []

    if TOPIC_EVENTS not in existing_topics:

        topics_to_create.append(
            NewTopic(
                name=TOPIC_EVENTS,
                num_partitions=3,
                replication_factor=1
            )
        )

    if TOPIC_ESCALATIONS not in existing_topics:

        topics_to_create.append(
            NewTopic(
                name=TOPIC_ESCALATIONS,
                num_partitions=1,
                replication_factor=1
            )
        )

    if topics_to_create:

        try:

            admin.create_topics(
                new_topics=topics_to_create,
                validate_only=False
            )

            logger.info(
                "Created %d Kafka topic(s).",
                len(topics_to_create)
            )

        except TopicAlreadyExistsError:

            logger.info(
                "Required Kafka topics already exist."
            )

    else:

        logger.info(
            "Required Kafka topics already exist."
        )

    admin.close()


# ==============================================================================
# PRODUCER
# ==============================================================================

def build_producer():
    """
    Build Kafka producer.

    Message value:
        JSON encoded UTF-8

    Message key:
        event_type encoded UTF-8
    """

    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,

        key_serializer=lambda key: (
            key.encode("utf-8")
            if key is not None
            else None
        ),

        value_serializer=lambda value: (
            json.dumps(value)
            .encode("utf-8")
        ),

        request_timeout_ms=30000,

        acks="all"
    )


def run_producer(
    producer,
    events_file: str,
    delay_seconds: float = 0.05
):
    """
    Read January event file and send each event to Kafka.

    Requirements:
      - Add produced_at timestamp.
      - Use event_type as Kafka key.
      - Sleep 50ms between messages.
      - Log every 100th event.
      - Track total messages sent.
    """

    logger.info(
        "Starting producer from: %s",
        events_file
    )

    sent_count = 0

    producer_start = time.time()

    with open(
        events_file,
        "r",
        encoding="utf-8"
    ) as file:

        for line in file:

            line = line.strip()

            if not line:
                continue

            event = json.loads(line)

            event["produced_at"] = (
                datetime.now(
                    timezone.utc
                ).isoformat()
            )

            event_type = event.get(
                "event_type"
            )

            producer.send(
                TOPIC_EVENTS,
                key=event_type,
                value=event
            )

            sent_count += 1

            if sent_count % 100 == 0:

                logger.info(
                    "Produced %d messages — "
                    "latest event_id=%s event_type=%s",
                    sent_count,
                    event.get("event_id"),
                    event_type
                )

            # Assessment requirement:
            # simulate streaming using 50ms delay.
            time.sleep(
                delay_seconds
            )

    producer.flush()

    elapsed = (
        time.time()
        - producer_start
    )

    logger.info(
        "Producer completed."
    )

    logger.info(
        "Total messages sent: %d",
        sent_count
    )

    if elapsed > 0:

        logger.info(
            "Producer throughput: %.2f messages/sec",
            sent_count / elapsed
        )

    producer.close()

    return sent_count


# ==============================================================================
# CONSUMER
# ==============================================================================

def build_consumer(topic: str):
    """
    Build consumer for the project event stream.

    auto_offset_reset='earliest'
        allows the assessment stream to be consumed from the beginning
        when no committed offset exists.

    enable_auto_commit=False
        avoids committing offsets during repeated assessment runs.
    """

    return KafkaConsumer(
        topic,

        bootstrap_servers=KAFKA_BOOTSTRAP,

        group_id=CONSUMER_GROUP,

        auto_offset_reset="earliest",

        enable_auto_commit=False,

        consumer_timeout_ms=10000,

        key_deserializer=lambda key: (
            key.decode("utf-8")
            if key is not None
            else None
        ),

        value_deserializer=lambda value: (
            json.loads(
                value.decode("utf-8")
            )
        )
    )


# ==============================================================================
# 3.2c / 3.2d / 3.2e — CONSUME AND FORWARD
# ==============================================================================

def run_consumer(consumer):
    """
    Consume project events.

    For every Critical escalation_raised event:
        forward to presight.escalations.critical

    Produce outputs/kafka/summary.json containing:
        run timestamp
        total messages consumed
        event counts
        critical escalations forwarded
        throughput
    """

    logger.info(
        "Starting consumer on topic: %s",
        TOPIC_EVENTS
    )

    # Separate producer used for forwarding
    # critical escalation messages.
    forwarding_producer = build_producer()

    event_counts = defaultdict(int)

    total_consumed = 0
    critical_forwarded = 0

    consumer_start = time.time()

    for message in consumer:

        event = message.value

        event_type = event.get(
            "event_type"
        )

        event_id = event.get(
            "event_id"
        )

        project_id = event.get(
            "project_id"
        )

        payload = (
            event.get("payload")
            or {}
        )

        total_consumed += 1

        event_counts[
            event_type
        ] += 1

        # --------------------------------------------------------------
        # Log only every 100th consumed message
        # --------------------------------------------------------------

        if total_consumed % 100 == 0:

            logger.info(
                "Consumed %d messages — "
                "event_id=%s event_type=%s project_id=%s",
                total_consumed,
                event_id,
                event_type,
                project_id
            )

        # --------------------------------------------------------------
        # Critical escalation forwarding
        # --------------------------------------------------------------

        severity = payload.get(
            "severity"
        )

        if (
            event_type
            == "escalation_raised"
            and severity
            == "Critical"
        ):

            forwarded_event = (
                event.copy()
            )

            forwarded_event[
                "forwarded_at"
            ] = (
                datetime.now(
                    timezone.utc
                ).isoformat()
            )

            forwarding_producer.send(
                TOPIC_ESCALATIONS,
                key=event_type,
                value=forwarded_event
            )

            critical_forwarded += 1

    forwarding_producer.flush()
    forwarding_producer.close()

    elapsed = (
        time.time()
        - consumer_start
    )

    throughput = (
        total_consumed / elapsed
        if elapsed > 0
        else 0.0
    )

    # --------------------------------------------------------------------------
    # Summary
    # --------------------------------------------------------------------------

    summary = {
        "run_timestamp": (
            datetime.now(
                timezone.utc
            ).isoformat()
        ),

        "source_topic": TOPIC_EVENTS,

        "critical_topic": (
            TOPIC_ESCALATIONS
        ),

        "total_messages_consumed": (
            total_consumed
        ),

        "event_type_counts": dict(
            sorted(
                event_counts.items()
            )
        ),

        "critical_escalations_forwarded": (
            critical_forwarded
        ),

        "throughput_messages_per_second": round(
            throughput,
            2
        )
    }

    summary_path = os.path.join(
        OUTPUT_DIR,
        "summary.json"
    )

    with open(
        summary_path,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            summary,
            file,
            indent=2
        )

    logger.info(
        "Consumer completed."
    )

    logger.info(
        "Total consumed: %d",
        total_consumed
    )

    logger.info(
        "Critical escalations forwarded: %d",
        critical_forwarded
    )

    logger.info(
        "Consumer throughput: %.2f messages/sec",
        throughput
    )

    logger.info(
        "Summary written to: %s",
        summary_path
    )

    consumer.close()

    return summary


# ==============================================================================
# ENTRY POINT
# ==============================================================================

def main():

    parser = argparse.ArgumentParser(
        description=(
            "StackUp Kafka Task 3.2 "
            "producer / consumer"
        )
    )

    parser.add_argument(
        "--mode",
        choices=[
            "producer",
            "consumer",
            "both"
        ],
        default="both"
    )

    args = parser.parse_args()

    # --------------------------------------------------------------------------
    # Ensure topics exist
    # --------------------------------------------------------------------------

    create_topics()

    # --------------------------------------------------------------------------
    # Producer only
    # --------------------------------------------------------------------------

    if args.mode == "producer":

        producer = build_producer()

        run_producer(
            producer,
            EVENTS_FILE
        )

    # --------------------------------------------------------------------------
    # Consumer only
    # --------------------------------------------------------------------------

    elif args.mode == "consumer":

        consumer = build_consumer(
            TOPIC_EVENTS
        )

        summary = run_consumer(
            consumer
        )

        print(
            "\nKafka summary:"
        )

        print(
            json.dumps(
                summary,
                indent=2
            )
        )

    # --------------------------------------------------------------------------
    # Producer then consumer
    # --------------------------------------------------------------------------

    elif args.mode == "both":

        producer = build_producer()

        sent_count = run_producer(
            producer,
            EVENTS_FILE
        )

        logger.info(
            "Producer phase sent %d messages.",
            sent_count
        )

        consumer = build_consumer(
            TOPIC_EVENTS
        )

        summary = run_consumer(
            consumer
        )

        print(
            "\n" + "=" * 70
        )

        print(
            "TASK 3.2 KAFKA SUMMARY"
        )

        print(
            "=" * 70
        )

        print(
            json.dumps(
                summary,
                indent=2
            )
        )


if __name__ == "__main__":
    main()