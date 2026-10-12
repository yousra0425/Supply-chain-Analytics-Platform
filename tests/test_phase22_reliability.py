
import pytest
import os
from cassandra.cluster import Cluster
from src.utils.cassandra_connection import CassandraConnection

KEYSPACE = "inventory_v2"

FAILED_MESSAGE = "inventory.v2.raw:0:3919"


@pytest.fixture(scope="module")
def session():
    cluster = Cluster(
        [os.getenv("CASSANDRA_HOST", "127.0.0.1")],
        port=int(os.getenv("CASSANDRA_PORT", "9042")),
        connection_class=CassandraConnection,
    )
    try:
        connection = cluster.connect(KEYSPACE)
        yield connection
    finally:
        cluster.shutdown()


def test_processing_ledger_has_entries(session):
    rows = list(
        session.execute(
            "SELECT message_id, status "
            "FROM kafka_processing_ledger"
        )
    )

    assert len(rows) > 0
    assert all(row.message_id for row in rows)


def test_completed_messages_exist(session):
    rows = session.execute(
        "SELECT status FROM kafka_processing_ledger"
    )

    assert any(
        row.status == "COMPLETED"
        for row in rows
    )


def test_dead_lettered_message_exists(session):
    rows = list(
        session.execute(
            "SELECT message_id, status "
            "FROM kafka_processing_ledger"
        )
    )

    matches = [
        row for row in rows
        if row.message_id == FAILED_MESSAGE
    ]

    assert len(matches) == 1
    assert matches[0].status == "DEAD_LETTERED"


def test_ledger_message_ids_are_unique(session):
    rows = list(
        session.execute(
            "SELECT message_id FROM kafka_processing_ledger"
        )
    )

    ids = [row.message_id for row in rows]

    assert len(ids) == len(set(ids))


def test_recovery_record_exists(session):
    rows = list(
        session.execute("""
            SELECT
                original_message_id,
                destination_topic,
                recovery_status
            FROM dlq_recovery_ledger
        """)
    )

    matches = [
        row for row in rows
        if row.original_message_id == FAILED_MESSAGE
    ]

    assert len(matches) == 1
    assert matches[0].destination_topic == (
        "inventory.v2.recovery-test"
    )
    assert matches[0].recovery_status == "PUBLISHED"


def test_recovery_has_destination_offset(session):
    rows = list(
        session.execute("""
            SELECT
                original_message_id,
                destination_partition,
                destination_offset
            FROM dlq_recovery_ledger
        """)
    )

    matches = [
        row for row in rows
        if row.original_message_id == FAILED_MESSAGE
    ]

    assert len(matches) == 1
    assert matches[0].destination_partition >= 0
    assert matches[0].destination_offset >= 0
