
from types import SimpleNamespace

from src.streaming.dead_letter import publish_dead_letter


def main():
    # Synthetic message: does not modify inventory data.
    test_message = SimpleNamespace(
        topic="inventory.v2.raw",
        partition=0,
        offset=-1,
        value={
            "event_id": "dlq-test-only",
            "material": "TEST-MATERIAL",
        },
    )

    publish_dead_letter(
        message=test_message,
        error=ValueError("DLQ connection test"),
        attempts=3,
    )

    print("Dead-letter publisher test successful.")


if __name__ == "__main__":
    main()
