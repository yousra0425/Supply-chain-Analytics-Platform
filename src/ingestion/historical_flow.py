
from prefect import flow, task
from pathlib import Path

from src.ingestion.historical_export import export_historical_snapshot
from src.messaging.publisher import publish_file_event


@task(name="Check source file", retries=2, retry_delay_seconds=5)
def check_source_file() -> str:
    path = Path(
        "data/incoming/Blocked Stock 17.08.26 - recreated.xlsx"
    )

    if not path.is_file():
        raise FileNotFoundError(path)

    print(f"Source file found: {path.name}")

    return str(path)


@task(name="Validate and export snapshot", retries=1)
def prepare_snapshot() -> str:
    # Reuses our existing loading and validation functions.
    path = export_historical_snapshot()
    return str(path)


@task(name="Publish ingestion event", retries=2, retry_delay_seconds=5)
def notify_worker(snapshot_path: str):
    publish_file_event(Path(snapshot_path))


@flow(name="Historical Blocked Stock Ingestion", log_prints=True)
def historical_ingestion_flow():
    check_source_file()

    snapshot_path = prepare_snapshot()

    notify_worker(snapshot_path)

    print("Prefect ingestion workflow completed.")


if __name__ == "__main__":
    historical_ingestion_flow()
