"""Capture every inventory Kafka record in the append-only Bronze ledger."""
try:
    from .raw_bronze import run_capture
except ImportError:  # Flink packages the jobs directory flat via -pyfs.
    from raw_bronze import run_capture


def run() -> None:
    run_capture("inventory")


if __name__ == "__main__":
    run()
