import os
import psutil
from contextlib import contextmanager
import time
from pathlib import Path


def data_path(filename: str):
    """
    Resolve a data file path, supporting local data/ directory or an S3 prefix
    via DATA_PREFIX env var (e.g., s3://my-bucket/path).
    """
    prefix = os.getenv("DATA_PREFIX")
    if prefix:
        return f"{prefix.rstrip('/')}/{filename}"
    return Path(__file__).resolve().parents[1] / "data" / filename


def current_memory_mb() -> float:
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)


@contextmanager
def measure_peak_memory(label: str = ""):
    """
    Context manager that samples memory usage and prints peak.
    Simple but good enough for demo purposes.
    """
    process = psutil.Process(os.getpid())
    peak = current_memory_mb()
    start = peak
    print(f"[{label}] Start memory: {start:.1f} MB")

    try:
        yield
    finally:
        for _ in range(5):
            mem = current_memory_mb()
            peak = max(peak, mem)
            time.sleep(0.1)

        end = current_memory_mb()
        delta = peak - start
        print(f"[{label}] End memory:   {end:.1f} MB")
        print(f"[{label}] Peak memory:  {peak:.1f} MB (+{delta:.1f} MB)\n")
