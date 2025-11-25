import pandas as pd
from pathlib import Path

from utils import measure_peak_memory

DATA_DIR = Path(__file__).resolve().parents[1] / "data"


def load_csv_full():
    csv_path = DATA_DIR / "big.csv"
    with measure_peak_memory("CSV full load"):
        df = pd.read_csv(csv_path)
        _ = df["value1"].mean()


def load_parquet_full():
    parquet_path = DATA_DIR / "big.parquet"
    with measure_peak_memory("Parquet full load"):
        df = pd.read_parquet(parquet_path, engine="pyarrow")
        _ = df["value1"].mean()


if __name__ == "__main__":
    load_csv_full()
    load_parquet_full()
