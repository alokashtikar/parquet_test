import pandas as pd

from utils import measure_peak_memory, data_path

PARQUET_PATH = data_path("big.parquet")


def load_all_columns():
    with measure_peak_memory("Parquet all columns"):
        df = pd.read_parquet(PARQUET_PATH, engine="pyarrow")
        _ = df["value1"].mean()


def load_few_columns():
    with measure_peak_memory("Parquet subset of columns"):
        df = pd.read_parquet(
            PARQUET_PATH,
            engine="pyarrow",
            columns=["id", "value1"],
        )
        _ = df["value1"].mean()


if __name__ == "__main__":
    load_all_columns()
    load_few_columns()
