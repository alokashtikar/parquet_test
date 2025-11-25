import pandas as pd

from utils import measure_peak_memory, data_path


def load_csv_full():
    csv_path = data_path("big.csv")
    with measure_peak_memory("CSV full load"):
        df = pd.read_csv(csv_path)
        _ = df["value1"].mean()


def load_parquet_full():
    parquet_path = data_path("big.parquet")
    with measure_peak_memory("Parquet full load"):
        df = pd.read_parquet(parquet_path, engine="pyarrow")
        print("Top 5 rows from Parquet:")
        print(df.head(5))
        _ = df["value1"].mean()


if __name__ == "__main__":
    load_csv_full()
    load_parquet_full()
