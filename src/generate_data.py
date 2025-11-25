import numpy as np
import pandas as pd
from pathlib import Path

from utils import data_path

N_ROWS = 1_000_000  # adjust based on your machine
ROW_GROUP_SIZE = 100_000


def main():
    csv_path = data_path("big.csv")
    parquet_path = data_path("big.parquet")

    if isinstance(csv_path, Path):
        csv_path.parent.mkdir(exist_ok=True)

    print("Generating synthetic data...")
    df = pd.DataFrame(
        {
            "id": np.arange(N_ROWS, dtype="int64"),
            "category": np.random.choice(["A", "B", "C", "D"], size=N_ROWS),
            "value1": np.random.randn(N_ROWS).astype("float32"),
            "value2": np.random.randn(N_ROWS).astype("float32"),
            "big_text": np.random.choice(
                [
                    "lorem ipsum dolor sit amet",
                    "the quick brown fox jumps over the lazy dog",
                    "some other fairly long-ish text fragment",
                ],
                size=N_ROWS,
            ),
        }
    )

    print(f"Saving CSV to {csv_path}...")
    df.to_csv(csv_path, index=False)

    print(f"Saving Parquet to {parquet_path} with row groups...")
    df.to_parquet(
        parquet_path,
        engine="pyarrow",
        row_group_size=ROW_GROUP_SIZE,
        compression="snappy",
    )

    print("Done.")


if __name__ == "__main__":
    main()
