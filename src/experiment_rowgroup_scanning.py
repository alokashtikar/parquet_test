from pathlib import Path
import pyarrow.parquet as pq

from utils import measure_peak_memory

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
PARQUET_PATH = DATA_DIR / "big.parquet"


def read_entire_file_at_once():
    with measure_peak_memory("Parquet read_table (all at once)"):
        pf = pq.ParquetFile(PARQUET_PATH)
        table = pf.read()
        _ = table.column("value1").to_pandas().mean()


def stream_in_batches():
    with measure_peak_memory("Parquet iterate batches"):
        pf = pq.ParquetFile(PARQUET_PATH)
        total = 0.0
        count = 0

        for batch in pf.iter_batches(batch_size=50_000, columns=["value1"]):
            col = batch.column("value1")
            total += col.to_pandas().sum()
            count += len(col)

        mean = total / count
        print(f"Mean value1 (streamed): {mean}")


def read_filtered_rows():
    import pyarrow.dataset as ds

    with measure_peak_memory("Parquet filtered scan (category == 'A')"):
        dataset = ds.dataset(PARQUET_PATH, format="parquet")
        table = dataset.to_table(
            filter=ds.field("category") == "A",
            columns=["id", "category", "value1"],
        )
        _ = table.num_rows
        print(f"Filtered rows: {table.num_rows}")


if __name__ == "__main__":
    read_entire_file_at_once()
    stream_in_batches()
    read_filtered_rows()
