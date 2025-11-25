# Parquet Memory Demo

Proof-of-concept showing that Parquet readers avoid loading entire files into memory, while CSV reads do. Includes column pruning, batch streaming, and filtered scans.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate  # or .venv\\Scripts\\activate on Windows
pip install pandas pyarrow psutil numpy
```

## Generate data

```bash
python src/generate_data.py
```

By default this creates ~10M rows in `data/big.csv` and `data/big.parquet` with 100k-row groups. Adjust `N_ROWS` in the script if needed for your machine.

## Experiments (run each in a fresh process)

```bash
python src/experiment_csv_vs_parquet.py          # baseline CSV vs full Parquet load
python src/experiment_column_pruning.py         # column pruning
python src/experiment_rowgroup_scanning.py      # batch streaming + filtered scan
```

Each script prints start/end/peak memory. Watch with `top`/`htop` if you want.

## What this demonstrates

- CSV full read materializes everything; memory scales with full uncompressed dataframe size.
- Parquet column pruning only reads requested columns (`id`, `value1`), so peak memory drops.
- Batch streaming (`iter_batches`) keeps only one batch in memory while still processing all rows.
- Filtered scans push predicates down to Parquet and skip row groups that cannot match (`category == 'A'`), reducing both IO and memory.
