from setuptools import setup

setup(
    name="parquet-memory-demo",
    version="0.1.0",
    description="PoC scripts demonstrating Parquet memory behavior vs CSV",
    author="",
    python_requires=">=3.10",
    package_dir={"": "src"},
    py_modules=[
        "utils",
        "generate_data",
        "experiment_csv_vs_parquet",
        "experiment_column_pruning",
        "experiment_rowgroup_scanning",
    ],
    install_requires=[
        "pandas",
        "pyarrow",
        "psutil",
        "numpy",
        "s3fs",
    ],
)
