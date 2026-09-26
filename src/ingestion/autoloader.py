"""Generic Auto Loader ingestion: CSV files in the landing volume -> bronze Delta tables.

Design principles:
- Config-driven: tables are defined in config.yml, no code change needed to add a table.
- Incremental: Auto Loader checkpoints track processed files, each file is ingested once.
- Raw fidelity: all columns are kept as strings; typing happens in the silver layer (dbt).
- Auditable: every row carries ingestion metadata (_ingested_at, _source_file, _batch_id).

Typical usage (from a notebook):
    cfg = load_config("olist_dev")
    for table in cfg["tables"]:
        ingest_table(spark, table, cfg, "olist_dev", batch_id)
"""
from __future__ import annotations

from pathlib import Path

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# config.yml lives next to this module
CONFIG_PATH = Path(__file__).parent / "config.yml"


def load_config(catalog: str, path: Path = CONFIG_PATH) -> dict:
    """Load the ingestion config and resolve the environment.

    The YAML file contains a {catalog} placeholder in all paths, so one config
    serves every environment (olist_dev, olist_prod).

    Args:
        catalog: Target Unity Catalog name, e.g. "olist_dev".
        path: Path to the YAML config. Defaults to config.yml next to this module.

    Returns:
        Config as a dictionary with resolved paths, e.g.
        {"landing_path": "/Volumes/olist_dev/landing/raw/olist", "tables": [...], ...}
    """
    raw = path.read_text().replace("{catalog}", catalog)
    return yaml.safe_load(raw)


def add_metadata(df: DataFrame, batch_id: str) -> DataFrame:
    """Append audit columns to every ingested row.

    These columns make bronze data traceable: when a row arrived, from which
    file, and in which pipeline run.

    Args:
        df: Streaming DataFrame read by Auto Loader.
        batch_id: Identifier of the current run (job run id or manual id).

    Returns:
        The same DataFrame with columns:
        _ingested_at, _source_file, _source_file_modified_at, _batch_id.
    """
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        # _metadata is a hidden column provided by Spark for file-based sources
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_source_file_modified_at", F.col("_metadata.file_modification_time"))
        .withColumn("_batch_id", F.lit(batch_id))
    )


def ingest_table(spark: SparkSession, table: str, cfg: dict, catalog: str, batch_id: str) -> dict:
    """Incrementally ingest new CSV files of one table into its bronze Delta table.

    Only files that have not been processed before are read (tracked by the
    checkpoint). Running the function twice without new files adds 0 rows,
    which makes the ingestion idempotent.

    Args:
        spark: Active SparkSession.
        table: Table name from config, e.g. "orders" (= folder name in landing).
        cfg: Config returned by load_config().
        catalog: Target Unity Catalog name, e.g. "olist_dev".
        batch_id: Identifier of the current run, stored in _batch_id.

    Returns:
        Summary of the run, e.g. {"table": "olist_dev.bronze.olist_orders", "new_rows": 99441}.
    """
    # Resolve paths and target table name for this table
    source_path = f"{cfg['landing_path']}/{table}"
    checkpoint = f"{cfg['checkpoint_path']}/{table}"
    target = f"{catalog}.{cfg['target_schema']}.{cfg['table_prefix']}{table}"

    # 1) Read: Auto Loader (cloudFiles) discovers new files in the folder
    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{checkpoint}/_schema")  # inferred schema is stored here
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")     # new source columns do not break the job
        .options(**cfg["csv_options"])
        .load(source_path)
    )

    # 2) Write: append new rows with audit columns to the bronze Delta table
    query = (
        add_metadata(stream, batch_id)
        .writeStream.option("checkpointLocation", checkpoint)  # remembers processed files
        .option("mergeSchema", "true")                         # allow new columns in target table
        .trigger(availableNow=True)                            # process all new files, then stop
        .toTable(target)
    )
    query.awaitTermination()

    # 3) Report: count rows written by this run
    new_rows = spark.table(target).where(F.col("_batch_id") == batch_id).count()
    return {"table": target, "new_rows": new_rows}