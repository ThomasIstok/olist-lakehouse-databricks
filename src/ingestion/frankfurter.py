"""Incremental ingestion of daily FX rates from the Frankfurter API into bronze.

Flow: watermark -> date window -> API call -> flatten JSON -> MERGE into Delta table.

Design decisions:
- Watermark: only dates after the latest date already in bronze are requested.
- Long format (one row = date x currency pair): a new currency is a config change, not a schema change.
- MERGE on (rate_date, base_currency, quote_currency): re-running or overlapping windows never
  create duplicates; a revised rate updates the existing row.
- rate_date is kept as ISO string (raw fidelity); typing to DATE happens in silver (dbt).
"""
from __future__ import annotations

import logging
from datetime import date, timedelta

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.ingestion.api_client import get_json

logger = logging.getLogger(__name__)

# Business key of one row; used for MERGE matching
MERGE_KEYS = ["rate_date", "base_currency", "quote_currency"]


def create_table_if_missing(spark: SparkSession, target: str) -> None:
    """Create the bronze table with an explicit schema on the first run."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {target} (
            rate_date       STRING    COMMENT 'ECB business day (ISO yyyy-MM-dd)',
            base_currency   STRING    COMMENT 'Currency being converted, e.g. BRL',
            quote_currency  STRING    COMMENT 'Target currency, e.g. EUR',
            rate            DOUBLE    COMMENT '1 base_currency = rate x quote_currency',
            _ingested_at    TIMESTAMP COMMENT 'When the row was written or last updated',
            _source         STRING    COMMENT 'API request URL',
            _batch_id       STRING    COMMENT 'Pipeline run identifier'
        )
        COMMENT 'Daily FX rates from Frankfurter API (ECB), long format'
    """)


def get_watermark(spark: SparkSession, target: str) -> date | None:
    """Return the latest rate_date already in bronze, or None if the table is empty."""
    latest = spark.table(target).agg(F.max("rate_date")).first()[0]
    return date.fromisoformat(latest) if latest else None


def resolve_window(fx_cfg: dict, watermark: date | None) -> tuple[date, date] | None:
    """Compute the date window to request, or None if bronze is already up to date.

    Start = day after watermark (incremental) or configured start_date (first run).
    End   = configured end_date, or today for live runs.
    """
    start = watermark + timedelta(days=1) if watermark else date.fromisoformat(fx_cfg["start_date"])
    end = date.today() if fx_cfg["end_date"] == "today" else date.fromisoformat(fx_cfg["end_date"])
    return (start, end) if start <= end else None


def flatten_rates(payload: dict) -> list[tuple[str, str, str, float]]:
    """Turn {"rates": {"2017-01-02": {"EUR": 0.29, ...}}} into long rows (date, base, quote, rate)."""
    base = payload["base"]
    return [
        (rate_date, base, quote, float(rate))
        for rate_date, quotes in payload["rates"].items()
        for quote, rate in quotes.items()
    ]


def merge_into_bronze(spark: SparkSession, df: DataFrame, target: str) -> dict:
    """Upsert rows into bronze and return how many rows were inserted and updated."""
    condition = " AND ".join(f"t.{k} = s.{k}" for k in MERGE_KEYS)
    (
        DeltaTable.forName(spark, target).alias("t")
        .merge(df.alias("s"), condition)
        .whenMatchedUpdateAll(condition="t.rate <> s.rate")  # update only if the rate was revised
        .whenNotMatchedInsertAll()
        .execute()
    )
    # Delta stores row counts of the last operation in the table history
    metrics = DeltaTable.forName(spark, target).history(1).select("operationMetrics").first()[0]
    return {
        "inserted": int(metrics.get("numTargetRowsInserted", 0)),
        "updated": int(metrics.get("numTargetRowsUpdated", 0)),
    }


def ingest_fx(spark: SparkSession, cfg: dict, catalog: str, batch_id: str) -> dict:
    """Run one incremental FX ingestion and return a run summary."""
    fx = cfg["fx"]
    target = f"{catalog}.{cfg['target_schema']}.{fx['target_table']}"
    create_table_if_missing(spark, target)

    # 1) Which dates do we still need?
    watermark = get_watermark(spark, target)
    window = resolve_window(fx, watermark)
    if window is None:
        logger.info("%s is up to date (watermark %s)", target, watermark)
        return {"table": target, "watermark": str(watermark), "window": None, "fetched": 0, "inserted": 0, "updated": 0}
    start, end = window

    # 2) Call the API for the whole window in one request
    url = f"{fx['base_url']}/{start}..{end}"
    payload = get_json(url, params={"from": fx["base_currency"], "to": ",".join(fx["quote_currencies"])})

    # 3) Flatten JSON to long rows and add audit columns
    rows = flatten_rates(payload)
    if not rows:
        logger.warning("API returned no rates for %s..%s (weekend or holiday only?)", start, end)
        return {"table": target, "watermark": str(watermark), "window": f"{start}..{end}", "fetched": 0, "inserted": 0, "updated": 0}

    df = (
        spark.createDataFrame(rows, "rate_date STRING, base_currency STRING, quote_currency STRING, rate DOUBLE")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source", F.lit(url))
        .withColumn("_batch_id", F.lit(batch_id))
    )

    # 4) Idempotent write
    counts = merge_into_bronze(spark, df, target)
    logger.info("Merged %d rows into %s: %s", len(rows), target, counts)
    return {"table": target, "watermark": str(watermark), "window": f"{start}..{end}", "fetched": len(rows), **counts}