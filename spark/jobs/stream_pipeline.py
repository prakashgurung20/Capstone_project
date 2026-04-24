"""
Payments Streaming ETL — SOLID + DRY refactor
==============================================

SOLID principles applied
─────────────────────────
S  Single Responsibility  — each class / function has exactly one job.
O  Open/Closed            — add a new fraud rule by subclassing FraudRule,
                            zero changes to existing code.
L  Liskov Substitution    — every FraudRule subclass can replace another
                            transparently inside FraudDetector.
I  Interface Segregation  — FraudRule exposes only .apply(); writers expose
                            only .start(); nothing forces unneeded contracts.
D  Dependency Inversion   — high-level orchestration (main) depends on
                            abstractions (FraudRule, StreamWriter), not on
                            concrete detection logic or sink details.

DRY wins
─────────────────────────
• validate_and_flag called once per batch (was duplicated across two writers).
• Timestamp parsing lives in one helper (_parse_timestamp).
• Window-join pattern extracted to _flag_by_window_agg.
• Kafka + Parquet sinks share the same _start_stream wrapper.
"""

from __future__ import annotations

import argparse
import logging
import operator
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from functools import reduce
from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("PaymentsETL")

# ─────────────────────────────────────────────────────────────────────────────
# Configuration  (single source of truth — O/C: extend, never modify)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class ETLConfig:
    kafka_bootstrap: str       = "kafka:29092"
    bronze_path: str           = "/data/bronze/payments"
    checkpoint_path: str       = "/data/checkpoints"
    amount_threshold: float    = 10_000.00
    velocity_max_txn: int      = 5
    blacklisted_merchants: frozenset = field(
        default_factory=lambda: frozenset({"M11111", "M22222"})
    )
    valid_currencies: tuple    = ("USD", "EUR", "GBP", "NPR")
    ts_format: str             = "yyyy-MM-dd HH:mm:ss"
    trigger_interval: str      = "10 seconds"
    shuffle_partitions: str    = "4"

PAYMENT_SCHEMA = T.StructType([
    T.StructField("transaction_id", T.StringType()),
    T.StructField("ts_event",       T.StringType()),
    T.StructField("card_hash",      T.StringType()),
    T.StructField("merchant_id",    T.StringType()),
    T.StructField("amount",         T.DoubleType()),
    T.StructField("currency",       T.StringType()),
    T.StructField("mcc",            T.StringType()),
    T.StructField("channel",        T.StringType()),
    T.StructField("auth_result",    T.StringType()),
    T.StructField("location",       T.StringType()),
])

# Helper functions
def _parse_timestamp(df: DataFrame, ts_format: str) -> DataFrame:
    """Add `event_time` column; caller decides what to do with null rows."""
    return df.withColumn(
        "event_time",
        F.to_timestamp("ts_event", ts_format),
    )

def _extract_country(df: DataFrame) -> DataFrame:
    """Derive `country` from the last segment of the comma-separated location."""
    parts = F.split(F.col("location"), ",")
    return df.withColumn(
        "country",
        F.when(F.size(parts) > 1, F.trim(F.element_at(parts, -1)))
         .otherwise(F.lit("UNKNOWN")),
    )

def _add_partition_columns(df: DataFrame) -> DataFrame:
    """Add year / month / day columns derived from event_time."""
    return (
        df
        .withColumn("year",  F.year("event_time"))
        .withColumn("month", F.month("event_time"))
        .withColumn("day",   F.dayofmonth("event_time"))
    )

def _flag_by_window_agg(
    df: DataFrame,
    window_duration: str,
    agg_expr,
    filter_expr,
    flag_col: str,
    alias_prefix: str,
) -> DataFrame:
    """
    DRY helper — groups by (window, card_hash), applies agg_expr + filter_expr,
    left-joins the result back onto df, and adds a boolean flag_col.

    Parameters
    ----------
    window_duration : e.g. "1 minute"
    agg_expr        : Column expression passed to .agg()
    filter_expr     : Column expression passed to .filter() after aggregation
    flag_col        : Name of the boolean column to add to df
    alias_prefix    : Short prefix for intermediate join columns (e.g. "v", "cb")
    """
    card_alias  = f"{alias_prefix}_card"
    start_alias = f"{alias_prefix}_start"
    end_alias   = f"{alias_prefix}_end"

    agg_df = (
        df.groupBy(F.window("event_time", window_duration), "card_hash")
          .agg(agg_expr)
          .filter(filter_expr)
          .select(
              F.col("card_hash").alias(card_alias),
              F.col("window.start").alias(start_alias),
              F.col("window.end").alias(end_alias),
          )
    )

    return (
        df.join(
            agg_df,
            (df.card_hash  == agg_df[card_alias]) &
            (df.event_time >= agg_df[start_alias]) &
            (df.event_time <  agg_df[end_alias]),
            how="left",
        )
        .withColumn(flag_col, F.col(card_alias).isNotNull())
        .drop(card_alias, start_alias, end_alias)
    )

# ─────────────────────────────────────────────────────────────────────────────
# Fraud rules  (O/C + L + I)
# ─────────────────────────────────────────────────────────────────────────────

class FraudRule(ABC):
    """
    Abstract base for every fraud-detection rule.

    Contract
    --------
    • .apply(df) → df  enriched with a single boolean column named `self.flag`.
    • The column name is the rule's identity — used to compose fraud_reasons
      and is_fraud without knowing which concrete rules are active.
    """

    @property
    @abstractmethod
    def flag(self) -> str:
        """Name of the boolean flag column this rule adds."""

    @property
    @abstractmethod
    def reason(self) -> str:
        """Human-readable label used in fraud_reasons."""

    @abstractmethod
    def apply(self, df: DataFrame) -> DataFrame:
        """Enrich df with self.flag column and return it."""


class HighAmountRule(FraudRule):
    """Flag transactions whose amount exceeds a threshold."""

    flag   = "is_high_amount"
    reason = "HIGH_AMOUNT"

    def __init__(self, threshold: float):
        self._threshold = threshold

    def apply(self, df: DataFrame) -> DataFrame:
        return df.withColumn(self.flag, F.col("amount") > self._threshold)


class BlacklistedMerchantRule(FraudRule):
    """Flag transactions from known bad merchants."""

    flag   = "is_blacklisted"
    reason = "BLACKLISTED_MERCHANT"

    def __init__(self, merchants: frozenset):
        self._merchants = list(merchants)   # isin() needs a list

    def apply(self, df: DataFrame) -> DataFrame:
        return df.withColumn(self.flag, F.col("merchant_id").isin(self._merchants))


class BankDeclinedRule(FraudRule):
    """Flag transactions that were declined by the issuing bank."""

    flag   = "is_bank_declined"
    reason = "BANK_DECLINED"

    def apply(self, df: DataFrame) -> DataFrame:
        return df.withColumn(self.flag, F.col("auth_result") == "DECLINED")


class HighVelocityRule(FraudRule):
    """Flag cards with more than `max_txn` transactions in a 1-minute window."""

    flag   = "is_high_velocity"
    reason = "VELOCITY"

    def __init__(self, max_txn: int):
        self._max_txn = max_txn

    def apply(self, df: DataFrame) -> DataFrame:
        return _flag_by_window_agg(
            df,
            window_duration="1 minute",
            agg_expr=F.count("*").alias("velocity_count"),
            filter_expr=F.col("velocity_count") > self._max_txn,
            flag_col=self.flag,
            alias_prefix="v",
        )


class CrossBorderRule(FraudRule):
    """Flag cards used in more than one country within a 10-minute window."""

    flag   = "is_cross_border"
    reason = "CROSS_BORDER"

    def apply(self, df: DataFrame) -> DataFrame:
        return _flag_by_window_agg(
            df,
            window_duration="10 minutes",
            agg_expr=F.countDistinct("country").alias("country_count"),
            filter_expr=F.col("country_count") > 1,
            flag_col=self.flag,
            alias_prefix="cb",
        )


class HighDeclineRateRule(FraudRule):
    """Flag cards whose decline rate exceeds 50 % in a 10-minute window."""

    flag   = "is_high_decline_rate"
    reason = "HIGH_DECLINE_RATE"

    def apply(self, df: DataFrame) -> DataFrame:
        agg_expr = (
            F.sum(F.when(F.col("auth_result") == "DECLINED", 1).otherwise(0))
             .alias("declined_txn")
        )
        # We need total_txn as well; build a composite aggregation via struct.
        # Workaround: compute decline_rate in a second withColumn after the join.
        agg_df = (
            df.groupBy(F.window("event_time", "10 minutes"), "card_hash")
              .agg(
                  F.count("*").alias("total_txn"),
                  F.sum(F.when(F.col("auth_result") == "DECLINED", 1).otherwise(0))
                   .alias("declined_txn"),
              )
              .withColumn("decline_rate", F.col("declined_txn") / F.col("total_txn"))
              .filter(F.col("decline_rate") > 0.5)
              .select(
                  F.col("card_hash").alias("dr_card"),
                  F.col("window.start").alias("dr_start"),
                  F.col("window.end").alias("dr_end"),
              )
        )
        return (
            df.join(
                agg_df,
                (df.card_hash  == agg_df.dr_card) &
                (df.event_time >= agg_df.dr_start) &
                (df.event_time <  agg_df.dr_end),
                how="left",
            )
            .withColumn(self.flag, F.col("dr_card").isNotNull())
            .drop("dr_card", "dr_start", "dr_end")
        )

# ─────────────────────────────────────────────────────────────────────────────
# FraudDetector  (D — depends on FraudRule abstraction)
# ─────────────────────────────────────────────────────────────────────────────

class FraudDetector:
    """
    Applies an ordered list of FraudRule objects and synthesises:
      • one boolean flag per rule
      • `fraud_reasons` — comma-joined human-readable reasons
      • `is_fraud`      — master boolean OR of all rule flags

    To add a new rule: instantiate it and pass it in the `rules` list.
    This class never changes.
    """

    def __init__(self, rules: List[FraudRule]):
        self._rules = rules

    def apply(self, df: DataFrame) -> DataFrame:
        # Enrich with country (needed by CrossBorderRule)
        df = _extract_country(df)

        # Apply every rule in order
        for rule in self._rules:
            df = rule.apply(df)

        # Composite columns derived from rule flags
        flag_cols = [r.flag for r in self._rules]

        df = df.withColumn(
            "fraud_reasons",
            F.concat_ws(
                ", ",
                *[F.when(F.col(flag), F.lit(rule.reason))
                  for flag, rule in zip(flag_cols, self._rules)],
            ),
        )

        # Use reduce with | to exactly match original OR chain null semantics.
        # F.greatest() ignores nulls — the original | operator propagates them.
        df = df.withColumn(
            "is_fraud",
            reduce(operator.or_, [F.col(f) for f in flag_cols]),
        )

        # Drop intermediate columns — matches original validate_and_flag() which
        # dropped "country" + all individual flag cols before returning.
        df = df.drop("country", *flag_cols)

        return df

# ─────────────────────────────────────────────────────────────────────────────
# Batch processor  (S — owns the per-batch orchestration logic only)
# ─────────────────────────────────────────────────────────────────────────────

class BatchProcessor:
    """
    Transforms one micro-batch:
      1. Parse timestamp.
      2. Dead-letter unparseable rows to disk.
      3. Run fraud detection.
      4. Return (valid_df, fraud_df) for callers to sink however they need.
    """

    def __init__(self, config: ETLConfig, detector: FraudDetector):
        self._config   = config
        self._detector = detector
        self._dl_base  = os.path.join(
            os.path.dirname(config.bronze_path), "dead_letter"
        )

    def process(self, batch_df: DataFrame, batch_id: int):
        count = batch_df.count()
        log.info(f"[Batch {batch_id}] received {count} rows")

        if count == 0:
            return None, None

        # 1 — timestamp parsing (DRY: one call, shared result)
        with_ts = _parse_timestamp(batch_df, self._config.ts_format)

        # 2 — dead-letter unparseable rows
        self._dead_letter_bad_rows(with_ts, batch_id)

        # 3 — keep only parseable rows
        clean_df = with_ts.filter(F.col("event_time").isNotNull())
        if clean_df.count() == 0:
            log.info(f"[Batch {batch_id}] no valid rows after timestamp filter")
            return None, None

        # 4 — add partition columns + fraud flags
        enriched = _add_partition_columns(clean_df)
        enriched = self._add_currency_flag(enriched)
        enriched = self._detector.apply(enriched)

        # 5 — split into valid / fraud+invalid
        valid_df = enriched.filter(~F.col("is_fraud") & ~F.col("is_invalid"))
        fraud_df = enriched.filter( F.col("is_fraud") |  F.col("is_invalid"))

        log.info(
            f"[Batch {batch_id}] valid={valid_df.count()}  "
            f"fraud/invalid={fraud_df.count()}"
        )
        return valid_df, fraud_df

    # ── private helpers ───────────────────────────────────────────────────────

    def _dead_letter_bad_rows(self, df: DataFrame, batch_id: int) -> None:
        bad_df = df.filter(F.col("event_time").isNull())
        bad_count = bad_df.count()
        if bad_count == 0:
            return
        dl_path = f"{self._dl_base}/batch_{batch_id}"
        (
            bad_df
            .withColumn("batch_id", F.lit(batch_id))
            .write.mode("append").json(dl_path)
        )
        log.warning(
            f"[Batch {batch_id}] dead-lettered {bad_count} unparseable rows → {dl_path}"
        )

    def _add_currency_flag(self, df: DataFrame) -> DataFrame:
        return df.withColumn(
            "is_invalid",
            ~F.col("currency").isin(list(self._config.valid_currencies)),
        )

# ─────────────────────────────────────────────────────────────────────────────
# Stream writers  (S — each class writes to exactly one sink)
# ─────────────────────────────────────────────────────────────────────────────

class BronzeWriter:
    """Writes valid rows to partitioned Parquet (the bronze layer)."""

    def __init__(self, config: ETLConfig, processor: BatchProcessor):
        self._config    = config
        self._processor = processor
        os.makedirs(config.bronze_path, exist_ok=True)
        log.info(f"Bronze path: {config.bronze_path}")

    def start(self, raw_df: DataFrame):
        def _foreach_batch(batch_df: DataFrame, batch_id: int):
            valid_df, _ = self._processor.process(batch_df, batch_id)
            if valid_df is None or valid_df.count() == 0:
                log.info(f"[Bronze] batch {batch_id} — nothing to write")
                return

            log.info(f"[Bronze] batch {batch_id} — partition distribution:")
            valid_df.groupBy("year", "month", "day").count().show()

            (
                valid_df
                .drop("is_fraud", "is_invalid", "fraud_reasons")
                .write
                .partitionBy("year", "month", "day")
                .mode("append")
                .parquet(self._config.bronze_path)
            )
            log.info(
                f"[Bronze] batch {batch_id} — wrote {valid_df.count()} rows"
            )

        return _start_stream(
            raw_df, _foreach_batch,
            checkpoint=f"{self._config.checkpoint_path}/bronze",
            trigger=self._config.trigger_interval,
        )


class DeadLetterKafkaWriter:
    """Pushes fraud / invalid rows to the dead-letter Kafka topic."""

    TOPIC = "payments.deadletter"

    def __init__(self, config: ETLConfig, processor: BatchProcessor):
        self._config    = config
        self._processor = processor

    def start(self, raw_df: DataFrame):
        def _foreach_batch(batch_df: DataFrame, batch_id: int):
            _, fraud_df = self._processor.process(batch_df, batch_id)
            if fraud_df is None or fraud_df.count() == 0:
                return
            (
                fraud_df
                .selectExpr("card_hash as key", "to_json(struct(*)) as value")
                .write
                .format("kafka")
                .option("kafka.bootstrap.servers", self._config.kafka_bootstrap)
                .option("topic", self.TOPIC)
                .save()
            )
            log.info(
                f"[DeadLetter] batch {batch_id} — sent {fraud_df.count()} rows "
                f"→ {self.TOPIC}"
            )

        return _start_stream(
            raw_df, _foreach_batch,
            checkpoint=f"{self._config.checkpoint_path}/deadletter",
            trigger=self._config.trigger_interval,
        )


def _start_stream(raw_df: DataFrame, foreach_batch_fn, checkpoint: str, trigger: str):
    """DRY wrapper — the only place writeStream options are set."""
    return (
        raw_df.writeStream
        .foreachBatch(foreach_batch_fn)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=trigger)
        .start()
    )

# ─────────────────────────────────────────────────────────────────────────────
# Spark factory  (S)
# ─────────────────────────────────────────────────────────────────────────────

def build_spark(app_name: str, config: ETLConfig) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", config.shuffle_partitions)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

# ─────────────────────────────────────────────────────────────────────────────
# Kafka source reader  (S)
# ─────────────────────────────────────────────────────────────────────────────

def read_payments_raw(spark: SparkSession, kafka_bootstrap: str) -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", "payments.raw")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .selectExpr("CAST(value AS STRING) as json")
        .select(F.from_json(F.col("json"), PAYMENT_SCHEMA).alias("data"))
        .select("data.*")
    )

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Payments Streaming ETL")
    parser.add_argument("--kafka-bootstrap", default="kafka:29092")
    parser.add_argument("--bronze-path",     default="/data/bronze/payments")
    parser.add_argument("--checkpoint-path", default="/data/checkpoints")
    args = parser.parse_args()

    config = ETLConfig(
        kafka_bootstrap=args.kafka_bootstrap,
        bronze_path=args.bronze_path,
        checkpoint_path=args.checkpoint_path,
    )

    log.info(f"Config: kafka={config.kafka_bootstrap}  bronze={config.bronze_path}")

    spark  = build_spark("PaymentsStreamingETL", config)
    print("=============================================")
    print(spark)

    raw_df = read_payments_raw(spark, config.kafka_bootstrap)

    print("=============================================")

    print(raw_df)
    # Build the rule pipeline — add / remove rules here without touching anything else
    rules = [
        HighAmountRule(config.amount_threshold),
        BlacklistedMerchantRule(config.blacklisted_merchants),
        BankDeclinedRule(),
        HighVelocityRule(config.velocity_max_txn),
        CrossBorderRule(),
        HighDeclineRateRule(),
    ]
    detector  = FraudDetector(rules)
    processor = BatchProcessor(config, detector)

    q1 = BronzeWriter(config, processor).start(raw_df)
    log.info("Bronze write stream started")

    q2 = DeadLetterKafkaWriter(config, processor).start(raw_df)
    log.info("Dead-letter write stream started")

    log.info(f"Active queries: {[q.name for q in spark.streams.active]}")
    spark.streams.awaitAnyTermination()