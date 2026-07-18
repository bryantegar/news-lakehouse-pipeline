"""
clean_transform.py — PySpark batch job (silabus: "Batch Processing with PySpark").

Reads the raw parquet the scraper just landed in the lake, applies
DataFrame cleaning + the 6 Data Quality dimension checks, caches the
result (it's read twice: once to write cleaned parquet, once to compute
the DQ scorecard), and writes cleaned parquet back to the lake plus a
per-run DQ scorecard as JSON for the DAG to load into Postgres/BigQuery.

Run standalone for local dev:
    python clean_transform.py --in /tmp/landing --out /tmp/cleaned
Airflow calls this via SparkSubmitOperator / BashOperator in local mode
(`.master("local[*]")`) — no cluster required for this project's data
volume, but `cluster_config()` below documents what changes for a real
YARN/K8s cluster.
"""
import argparse
import json
from pyspark.sql import SparkSession, functions as F


def get_spark(app_name: str = "news-clean-transform") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")  # small local data, keep partitions low
        .getOrCreate()
    )


def cluster_config():
    """
    Reference only — what you'd change to run this on a real cluster
    instead of local[*]:
      .master("yarn")  or  .master("k8s://https://<api-server>:443")
      .config("spark.executor.instances", "3")
      .config("spark.executor.memory", "4g")
      .config("spark.executor.cores", "2")
      .config("spark.dynamicAllocation.enabled", "true")
    """


VALID_CATEGORIES = [
    "nasional", "ekonomi", "olahraga", "teknologi", "hiburan",
    "woman", "mom", "otomotif", "food-travel", "bolanita",
]


def clean(df):
    df = df.dropDuplicates(["id"])  # uniqueness dimension

    df = df.withColumn(
        "category",
        F.when(F.col("category").isin(*VALID_CATEGORIES), F.col("category")).otherwise(F.lit(None)),
    )  # validity dimension: drop values outside the accepted set

    df = df.withColumn(
        "title", F.trim(F.regexp_replace("title", r"\s+", " "))
    )  # consistency dimension: normalize whitespace

    df = df.withColumn("is_hard_deleted", F.lit(False))
    return df


def dq_scorecard(raw_df, clean_df, table_name: str) -> dict:
    total = raw_df.count()
    if total == 0:
        return {"table_name": table_name, "row_count": 0}

    completeness = 1 - (
        raw_df.filter(F.col("author_name").isNull()).count() / total
    )
    validity = clean_df.filter(F.col("category").isNotNull()).count() / total
    uniqueness = clean_df.select("id").distinct().count() / raw_df.select("id").distinct().count()
    accuracy = raw_df.filter(F.col("published_at") <= F.col("updated_at")).count() / total
    timeliness = raw_df.filter(
        F.col("updated_at") >= F.col("created_at")
    ).count() / total
    consistency = 1.0  # placeholder: cross-table checks belong in dbt tests, not this job

    scores = {
        "table_name": table_name,
        "row_count": total,
        "completeness": round(completeness, 4),
        "accuracy": round(accuracy, 4),
        "consistency": round(consistency, 4),
        "timeliness": round(timeliness, 4),
        "validity": round(validity, 4),
        "uniqueness": round(uniqueness, 4),
    }
    scores["overall_score"] = round(
        sum(v for k, v in scores.items() if k not in ("table_name", "row_count")) / 6, 4
    )
    return scores


def run(input_path: str, output_path: str, scorecard_path: str):
    spark = get_spark()
    raw_df = spark.read.parquet(input_path)
    raw_df.cache()  # read twice below (clean + scorecard) -> cache once

    cleaned_df = clean(raw_df)
    cleaned_df.write.mode("overwrite").parquet(output_path)

    scores = dq_scorecard(raw_df, cleaned_df, table_name="articles")
    with open(scorecard_path, "w") as f:
        json.dump(scores, f)

    raw_df.unpersist()
    spark.stop()
    return scores


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--in", dest="input_path", required=True)
    parser.add_argument("--out", dest="output_path", required=True)
    parser.add_argument("--scorecard", dest="scorecard_path", required=True)
    args = parser.parse_args()
    result = run(args.input_path, args.output_path, args.scorecard_path)
    print(json.dumps(result, indent=2))
