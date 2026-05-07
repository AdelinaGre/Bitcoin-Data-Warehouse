from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    coalesce,
    col,
    count,
    current_timestamp,
    date_format,
    lit,
    pow,
    sqrt,
    sum as spark_sum,
    unix_timestamp,
    when,
)

MONGO_URI = "mongodb://admin:password@127.0.0.1:27017/datawarehouse?authSource=admin"
MIN_ROWS_PER_ASSET = 10


spark = (
    SparkSession.builder
    .appName("ACME DWH - Train Price Regression")
    .config("spark.mongodb.read.connection.uri", MONGO_URI)
    .config("spark.mongodb.write.connection.uri", MONGO_URI)
    .getOrCreate()
)

raw_df = (
    spark.read
    .format("mongodb")
    .option("database", "datawarehouse")
    .option("collection", "time_series_data")
    .load()
)

training_df = (
    raw_df
    .filter((col("deleted") != True) | col("deleted").isNull())
    .select(
        col("assetId"),
        col("dataSourceId"),
        date_format(col("businessDate"), "yyyy-MM-dd").alias("date"),
        (unix_timestamp(col("businessDate")).cast("double") / lit(86400.0)).alias("x"),
        coalesce(
            col("payload.close").cast("double"),
            col("payload.last").cast("double"),
            col("payload.mid").cast("double"),
            col("payload.open").cast("double"),
        ).alias("actualClose"),
    )
    .filter(col("assetId").isNotNull())
    .filter(col("dataSourceId").isNotNull())
    .filter(col("date").isNotNull())
    .filter(col("x").isNotNull())
    .filter(col("actualClose").isNotNull())
)

stats_df = (
    training_df
    .groupBy("assetId", "dataSourceId")
    .agg(
        count("*").alias("n"),
        spark_sum("x").alias("sumX"),
        spark_sum("actualClose").alias("sumY"),
        spark_sum(col("x") * col("actualClose")).alias("sumXY"),
        spark_sum(col("x") * col("x")).alias("sumX2"),
        avg("actualClose").alias("avgY"),
    )
    .filter(col("n") >= MIN_ROWS_PER_ASSET)
    .withColumn("denominator", col("n") * col("sumX2") - col("sumX") * col("sumX"))
    .filter(col("denominator") != 0)
    .withColumn(
        "slope",
        (col("n") * col("sumXY") - col("sumX") * col("sumY")) / col("denominator"),
    )
    .withColumn("intercept", (col("sumY") - col("slope") * col("sumX")) / col("n"))
    .select("assetId", "dataSourceId", "avgY", "slope", "intercept")
)

scored_df = (
    training_df
    .join(stats_df, ["assetId", "dataSourceId"], "inner")
    .withColumn("predictedClose", col("intercept") + col("slope") * col("x"))
    .withColumn("residual", col("actualClose") - col("predictedClose"))
)

metrics_df = (
    scored_df
    .groupBy("assetId", "dataSourceId")
    .agg(
        sqrt(avg(pow(col("residual"), 2))).alias("modelRMSE"),
        spark_sum(pow(col("residual"), 2)).alias("ssRes"),
        spark_sum(pow(col("actualClose") - col("avgY"), 2)).alias("ssTot"),
    )
    .withColumn(
        "modelR2",
        when(col("ssTot") == 0, lit(1.0)).otherwise(lit(1.0) - col("ssRes") / col("ssTot")),
    )
    .select("assetId", "dataSourceId", "modelRMSE", "modelR2")
)

output_df = (
    scored_df
    .join(metrics_df, ["assetId", "dataSourceId"], "inner")
    .select(
        col("assetId"),
        col("dataSourceId"),
        col("date"),
        col("actualClose"),
        col("predictedClose"),
        col("residual"),
        col("modelR2"),
        col("modelRMSE"),
    )
    .withColumn("computedAt", current_timestamp())
    .withColumn("jobName", lit("train_price_regression"))
)

output_df.show(50, truncate=False)

(
    output_df.write
    .format("mongodb")
    .mode("overwrite")
    .option("database", "datawarehouse")
    .option("collection", "analytics_price_predictions")
    .save()
)

spark.stop()
