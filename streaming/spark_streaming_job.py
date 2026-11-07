import os
from datetime import timedelta

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
CHECKPOINT = "/tmp/checkpoint_streaming"

spark = (
    SparkSession.builder
    .appName("streaming_transaksi")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# amount sama timestamp dibaca string dulu, nanti di cast
# soalnya kalau langsung double, "seratus ribu" jadi null dan ga ketauan itu salah tipe atau emang kosong
schema = StructType([
    StructField("user_id", StringType()),
    StructField("amount", StringType()),
    StructField("timestamp", StringType()),
    StructField("source", StringType()),
])

df_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("subscribe", "transactions")
    .option("startingOffsets", "latest")
    .load()
)

df = (
    df_kafka
    .selectExpr("CAST(value AS STRING) AS json_str", "offset")
    .withColumn("data", F.from_json("json_str", schema))
    .select("json_str", "offset", "data.*")
    .withColumn("amount_double", F.col("amount").cast("double"))
    .withColumn("event_time", F.to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ssX"))
)

# variabel buat nyimpen data antar batch
running_total = 0
max_event_time = None
key_sebelumnya = set()


def proses_batch(batch_df, batch_id):
    global running_total, max_event_time, key_sebelumnya

    if batch_df.count() == 0:
        return

    # watermark = event time paling besar - 3 menit
    watermark = None
    if max_event_time is not None:
        watermark = max_event_time - timedelta(minutes=3)

    # cek duplikat di batch yang sama
    w = Window.partitionBy("user_id", "timestamp").orderBy("offset")
    batch_df = batch_df.withColumn("urutan", F.row_number().over(w))

    # cek duplikat sama batch sebelumnya
    gabungan_key = F.concat_ws("|", "user_id", "timestamp")
    sudah_ada = gabungan_key.isin(list(key_sebelumnya)) if key_sebelumnya else F.lit(False)
    duplikat = F.col("user_id").isNotNull() & ((F.col("urutan") > 1) | sudah_ada)

    if watermark is not None:
        telat = F.col("event_time") < F.lit(watermark)
    else:
        telat = F.lit(False)

    # validasi, kalau error lebih dari satu digabung pake ;
    batch_df = batch_df.withColumn(
        "error_reason",
        F.concat_ws(
            "; ",
            # 1. mandatory field
            F.when(F.col("user_id").isNull(), "user_id kosong"),
            F.when(F.col("amount").isNull(), "amount kosong"),
            F.when(F.col("timestamp").isNull(), "timestamp kosong"),
            # 2. tipe data
            F.when(F.col("amount").isNotNull() & F.col("amount_double").isNull(), "amount bukan angka"),
            F.when(F.col("timestamp").isNotNull() & F.col("event_time").isNull(), "format timestamp salah"),
            # 3. range amount
            F.when((F.col("amount_double") < 1) | (F.col("amount_double") > 10000000), "amount di luar range"),
            # 4. source
            F.when(~F.col("source").isin("mobile", "web", "pos") | F.col("source").isNull(), "source tidak dikenal"),
            # 5. duplikat
            F.when(duplikat, "duplicate event"),
            # late event
            F.when(telat, "late event (lebih dari 3 menit)"),
        ),
    )
    batch_df = batch_df.withColumn(
        "error_reason", F.when(F.col("error_reason") == "", None).otherwise(F.col("error_reason"))
    )
    batch_df = batch_df.withColumn("is_valid", F.col("error_reason").isNull())
    batch_df.persist()

    data_valid = batch_df.filter("is_valid = true")
    data_invalid = batch_df.filter("is_valid = false")

    # kirim yang valid ke transactions_valid
    (
        data_valid
        .select(
            F.col("user_id").alias("key"),
            F.to_json(F.struct(
                "user_id",
                F.col("amount_double").cast("long").alias("amount"),
                "timestamp",
                "source",
                "is_valid",
            )).alias("value"),
        )
        .write.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVER)
        .option("topic", "transactions_valid")
        .save()
    )

    # kirim yang invalid ke dlq
    (
        data_invalid
        .select(
            F.col("user_id").alias("key"),
            F.to_json(F.struct("user_id", "amount", "timestamp", "source", "is_valid", "error_reason")).alias("value"),
        )
        .write.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVER)
        .option("topic", "transactions_dlq")
        .save()
    )

    jml_valid = data_valid.count()
    jml_invalid = data_invalid.count()
    running_total += jml_valid

    # update max event time buat watermark batch berikutnya
    max_batch = batch_df.agg(F.max("event_time")).collect()[0][0]
    if max_batch is not None and (max_event_time is None or max_batch > max_event_time):
        max_event_time = max_batch

    # simpen key buat cek duplikat nanti
    for row in batch_df.filter("user_id is not null and timestamp is not null").select("user_id", "timestamp").collect():
        key_sebelumnya.add(row["user_id"] + "|" + row["timestamp"])

    print("\n===== batch", batch_id, "=====")
    hasil = spark.createDataFrame(
        [(running_total, jml_valid, jml_invalid)],
        "running_total long, valid long, invalid long",
    )
    hasil.select(F.current_timestamp().alias("timestamp"), "running_total", "valid", "invalid").show(truncate=False)

    if jml_invalid > 0:
        print("data masuk DLQ:")
        data_invalid.select("user_id", "amount", "timestamp", "source", "is_valid", "error_reason").show(truncate=False)

    batch_df.unpersist()


query1 = (
    df.withWatermark("event_time", "3 minutes")
    .writeStream
    .foreachBatch(proses_batch)
    .option("checkpointLocation", CHECKPOINT + "/validasi")
    .trigger(processingTime="5 seconds")
    .start()
)

# tumbling window 1 menit buat monitoring
# event yang telat lebih dari watermark otomatis di drop sama spark
df_window = (
    df.filter("event_time is not null")
    .withWatermark("event_time", "3 minutes")
    .groupBy(F.window("event_time", "1 minute"))
    .agg(F.count("*").alias("total_transaksi"))
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "total_transaksi",
    )
)

query2 = (
    df_window.writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation", CHECKPOINT + "/window")
    .trigger(processingTime="10 seconds")
    .start()
)

print("streaming jalan...")

# cek berapa late event yang di drop sama watermark
while query1.isActive and query2.isActive:
    spark.streams.awaitAnyTermination(15)
    progress = query2.lastProgress
    if progress and progress.get("stateOperators"):
        jml_drop = progress["stateOperators"][0].get("numRowsDroppedByWatermark", 0)
        if jml_drop > 0:
            print("late event ke-drop watermark:", jml_drop, "| watermark:", progress["eventTime"].get("watermark"))
