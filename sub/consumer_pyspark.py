from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092'
SUHU_TOPIC = "sensor-suhu-gudang"
KELEMBABAN_TOPIC = "sensor-kelembaban-gudang"

# Skema untuk data suhu
suhu_schema = StructType([
    StructField("gudang_id", StringType(), True),
    StructField("suhu", IntegerType(), True),
    StructField("timestamp", DoubleType(), True)
])

# Skema untuk data kelembaban
kelembaban_schema = StructType([
    StructField("gudang_id", StringType(), True),
    StructField("kelembaban", IntegerType(), True),
    StructField("timestamp", DoubleType(), True)
])

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("WarehouseMonitor") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Tangkap stream dari Kafka
    df_suhu_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", SUHU_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    df_kelembaban_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KELEMBABAN_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # Parse JSON dari value Kafka dan tambahkan timestamp event
    df_suhu = df_suhu_raw.selectExpr("CAST(value AS STRING) as json_string") \
        .select(from_json(col("json_string"), suhu_schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_time_suhu", (col("timestamp")).cast(TimestampType())) \
        .withColumnRenamed("gudang_id", "gudang_id_suhu") \
        .withColumnRenamed("suhu", "nilai_suhu")

    df_kelembaban = df_kelembaban_raw.selectExpr("CAST(value AS STRING) as json_string") \
        .select(from_json(col("json_string"), kelembaban_schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_time_kelembaban", (col("timestamp")).cast(TimestampType())) \
        .withColumnRenamed("gudang_id", "gudang_id_kelembaban") \
        .withColumnRenamed("kelembaban", "nilai_kelembaban")

    # Peringatan Suhu Tinggi
    query_suhu_alert = df_suhu \
        .filter(col("nilai_suhu") > 80) \
        .selectExpr("'[Peringatan Suhu Tinggi] Gudang ' || gudang_id_suhu || ': Suhu ' || nilai_suhu || '°C' AS message") \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    # Peringatan Kelembaban Tinggi
    query_kelembaban_alert = df_kelembaban \
        .filter(col("nilai_kelembaban") > 70) \
        .selectExpr("'[Peringatan Kelembaban Tinggi] Gudang ' || gudang_id_kelembaban || ': Kelembaban ' || nilai_kelembaban || '%' AS message") \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    # Watermarking stream suhu dan kelembaban
    df_suhu_watermarked = df_suhu.withWatermark("event_time_suhu", "10 seconds")
    df_kelembaban_watermarked = df_kelembaban.withWatermark("event_time_kelembaban", "10 seconds")

    # Join stream suhu dan kelembaban berdasarkan gudang_id
    joined_df = df_suhu_watermarked.join(
        df_kelembaban_watermarked,
        expr("""
            gudang_id_suhu = gudang_id_kelembaban AND
            event_time_suhu >= event_time_kelembaban - interval 5 seconds AND
            event_time_suhu <= event_time_kelembaban + interval 5 seconds
        """),
        "inner"
    )

    # Status report untuk setiap gudang
    status_df = joined_df.select(
        col("gudang_id_suhu").alias("gudang_id"),
        col("nilai_suhu"),
        col("nilai_kelembaban"),
        col("event_time_suhu").alias("event_time")
    ).withColumn("status",
        when((col("nilai_suhu") > 80) & (col("nilai_kelembaban") > 70), "Bahaya tinggi! Barang berisiko rusak")
        .when(col("nilai_suhu") > 80, "Suhu tinggi, kelembaban normal")
        .when(col("nilai_kelembaban") > 70, "Kelembaban tinggi, suhu aman")
        .otherwise("Aman")
    )

    # Fungsi untuk memformat output gabungan per batch
    def process_combined_batch(batch_df, epoch_id):
        if batch_df.count() == 0:
            return
        
        print(f"\n--- Laporan Gabungan Batch {epoch_id} ---")
        collected_rows = batch_df.orderBy("gudang_id", "event_time").collect()

        for row in collected_rows:
            if row.status == "Bahaya tinggi! Barang berisiko rusak":
                print("\n[PERINGATAN KRITIS]")
            else:
                print(f"\n[Status Gudang]")

            print(f"Gudang {row.gudang_id}:")
            print(f"  - Suhu: {row.nilai_suhu}°C")
            print(f"  - Kelembaban: {row.nilai_kelembaban}%")
            print(f"  - Status: {row.status}")
            print(f"  - Waktu Event: {row.event_time}")
        print("--- Akhir Laporan Gabungan Batch ---")


    # Output gabungan ke console menggunakan foreachBatch
    query_combined_status = status_df.writeStream.outputMode("append") \
      .foreachBatch(process_combined_batch) \
      .trigger(processingTime="10 seconds") \
      .start()

    # Tunggu semua stream query selesai
    spark.streams.awaitAnyTermination()