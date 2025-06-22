from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# ====================================================================
# 1. Konfigurasi dan Inisialisasi Spark Session
# ====================================================================

# Konfigurasi untuk terhubung ke MinIO (S3 compatible storage)
# Kredensial ini harus sama dengan yang ada di docker-compose.yml Anda
S3_ENDPOINT = "http://minio:9000"  # Gunakan nama layanan 'minio', bukan 'localhost'
S3_ACCESS_KEY = "admin"
S3_SECRET_KEY = "password123"

# Inisialisasi Spark Session dengan dukungan Delta Lake dan konektor ke S3/MinIO
# Ini adalah bagian boilerplate yang penting
spark = SparkSession.builder \
    .appName("RealtimeETLFromKafkaToDelta") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Mengurangi pesan log yang tidak perlu di console
spark.sparkContext.setLogLevel("ERROR")

print("Spark Session berhasil dibuat dan dikonfigurasi.")

# ====================================================================
# 2. Baca Data Streaming dari Kafka
# ====================================================================

# 'readStream' memberitahu Spark untuk membuat koneksi berkelanjutan ke Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "user_events") \
    .option("startingOffsets", "latest") \
    .load()

print("Berhasil terhubung ke topic 'user_events' di Kafka.")

# ====================================================================
# 3. Transformasi Data
# ====================================================================

# Data dari Kafka datang dalam format key-value. 'value' berisi data JSON kita.
# Pertama, kita definisikan skema data agar Spark tahu cara membacanya.
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", StringType(), True), # Ubah ke String agar konsisten dengan 'asin'
    StructField("timestamp", TimestampType(), True),
    StructField("ip_address", StringType(), True),
])

# 'from_json' adalah fungsi Spark untuk mem-parsing kolom string yang berisi JSON
# menjadi beberapa kolom terstruktur sesuai skema yang kita definisikan.
transformed_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data")) \
                         .select("data.*")

print("Transformasi data dari JSON ke kolom-kolom DataFrame berhasil.")
print("Skema DataFrame yang akan ditulis:")
transformed_df.printSchema()

# ====================================================================
# 4. Tulis Data Stream ke Delta Lake (MinIO) - Silver Layer
# ====================================================================

# Tentukan lokasi di MinIO tempat kita akan menyimpan Delta Table ini
# Ini akan membuat folder 'user_events' di dalam bucket 'datalake' Anda.
delta_path = "s3a://datalake/silver/user_events"
checkpoint_path = "s3a://datalake/checkpoints/user_events"

# 'writeStream' memulai proses penulisan berkelanjutan
query = (
    transformed_df.writeStream
        .format("delta")                # Format output adalah Delta Lake
        .outputMode("append")           # Tambahkan data baru (jangan timpa yang lama)
        .option("checkpointLocation", checkpoint_path) # Folder untuk menyimpan state dari stream
        .start(delta_path)              # Mulai proses penulisan
)

print(f"Streaming data dari Kafka ke Delta Lake di path: {delta_path}")
print("Menunggu data baru... (Biarkan skrip ini berjalan)")

# 'awaitTermination' membuat skrip tetap berjalan untuk memproses data
# yang masuk secara terus-menerus.
query.awaitTermination()