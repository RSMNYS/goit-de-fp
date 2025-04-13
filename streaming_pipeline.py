from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os

from configs import kafka_config

jdbc_url = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
jdbc_user = "neo_data_admin"
jdbc_password = "Proyahaxuqithab9oplp"

input_topic = "athlete_event_results"
output_topic = "athlete_stats"
jdbc_jar = "mysql-connector-j-8.0.32.jar"

if not os.path.exists(jdbc_jar):
    print(f"Увага: JDBC драйвер {jdbc_jar} не знайдено. Переконайтеся, що він доступний.")

print("Ініціалізую Spark сесію...")
spark = SparkSession.builder \
    .appName("AthleteBioAndResultsStreamingPipeline") \
    .master("local[*]") \
    .config("spark.jars", jdbc_jar) \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.driver.host", "127.0.0.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Етап 1: Зчитати дані фізичних показників атлетів з MySQL
print("Етап 1: Зчитування даних фізичних показників атлетів з MySQL")
athlete_bio_df = spark.read.format('jdbc').options(
    url=jdbc_url,
    driver='com.mysql.cj.jdbc.Driver',
    dbtable="athlete_bio",
    user=jdbc_user,
    password=jdbc_password).load()

athlete_bio_df.printSchema()
athlete_bio_df.show(5)

# Етап 2: Фільтрація
print("Етап 2: Фільтрація даних за зростом та вагою")
filtered_bio_df = athlete_bio_df.filter(
    col("height").isNotNull() &
    col("weight").isNotNull() &
    col("height").cast("float").isNotNull() &
    col("weight").cast("float").isNotNull()
)
print("Filtered Bio Data Count:", filtered_bio_df.count())
filtered_bio_df.show(5)

# Етап 3a: Завантаження даних з MySQL та запис у Kafka
print("Етап 3a: Зчитування результатів змагань з MySQL")
event_results_df = spark.read.format('jdbc').options(
    url=jdbc_url,
    driver='com.mysql.cj.jdbc.Driver',
    dbtable="athlete_event_results",
    user=jdbc_user,
    password=jdbc_password).load()

event_results_df.printSchema()
event_results_df.show(5)

print("Запис у Kafka...")
event_results_df.selectExpr("to_json(struct(*)) AS value") \
    .write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
    .option("kafka.security.protocol", kafka_config['security_protocol']) \
    .option("kafka.sasl.mechanism", kafka_config['sasl_mechanism']) \
    .option("kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["username"]}" password="{kafka_config["password"]}";') \
    .option("topic", input_topic) \
    .save()

print("Data has been written to Kafka topic.")

# Схема для Kafka
event_schema = StructType([
    StructField("edition", StringType(), True),
    StructField("edition_id", IntegerType(), True),
    StructField("country_noc", StringType(), True),
    StructField("sport", StringType(), True),
    StructField("event", StringType(), True),
    StructField("result_id", IntegerType(), True),
    StructField("athlete", StringType(), True),
    StructField("athlete_id", IntegerType(), True),
    StructField("pos", StringType(), True),
    StructField("medal", StringType(), True),
    StructField("isTeamSport", StringType(), True)
])

# Етап 3b: Зчитування з Kafka
print("Етап 3b: Налаштування Kafka stream")
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
    .option("kafka.security.protocol", kafka_config['security_protocol']) \
    .option("kafka.sasl.mechanism", kafka_config['sasl_mechanism']) \
    .option("kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["username"]}" password="{kafka_config["password"]}";') \
    .option("subscribe", input_topic) \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 20) \
    .load()

parsed_stream = kafka_stream \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), event_schema).alias("data")) \
    .select("data.*") \
    .withColumn("athlete_id", col("athlete_id").cast("int"))

# Етап 4-6: Обробка батчів
def process_batch(batch_df, batch_id):
    print(f"\n====================")
    print(f"⚙️ Обробка батчу {batch_id}")

    try:
        if batch_df.rdd.isEmpty():
            print(f"⚠️ Батч {batch_id} порожній — нічого не обробляємо.")
            return

        print(f"✅ Batch {batch_id} містить {batch_df.count()} рядків")
        print("🔍 Перші 5 рядків batch_df:")
        print(batch_df.limit(5).collect())  # Обережно з великими обʼємами

        print("🧩 Схема batch_df:")
        batch_df.printSchema()

        # Етап 4: Обʼєднання
        print("🔗 Етап 4: Обʼєднання з athlete_bio")
        joined_df = batch_df.join(
            filtered_bio_df,
            batch_df.athlete_id == filtered_bio_df.athlete_id,
            "inner"
        )

        print(f"🔎 Обʼєднаних рядків: {joined_df.count()}")
        joined_df.show(5)

        # Етап 5: Обчислення
        print("📊 Етап 5: Розрахунок статистики")
        stats_df = joined_df.groupBy(
            "sport",
            "medal",
            filtered_bio_df["sex"],
            filtered_bio_df["country_noc"]
        ).agg(
            avg("height").alias("avg_height"),
            avg("weight").alias("avg_weight")
        ).withColumn("timestamp", current_timestamp())

        print("📈 Результати статистики:")
        stats_df.show()

        # Етап 6a: Запис у Kafka
        print("📤 Етап 6a: Запис у Kafka")
        stats_df.selectExpr("to_json(struct(*)) AS value") \
            .write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
            .option("kafka.security.protocol", kafka_config['security_protocol']) \
            .option("kafka.sasl.mechanism", kafka_config['sasl_mechanism']) \
            .option("kafka.sasl.jaas.config",
                    f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["username"]}" password="{kafka_config["password"]}";') \
            .option("topic", output_topic) \
            .save()

        # Етап 6b: Запис у MySQL
        print("💾 Етап 6b: Запис у MySQL")
        stats_df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", "sergijr.athlete_stats") \
            .option("user", jdbc_user) \
            .option("password", jdbc_password) \
            .mode("append") \
            .save()

        print(f"✅ Батч {batch_id} успішно оброблено.")
    except Exception as e:
        print(f"❌ Помилка при обробці батчу {batch_id}: {str(e)}")

# Запуск стримінгу
print("Запуск потоку...")
query = parsed_stream \
    .writeStream \
    .foreachBatch(process_batch) \
    .outputMode("update") \
    .trigger(processingTime="10 seconds") \
    .start()

print("✅ Потоковий запит запущено. Очікування завершення...")
query.awaitTermination()