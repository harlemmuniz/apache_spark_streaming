from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__ == "__main__":

    spark = SparkSession.builder.appName("Kafka + Spark Streaming + Google Big Query").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    bigquery_table = "dataset-name.table-name"
    spark.conf.set("temporaryGcsBucket","bucket-name")

    df = (
        spark.readStream.format("kafka")
        .option(
            "kafka.bootstrap.servers",
            "<broker-1-ip>:<broker-1-port>,<broker-2-ip>:<broker-2-port>,<broker-n-ip>:<broker-n-port>",
        )
        .option("subscribe", "kafka-topic")
        .option("group.id", "spark-group")
        .load()
    )
    df.printSchema()
    df.selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value")
    
    df2 = df.withColumn("key", df.key.cast("string")).withColumn(
        "value", df.value.cast("string")
    )
    df2.printSchema()

    schema_bigquery = StructType([
        StructField("hitType", StringType(), True),
        StructField("page", StringType(), True),    
        StructField("clientId", StringType(), True),
        StructField("eventCategory", StringType(), True),
        StructField("eventAction", StringType(), True),
        StructField("eventLabel", StringType(), True),
        StructField("utmSource", StringType(), True),
        StructField("utmMedium", StringType(), True),
        StructField("utmCampaign", StringType(), True),
        StructField("timestampGTM", StringType(), True),
    ])

    dataframe = df2.withColumn("dataLayer", from_json(col("value"), schema_bigquery)).select(col('dataLayer.*'), col('key'), col("topic"), col('partition'), col('offset'), col('timestamp'))

    query = (
        dataframe.writeStream.format("console").outputMode("append")
        .trigger(processingTime="60 second")
        .start()
    )

    query = (
        dataframe.writeStream 
        .format("bigquery") 
        .option("checkpointLocation", "gs://bucket-location/checkpoints/") 
        .option("table", bigquery_table)
        .trigger(processingTime="60 second")
        .start()
    )

    query.awaitTermination()
