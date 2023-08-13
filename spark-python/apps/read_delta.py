from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from config.config import config_dict

spark: SparkSession = SparkSession.builder.appName(config_dict["spark"]).getOrCreate()

spark.conf.set("fs.s3a.endpoint", "http://minio:9000")
spark.conf.set("fs.s3a.access.key", "minioadmin")
spark.conf.set("fs.s3a.secret.key", "minioadmin")
spark.conf.set("fs.s3a.path.style.access", "true")
spark.conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

data = spark.read.format("delta").load("s3a://delta/simple-table")
data.show()