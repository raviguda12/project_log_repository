from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import Rawlayer,Cleansedlayer

class Curatedlayer():

raw7 = raw6.withColumn("status_code", col("status_code").cast("int")).withColumn("row_id", col("row_id").cast("int"))

raw7.show(truncate=False)

raw7.dtypes


raw7.select("datetime").distinct().show(5000)

def split_date(val):
  return val.split(":")[0]

split_date_udf = udf(lambda x: split_date(x), StringType())

cnt_cond = lambda cond: sum(when(cond, 1).otherwise(0))
raw8 = raw7.withColumn("day_hour", split_date_udf(col("datetime"))).groupBy("day_hour", "client/ip") \
											    .agg(cnt_cond(col('method') == "GET").alias("no_get"), \
											         cnt_cond(col('method') == "POST").alias("no_post"), \
											         cnt_cond(col('method') == "HEAD").alias("no_head"), \
											        ).orderBy(asc("day_hour")).withColumn("row_id", monotonically_increasing_id())\
                              .select("row_id", "day_hour","client/ip","no_get","no_post","no_head")

raw8.show(truncate=False)

raw8.groupBy("day_hour").count().orderBy(desc("count")).show(200)

# per device end

raw9 = raw8.groupBy("day_hour") \
											    .agg(count(col("client/ip")).alias("no_of_clients"), \
                              sum(col('no_get')).alias("no_get"), \
											         sum(col('no_post')).alias("no_post"), \
											         sum(col('no_head')).alias("no_head"), \
											        ).orderBy(asc("day_hour")).withColumn("row_id", monotonically_increasing_id())\
                              .select("row_id", "day_hour","no_of_clients","no_get","no_post","no_head")

raw9.show(700,truncate=False)

# across device end

def write_to_s3(self):
        self.raw_df.write.csv("s3a://project-layers-kamal/raw-layer/", mode="append",header=True)

def write_to_hive(self):
        pass
        # **************************
        #self.raw_df.write.csv("s3a://project-layers-kamal/raw-layer/raw.csv", mode="append",header=True)
        self.raw_df.write.saveAsTable('RawDataTable')


