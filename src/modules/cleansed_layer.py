from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import Rawlayer

class Cleansedlayer():
  def convert_to_kb(val):
    return int(val)/(10**3)

  convert_to_kb_udf = udf(lambda x: convert_to_kb(x), FloatType())

  raw4 = raw3.withColumn("size", convert_to_kb_udf("size"))

  raw4.show(truncate=False)

  raw5 = raw4.withColumn("datetime", to_timestamp(col("datetime"), "dd/MMM/yyyy:HH:mm:ss +SSSS")).withColumn('datetime', date_format(col("datetime"),"MM/dd/yyyy HH:mm:ss"))

  raw5.show(truncate=False)

  raw5.select("datetime").distinct().show()

  raw5.dtypes

  raw6 = raw5.withColumn("referer", when(col("referer") == "NA","N").otherwise("Y")).withColumnRenamed("referer", "referer_present")

  raw6.show(truncate=False)

  raw6.dtypes

  # for i in raw6.columns:
  #   raw6.select(i).distinct().show()

if __name__ == '__main__':
    convert_to_kb(val)