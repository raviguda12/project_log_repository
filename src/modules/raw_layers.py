from pyspark.sql import SparkSession

from pyspark.sql.types import *

from pyspark.sql.functions import *

from pyspark.sql import S3Session

class Rawlayer:

	def __init__(self):
    sc = self.spark.sparkContext
    sc.setLogLevel("Error")

	def raw_layer():
		spark = SparkSession.builder.getOrCreate()

		spark_context = spark.sparkContext

		rdd_file = spark_context.textFile("/content/drive/MyDrive/Demo Project (1)/project-demo-processed-input.txt")

		rdd_file.take(2)

	def get_col_val(val):
		try:
			splitted_str = val.split('"')
		except:
			return ["", "", "", "", "", "", "", ""]

		try:
				ip_col = splitted_str[0].split("-")[0].strip()
		except:
				ip_col = "-"

		try:
				date_col = splitted_str[0].split("[")[1].rstrip("] ")
		except:
					date_col = "-"

		try:
				method_col = splitted_str[1].split(" ")[0]
		except:
					method_col = "-"

		try:
				request_col = " ".join(splitted_str[1].split(" ")[1:])
		except:
					request_col = "-"

		try:
				status_code_col = splitted_str[2].split(" ")[1]
		except:
				status_code_col = "-"

		try:
				size_col = splitted_str[2].split(" ")[2]
		except:
					size_col = "-"

		try:
				referer_col = splitted_str[3]
		except:
					referer_col = "-"

		try:
				device_col = splitted_str[5]
		except:
					device_col = "-"

		fin_list = [ip_col, date_col, method_col, request_col, status_code_col, size_col, referer_col, device_col]

		return ['' if val_str == '-' else val_str for val_str in fin_list]

		transformed_rdd = rdd_file.map(lambda x: get_col_val(x))

		schema = StructType(
									    [
									        StructField("client/ip", StringType(), True),
									        StructField("datetime", StringType(), True),
									        StructField("method", StringType(), True),
									        StructField("request", StringType(), True),
									        StructField("status_code", StringType(), True),
									        StructField("size", StringType(), True),
									        StructField("referer", StringType(), True),
									        StructField("user_agent", StringType(), True)
									    ]
									)

		log_df = spark.createDataFrame(transformed_rdd, schema)

		log_df.show(truncate=False)

		log_df = log_df.withColumn("row_id", monotonically_increasing_id())

		log_df.show()

		log_df.columns

		raw1 = log_df.select("row_id","client/ip","datetime","method","request","status_code","size","referer","user_agent")

		raw1.show(truncate=False)

	def remove_special_char(val):
		  remove_char_list = ['%', '-', '?', ",", "="]
		  return "".join(["" if val_str in remove_char_list else val_str for val_str in val])

		remove_special_char_udf = udf(lambda x: remove_special_char(x), StringType())

		raw2 = raw1.withColumn("request", remove_special_char_udf("request")).withColumn("referer", remove_special_char_udf("referer"))

		raw2.show(truncate=False)

		raw3 = raw2.select([when(col(c)=="","NA").otherwise(col(c)).alias(c) for c in raw2.columns])

		raw3.show()

if __name__ == '__main__':
    raw_layer()
    get_col_val(val)
    remove_special_char(val)