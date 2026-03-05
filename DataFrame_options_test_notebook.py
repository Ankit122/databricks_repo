# Databricks notebook source
import pyspark 
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark import SparkFiles
from pyspark.sql.functions import split
spark = SparkSession.builder.master("local").appName("DataframeOption").getOrCreate()
df=spark.read.option("header",True).csv("/FileStore/tables/tutorial_file_with_header.txt")
df.show()

# COMMAND ----------

# DBTITLE 1,Add new column
# df1 = df.withColumn("db_type_cd",lit("Relation Database"))
# df2 = df1.withColumn("db_alterName", lit("Altername name"))
df1 = df.withColumn("new_db_type", lit("Relational Database"))
df1 = df.withColumn("db_altername", lit("relational database"))
df2 = df1.withColumn("new_db_alterName", lit("Put Altername name here..")).show()


# COMMAND ----------

#df2 = df1.withColumn("only_4_char_of_db_name", substring("db_name",1,4)).show()
#by Select column
#df3 = df2.select("db_alterName", substring("db_alterName", 1,4).alias("new altername"))
#df3 = df2.select(substring("db_alterName", 1,4).alias("newatername"))
#df3 = df2.select("db_name", substring("db_name", 1,4).alias("only_4_char_of_db_name"))
#df2.show()
#revision
df2 = df1.withColumn("remove_4_char_from_string", substring("db_name", 1,4))
df3 = df2.select(substring("db_name",1,4).alias("newstring")).show()


# COMMAND ----------

#Update Column value based on condition:
#df3 = df.select("db_id", "db_name", when(col("db_type")=="RDBMS", "On Premise").when(col("db_type")=="CloudDB", "Cloud").otherwise("Not Known").alias("db_type"))
# df3 = df.select("db_type", "db_name", when(col("db_id")=="12", "On Premise").when(col("db_id")=="14", "Cloud").otherwise("Not Known").alias("db_id"))
df3 = df2.select(when(col("db_type")=="RDBMS","AnkitRDBMS").when(col("db_type")=="CloudDB","AnkitCloudDB").otherwise("NotKnown").alias("newDb_type"))
df3.show()

# COMMAND ----------

#Change Column datatype in dataframe
#df4 = df.select(col("db_name").cast("integer"),"db_id","db_type")
df4 = df.select(col("db_name").cast("integer"))
df4.printSchema()

# COMMAND ----------

#Change Column name in dataframe
# df5 = df.withColumnRenamed("db_type","db_type_CD").show()
#df5 = df.withColumnRenamed("db_type", "db_type_new_cd").show()
#copy one column to new column
#df5 = df.withColumn("db_type_New",df["db_type"]).drop("db_type").show()
df5 = df.withColumn("db_type_New", df["db_type"]).drop("db_type").show()
#df5 = df.select("db_id","db_name",col("db_type").alias("db_type_select_CD")).show()

# COMMAND ----------

spark = spark.sparkContext

# COMMAND ----------

rows = [["alex",25],["ankit", 26]]
df_n= spark.createDataFrame(rows)
df_n.show()

# COMMAND ----------

schema = StructType([
  StructField("name", StringType()),
  StructField("age", StringType())
])
rows = [["ankit", 25],["sheebu", 26]]
df = spark.createDataFrame(rows, schema)
df.show(e a)
schema = StructType([
  StructField("name", StringType()),
  StructField("age", StringType())
])
row = [["ankit", 25], ["sheebu", 26]]
df = spark.createDataFrame(rows, schema)
df.shoe()

# COMMAND ----------


