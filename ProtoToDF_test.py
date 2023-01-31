# Databricks notebook source
# Start by installing some libraries
%pip install protobuf==4.21.12
%pip install pbspark==0.8.0
%pip install dill==0.3.6

# COMMAND ----------

from pyspark.sql.session import SparkSession
from pbspark import from_protobuf
from pbspark import to_protobuf
from pbspark import df_from_protobuf
from pbspark import df_to_protobuf


# COMMAND ----------

spark.sparkContext.addPyFile("/dbfs/FileStore/CAP/QoS_QoE/test/example_pb2.py")
from example_pb2 import SimpleMessage

# COMMAND ----------

# MAGIC %run /Repos/harold.nikoue@warnermedia.com/MUX_Quality_Discovery_EDA/example/example_pb2

# COMMAND ----------

example = SimpleMessage(name="hello", quantity=5, measure=12.3)
data = [{"value": example.SerializeToString()}]
df_encoded = spark.createDataFrame(data)

# COMMAND ----------

df_decoded = df_encoded.select(from_protobuf(df_encoded.value, SimpleMessage).alias("value"))
df_expanded = df_decoded.select("value.*")
df_expanded.show()

# COMMAND ----------

import dill
dill.detect.trace(True)
dill.detect.errors(df_encoded.value)

# COMMAND ----------



# COMMAND ----------

df_expanded = df_from_protobuf(df_encoded, SimpleMessage, expanded=True)
df_expanded.show()

# COMMAND ----------


