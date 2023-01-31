# Databricks notebook source
# MAGIC %md
# MAGIC # Ingesting Protobufs files on S3
# MAGIC In this file, the objective is to find ways to convert all protobuf files in a folder to an object that can be manipulated in Databricks
# MAGIC 
# MAGIC **REMINDER**
# MAGIC SparkSession and SparkContext are created automatically by Databricks and are accessible through *spark* command

# COMMAND ----------

spark

# COMMAND ----------

spark.sparkContext
# SparkSession is a single-unified entry point to manipulate data with Spark
# only a single SparkContext exists per JVM
# It allows you to configure Spark configuration parameters

# COMMAND ----------

sys.path.append(os.path.abspath("/Repos/harold.nikoue@warnermedia.com/MUX_Quality_Discovery_EDA/ancillary"))

# COMMAND ----------

print(sys.path)

# COMMAND ----------

# MAGIC %pip install protobuf
# MAGIC %pip install pbspark

# COMMAND ----------

import os
import sys
import numpy as np
from google.protobuf.json_format import MessageToJson, MessageToDict
from typing import Iterator, Dict
from pbspark import MessageConverter

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pick one file from January 10th

# COMMAND ----------

# read one file
files_on_one_day = dbutils.fs.ls("s3://mux-dplusna-finalized-view-data/2023/01/10")
files_in_one_hour = list(dbutils.fs.ls(files_on_one_day[20].path))
list_of_full_session_paths = [session_path_record[0] for session_path_record in files_in_one_hour]

# COMMAND ----------

selected_file = np.random.choice(list_of_full_session_paths)

# COMMAND ----------

selected_file

# COMMAND ----------

# MAGIC %md
# MAGIC ## ChatGPT code to read protobuf files

# COMMAND ----------

# Here is an example of a PySpark function that ingests multiple protobuf files into a dataframe:
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# COMMAND ----------

def ingest_protobuf_to_df(spark: SparkSession, file_path: str) -> None:
    # Create a DataFrame from the protobuf files
    df = spark.read.format("binaryFile").load(file_path)
    # Use the `select` function to extract the binary data
    df = df.select(F.col("value").alias("binary_data"))
    # Convert the binary data to a string
    df = df.select(F.concat_ws("", F.col("binary_data")).alias("binary_string"))
    # Deserialize the protobuf message
    df = df.select(F.from_avro("binary_string", "protobuf").alias("protobuf_msg"))
    # Extract the fields of the protobuf message
    df = df.select("protobuf_msg.*")
    # Show the DataFrame
    df.show()

# COMMAND ----------

spark = SparkSession.builder.appName("ProtobufIngestion").getOrCreate()
ingest_protobuf_to_df(spark, selected_file)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Python file from the compiling of the proto schema file
# MAGIC ## Use it to deserialize a data file

# COMMAND ----------

# MAGIC %run /Repos/harold.nikoue@warnermedia.com/MUX_Quality_Discovery_EDA/ancillary/eventStream_pb2

# COMMAND ----------

# function to read one file and create a dictionary
def ingest_proto_file_into_view_dictionary(filename: str) -> Dict[str, str]:
  f = open(filename, "rb")
  view_object.ParseFromString(f.read())
  return MessageToDict(view_object)

# COMMAND ----------

# choose a test file
list_of_test_files = list(dbutils.fs.ls("FileStore/CAP/QoS_QoE/test"))
chosen_test_file = np.random.choice([path_record[0].replace("dbfs:/", "/dbfs/") for path_record in list_of_test_files])
print(chosen_test_file)

# COMMAND ----------

view_object = ExternalVideoViewRecord()

# COMMAND ----------

# read a test file
f = open(chosen_test_file, "rb")
view_object.ParseFromString(f.read())
print(view_object)

# COMMAND ----------

view_object.IsInitialized()
# use it when processing

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use PbSpark to convert the Protobuf message to Spark Dataframe

# COMMAND ----------

view_data = [{"value": view_object.SerializeToString()}] # this is a message
example_encoded_df = spark.createDataFrame(view_data)

# COMMAND ----------

from pbspark import from_protobuf

# COMMAND ----------

# see pbspark documentation
mc = MessageConverter()
example_decoded_df = example_encoded_df.select(from_protobuf(example_encoded_df.value, ExternalVideoViewRecord).alias("value"))
example_expanded_df = example_decoded_df.select("value.*")
example_expanded_df.show()

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.functions import udf

# COMMAND ----------

isinstance(example_encoded_df.value, str)

# COMMAND ----------

view_object.view_id
view_object.property_id
view_object.view_start
view_object.view_end
view_object.asn
view_object.browser
view_object.browser_version
view_object.cdn
view_object.city
view_object.continent_code
view_object.country
view_object.country_name
view_object.custom_2
view_object.custom_3
view_object.custom_4
view_object.custom_5
view_object.error_type
view_object.exit_before_video_start
view_object.experiment_name
view_object.latitude
view_object.longitude
view_object.max_downscale_percentage
view_object.max_upscale_percentage
view_object.mux_api_version
view_object.mux_embed_versin
view_object.mux_viewer_id
view_object.operating_system
view_object.operating_system_version
view_object.page_load_time
view_object.page_url
view_object.playback_success_score
view_object.player_autoplay
view_object.player_error_code
view_object.player_error_message
view_object.player_height
view_object.player_instance_id
view_object.player_language
view_object.player_mux_plugin_name
view_object.player_mux_plugin_version
view_object.player_name
view_object.player_preload
view_object.player_software
view_object.player_software_version
view_object.player_source_domain
view_object.player_source_duration
view_object.player_source_height
view_object.player_source_url
view_object.player_source_width
view_object.player_startup_time
view_object.player_version
view_object.player_view_count 
view_object.player_width
view_object.rebuffer_count
view_object.rebuffer_duration
view_object.rebuffer_frequency
view_object.rebuffer_percentage
view_object.region
view_object.session_id
view_object.smoothness_score
view_object.source_hostname
view_object.source_type
view_object.startup_time_score
view_object.used_fullscreen
view_object.video_content_type
view_object.video_duration
view_object.video_encoding_variant
view_object.video_id 
view_object.video_quality_score
view_object.video_series
view_object.video_startup_time 
view_object.video_title
view_object.view_max_playhead_position
view_object.view_playing_time
view_object.view_seek_count
view_object.view_seek_duration
view_object.view_session_id
view_object.view_total_content_playback_time
view_object.view_total_downscaling
view_object.view_total_upscaling
view_object.viewer_application_engine
view_object.viewer_connection_type
view_object.viewer_device_category
view_object.viewer_device_manufacturer
view_object.viewer_device_name
view_object.viewer_experience_score
view_object.viewer_os_architecture
view_object.viewer_user_agent
view_object.viewer_user_id
view_object.watch_time
view_object.watched
view_object.weighted_average_bitrate
view_object.preroll_played
view_object.requests_for_first_preroll
view_object.video_startup_preroll_load_time
view_object.video_startup_preroll_request_time
view_object.stream_type


# COMMAND ----------

view_json = MessageToJson(view_object)

# COMMAND ----------

view_dic = MessageToDict(view_object)

# COMMAND ----------

#list all fields
view_dic.keys()

# COMMAND ----------

# convert the data to a rdd
df = spark.createDataFrame()

# COMMAND ----------

view_object.DESCRIPTOR.full_name

# COMMAND ----------

view_object.DESCRIPTOR.fields

# COMMAND ----------


