# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC # Ingesting MUX Proto Messages on Databricks
# MAGIC ## Reading proto messages into Spark Dataframe
# MAGIC The purpose of this notebook is to develop a method to ingest protobuf files stored on dbfs and convert them to pyspark dataframes
# MAGIC ## Start date
# MAGIC 1/24/2023
# MAGIC ## Completed date
# MAGIC TBD
# MAGIC 
# MAGIC ## Current Status
# MAGIC Can read one file into a spark dataframe but the udf functionality to generalize to all files in a folder breaks down
# MAGIC ## Next
# MAGIC Read multiple files at once
# MAGIC 
# MAGIC *This file follows ParsingProtoMessagesIntoSparkDFv0.2 and cleans up the most promising process*

# COMMAND ----------

# Start by installing some libraries
%pip install protobuf==4.21.12
%pip install pbspark==0.8.0

# COMMAND ----------

import os
import sys
import numpy as np
import inspect
from pprint import PrettyPrinter
from typing import Iterator, Dict
import typing as t
from google.protobuf import json_format
from google.protobuf.json_format import MessageToJson, MessageToDict
from google.protobuf.message import Message
from google.protobuf.descriptor import Descriptor
from google.protobuf.descriptor import FieldDescriptor
from google.protobuf.descriptor_pool import DescriptorPool
from google.protobuf.timestamp_pb2 import Timestamp
from pbspark import MessageConverter
from pbspark import from_protobuf
from pyspark.sql.types import ArrayType
from pyspark.sql.types import BinaryType
from pyspark.sql.types import BooleanType
from pyspark.sql.types import DataType
from pyspark.sql.types import DoubleType
from pyspark.sql.types import FloatType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import LongType
from pyspark.sql.types import Row
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import TimestampType
from pyspark.sql import DataFrame as SparkDataFrame

# COMMAND ----------

# Protobuf types map to these CPP Types. We map
# them to Spark types for generating a spark schema.
# Note that bytes fields are specified by the `type` attribute in addition to
# the `cpp_type` attribute so there is special handling in the `get_spark_schema`
# method.
_CPPTYPE_TO_SPARK_TYPE_MAP: t.Dict[int, DataType] = {
    FieldDescriptor.CPPTYPE_INT32: IntegerType(),
    FieldDescriptor.CPPTYPE_INT64: LongType(),
    FieldDescriptor.CPPTYPE_UINT32: LongType(),
    FieldDescriptor.CPPTYPE_UINT64: LongType(),
    FieldDescriptor.CPPTYPE_DOUBLE: DoubleType(),
    FieldDescriptor.CPPTYPE_FLOAT: FloatType(),
    FieldDescriptor.CPPTYPE_BOOL: BooleanType(),
    FieldDescriptor.CPPTYPE_ENUM: StringType(),
    FieldDescriptor.CPPTYPE_STRING: StringType(),
}

# Built in types like these have special methods
# for serialization via MessageToDict. Because the
# MessageToDict function is an intermediate step to
# JSON, some types are serialized to strings.
_MESSAGETYPE_TO_SPARK_TYPE_MAP: t.Dict[str, DataType] = {
    # google/protobuf/timestamp.proto
    "google.protobuf.Timestamp": StringType(),
    # google/protobuf/duration.proto
    "google.protobuf.Duration": StringType(),
    # google/protobuf/wrappers.proto
    "google.protobuf.DoubleValue": DoubleType(),
    "google.protobuf.FloatValue": FloatType(),
    "google.protobuf.Int64Value": LongType(),
    "google.protobuf.UInt64Value": LongType(),
    "google.protobuf.Int32Value": IntegerType(),
    "google.protobuf.UInt32Value": LongType(),
    "google.protobuf.BoolValue": BooleanType(),
    "google.protobuf.StringValue": StringType(),
    "google.protobuf.BytesValue": BinaryType(),
}
_message_type_to_spark_type_map = _MESSAGETYPE_TO_SPARK_TYPE_MAP.copy()

# COMMAND ----------

def get_spark_schema(
        descriptor: t.Union[t.Type[Message], Descriptor],
        preserving_proto_field_name: bool = False,
        use_integers_for_enums: bool = False,
    ) -> DataType:
        """Generate a spark schema from a message type or descriptor
        Given a message type generated from protoc (or its descriptor),
        create a spark schema derived from the protobuf schema when
        serializing with ``MessageToDict``.
        Args:
            descriptor: A message type or its descriptor
            preserving_proto_field_name: If True, use the original proto field
                names as defined in the .proto file. If False, convert the field
                names to lowerCamelCase.
            use_integers_for_enums: If true, print integers instead of enum names.
        """
        schema = []
        if inspect.isclass(descriptor) and issubclass(descriptor, Message):
            descriptor_ = descriptor.DESCRIPTOR
        else:
            descriptor_ = descriptor  # type: ignore[assignment]
        full_name = descriptor_.full_name

        for field in descriptor_.fields:
            spark_type: DataType
            if field.cpp_type == FieldDescriptor.CPPTYPE_MESSAGE:
                full_name = field.message_type.full_name
                if full_name in _message_type_to_spark_type_map:
                    spark_type = _message_type_to_spark_type_map[full_name]
                else:
                    spark_type = get_spark_schema(
                        descriptor=field.message_type,
                        preserving_proto_field_name=preserving_proto_field_name,
                    )
            # protobuf converts to/from b64 strings, but we prefer to stay as bytes
            elif (
                field.cpp_type == FieldDescriptor.CPPTYPE_STRING
                and field.type == FieldDescriptor.TYPE_BYTES
            ):
                spark_type = BinaryType()
            elif (
                field.cpp_type == FieldDescriptor.CPPTYPE_ENUM
                and use_integers_for_enums
            ):
                spark_type = IntegerType()
            else:
                spark_type = _CPPTYPE_TO_SPARK_TYPE_MAP[field.cpp_type]
            if field.label == FieldDescriptor.LABEL_REPEATED:
                spark_type = ArrayType(spark_type, True)
            field_name = (
                field.camelcase_name if not preserving_proto_field_name else field.name
            )
            schema.append((field_name, spark_type, True))
        struct_args = [StructField(*entry) for entry in schema]
        return StructType(fields=struct_args)


# COMMAND ----------

# MAGIC %md 
# MAGIC ### Manually define the schema

# COMMAND ----------

my_spark_schema = StructType([
  StructField('view_id', StringType(), True), 
  StructField('property_id', StringType(), True), 
  StructField('asn', IntegerType(), True), 
  StructField('browser', StringType(), True), 
  StructField('browser_version', StringType(), True), 
  StructField('cdn', StringType(), True), 
  StructField('city', StringType(), True), 
  StructField('continent_code', StringType(), True), 
  StructField('country', StringType(), True), 
  StructField('country_name', StringType(), True), 
  StructField('error_type', IntegerType(), True), 
  StructField('exit_before_video_start', BooleanType(), True), 
  StructField('experiment_name', StringType(), True), 
  StructField('latitude', DoubleType(), True), 
  StructField('longitude', DoubleType(), True), 
  StructField('max_downscale_percentage', FloatType(), True), 
  StructField('max_upscale_percentage', FloatType(), True), 
  StructField('mux_api_version', StringType(), True), 
  StructField('mux_embed_version', StringType(), True), 
  StructField('mux_viewer_id', StringType(), True), 
  StructField('operating_system', StringType(), True), 
  StructField('operating_system_version', StringType(), True), 
  StructField('page_load_time', IntegerType(), True), 
  StructField('page_url', StringType(), True), 
  StructField('playback_success_score', FloatType(), True), 
  StructField('player_autoplay', BooleanType(), True), 
  StructField('player_error_code', StringType(), True), 
  StructField('player_error_message', StringType(), True), 
  StructField('player_height', IntegerType(), True), 
  StructField('player_instance_id', StringType(), True), 
  StructField('player_language', StringType(), True), 
  StructField('player_mux_plugin_name', StringType(), True), 
  StructField('player_mux_plugin_version', StringType(), True), 
  StructField('player_name', StringType(), True), 
  StructField('player_preload', BooleanType(), True), 
  StructField('player_software', StringType(), True), 
  StructField('player_software_version', StringType(), True), 
  StructField('player_source_domain', StringType(), True), 
  StructField('player_source_duration', LongType(), True), 
  StructField('player_source_height', IntegerType(), True), 
  StructField('player_source_url', StringType(), True), 
  StructField('player_source_width', IntegerType(), True), 
  StructField('player_startup_time', IntegerType(), True), 
  StructField('player_version', StringType(), True), 
  StructField('player_view_count', IntegerType(), True), 
  StructField('player_width', IntegerType(), True), 
  StructField('rebuffer_count', IntegerType(), True), 
  StructField('rebuffer_duration', IntegerType(), True), 
  StructField('rebuffer_frequency', FloatType(), True), 
  StructField('rebuffer_percentage', FloatType(), True), 
  StructField('region', StringType(), True), 
  StructField('session_id', StringType(), True), 
  StructField('smoothness_score', FloatType(), True), 
  StructField('source_hostname', StringType(), True), 
  StructField('source_type', StringType(), True), 
  StructField('startup_time_score', FloatType(), True), 
  StructField('used_fullscreen', BooleanType(), True), 
  StructField('video_content_type', StringType(), True), 
  StructField('video_duration', IntegerType(), True), 
  StructField('video_encoding_variant', StringType(), True), 
  StructField('video_id', StringType(), True), 
  StructField('video_quality_score', FloatType(), True), 
  StructField('video_series', StringType(), True), 
  StructField('video_startup_time', IntegerType(), True), 
  StructField('video_title', StringType(), True), 
  StructField('view_max_playhead_position', LongType(), True), 
  StructField('view_playing_time', LongType(), True), 
  StructField('view_seek_count', IntegerType(), True), 
  StructField('view_seek_duration', IntegerType(), True), 
  StructField('view_session_id', StringType(), True), 
  StructField('view_total_content_playback_time', IntegerType(), True), 
  StructField('view_total_downscaling', FloatType(), True), 
  StructField('view_total_upscaling', FloatType(), True), 
  StructField('viewer_application_engine', StringType(), True), 
  StructField('viewer_connection_type', StringType(), True), 
  StructField('viewer_device_category', StringType(), True), 
  StructField('viewer_device_manufacturer', StringType(), True), 
  StructField('viewer_device_name', StringType(), True), 
  StructField('viewer_experience_score', FloatType(), True), 
  StructField('viewer_os_architecture', StringType(), True), 
  StructField('viewer_user_agent', StringType(), True), 
  StructField('viewer_user_id', StringType(), True), 
  StructField('watch_time', IntegerType(), True), 
  StructField('watched', BooleanType(), True), 
  StructField('weighted_average_bitrate', DoubleType(), True), 
  StructField('preroll_played', BooleanType(), True), 
  StructField('requests_for_first_preroll', IntegerType(), True), 
  StructField('video_startup_preroll_load_time', IntegerType(), True), 
  StructField('video_startup_preroll_request_time', IntegerType(), True), 
  StructField('stream_type', StringType(), True)
])

# COMMAND ----------

def return_flatten_dic(a_view_object) -> dict:
  # eval("a_view_object.{}".format(field))
  a_view_object_dic_perso = {
  "view_id": a_view_object.view_id,
  'property_id': a_view_object.property_id,
  'asn': a_view_object.asn,
  'browser': a_view_object.browser,
  'browser_version': a_view_object.browser_version, 
  'cdn': a_view_object.cdn,
  'city': a_view_object.city,
  'continent_code': a_view_object.continent_code,
  'country': a_view_object.country,
  'country_name': a_view_object.country_name,
  'error_type': a_view_object.error_type,
  'exit_before_video_start': a_view_object.exit_before_video_start,
  'experiment_name': a_view_object.experiment_name,
  'latitude': a_view_object.latitude,
  'longitude': a_view_object.longitude,
  'max_downscale_percentage': a_view_object.max_downscale_percentage,
  'max_upscale_percentage': a_view_object.max_upscale_percentage,
  'mux_api_version': a_view_object.mux_api_version,
  'mux_embed_version': a_view_object.mux_embed_version,
  'mux_viewer_id': a_view_object.mux_viewer_id,
  'operating_system': a_view_object.operating_system,
  'operating_system_version': a_view_object.operating_system_version,
  'page_load_time': a_view_object.page_load_time,
  'page_url': a_view_object.page_url,
  'playback_success_score': a_view_object.playback_success_score, 
  'player_autoplay': a_view_object.player_autoplay, 
  'player_error_code': a_view_object.player_error_code, 
  'player_error_message': a_view_object.player_error_message,
  'player_height': a_view_object.player_height, 
  'player_instance_id': a_view_object.player_instance_id, 
  'player_language': a_view_object.player_language, 
  'player_mux_plugin_name': a_view_object.player_mux_plugin_name, 
  'player_mux_plugin_version': a_view_object.player_mux_plugin_version,
  'player_name': a_view_object.player_name, 
  'player_preload': a_view_object.player_preload, 
  'player_software': a_view_object.player_software, 
  'player_software_version': a_view_object.player_software_version,
  'player_source_domain': a_view_object.player_source_domain,
  'player_source_duration': a_view_object.player_source_duration,
  'player_source_height': a_view_object.player_source_height,
  'player_source_url': a_view_object.player_source_url,
  'player_source_width': a_view_object.player_source_width,
  'player_startup_time': a_view_object.player_startup_time,
  'player_version': a_view_object.player_version,
  'player_view_count': a_view_object.player_view_count,
  'player_width': a_view_object.player_width,
  'rebuffer_count': a_view_object.rebuffer_count,
  'rebuffer_duration': a_view_object.rebuffer_duration,
  'rebuffer_frequency': a_view_object.rebuffer_frequency,
  'rebuffer_percentage': a_view_object.rebuffer_percentage,
  'region': a_view_object.region, 
  'session_id': a_view_object.session_id,
  'smoothness_score': a_view_object.smoothness_score,
  'source_hostname': a_view_object.source_hostname,
  'source_type': a_view_object.source_type,
  'startup_time_score': a_view_object.startup_time_score,
  'used_fullscreen': a_view_object.used_fullscreen,
  'video_content_type': a_view_object.video_content_type,
  'video_duration': a_view_object.video_duration,
  'video_encoding_variant': a_view_object.video_encoding_variant,
  'video_id': a_view_object.video_id,
  'video_quality_score': a_view_object.video_quality_score,
  'video_series': a_view_object.video_series,
  'video_startup_time': a_view_object.video_startup_time,
  'video_title': a_view_object.video_title,
  'view_max_playhead_position': a_view_object.view_max_playhead_position,
  'view_playing_time': a_view_object.view_playing_time,
  'view_seek_count': a_view_object.view_seek_count,
  'view_seek_duration': a_view_object.view_seek_duration,
  'view_session_id': a_view_object.view_session_id,
  'view_total_content_playback_time': a_view_object.view_total_content_playback_time,
  'view_total_downscaling': a_view_object.view_total_downscaling,
  'view_total_upscaling': a_view_object.view_total_upscaling,
  'viewer_application_engine': a_view_object.viewer_application_engine,
  'viewer_connection_type': a_view_object.viewer_connection_type,
  'viewer_device_category': a_view_object.viewer_device_category,
  'viewer_device_manufacturer': a_view_object.viewer_device_manufacturer,
  'viewer_device_name': a_view_object.viewer_device_name,
  'viewer_experience_score': a_view_object.viewer_experience_score,
  'viewer_os_architecture': a_view_object.viewer_os_architecture,
  'viewer_user_agent': a_view_object.viewer_user_agent,
  'viewer_user_id': a_view_object.viewer_user_id,
  'watch_time': a_view_object.watch_time,
  'watched': a_view_object.watched,
  'weighted_average_bitrate': a_view_object.weighted_average_bitrate,
  'preroll_played': a_view_object.preroll_played,
  'requests_for_first_preroll': a_view_object.requests_for_first_preroll,
  'video_startup_preroll_load_time': a_view_object.video_startup_preroll_load_time,
  'video_startup_preroll_request_time': a_view_object.video_startup_preroll_request_time,
  'stream_type': a_view_object.stream_type
  }
  return a_view_object_dic_perso

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define a function to read a protobuf file into this custom dictionary

# COMMAND ----------

# MAGIC %run /Repos/harold.nikoue@warnermedia.com/MUX_Quality_Discovery_EDA/ancillary/eventStream_pb2

# COMMAND ----------

def convert_proto_file_name_to_spark_df(file_name: str) -> SparkDataFrame:
  my_view_object = ExternalVideoViewRecord() # may put outside of function. I imagine I can reuse the same object  but maybe not since I am parallelizing the process
  # read a test file
  f = open(file_name, "rb")
  my_view_object.ParseFromString(f.read())
  # get the flattend dictionary
  flatten_view_dic = return_flatten_dic(my_view_object)
  a_list_of_values = [[v for k,v in flatten_view_dic.items()]]
  a_view_spark_df = spark.createDataFrame(a_list_of_values, my_spark_schema)
  return a_view_spark_df

# COMMAND ----------

# list my files
list_of_test_files = list(dbutils.fs.ls("FileStore/CAP/QoS_QoE/test"))
list_of_test_files = [path_record[0].replace("dbfs:/", "/dbfs/") for path_record in list_of_test_files if 'contains' in path_record[0]]
#chosen_test_file = np.random.choice(list_of_test_files)
#print(chosen_test_file)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read all files into a dataframe and merge (union) them

# COMMAND ----------

for i, chosen_test_file in enumerate(list_of_test_files):
  print(f"file ({i}): {chosen_test_file} ")
  new_spark_df = convert_proto_file_name_to_spark_df(chosen_test_file)
  if i > 0:
    mux_spark_df = mux_spark_df.union(new_spark_df)
  else:
    mux_spark_df = new_spark_df 
display(mux_spark_df)

# COMMAND ----------

display(new_spark_df)

# COMMAND ----------

chosen_test_file2 = np.random.choice([path_record[0].replace("dbfs:/", "/dbfs/") for path_record in list_of_test_files])
print(chosen_test_file2)

# COMMAND ----------

second_spark_df = convert_proto_file_name_to_spark_df(chosen_test_file2)

# COMMAND ----------

union_df = second_spark_df.union(new_spark_df)
union_df.show()

# COMMAND ----------

display(union_df)

# COMMAND ----------


