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
# MAGIC Can read multipe files into a spark dataframe without using Spark udf functionality to paralellize the process
# MAGIC ## Next
# MAGIC - automate schema definition to define the schema directly from the proto class definition or the object definition
# MAGIC - automate the passing of data from s3 to dbfs based on a desired date
# MAGIC - move to v0.4
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
np.random.seed(48)
import inspect
from pprint import PrettyPrinter
from typing import Iterator, Dict, List
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

# MAGIC %md
# MAGIC ## Pick a folder to read in s3
# MAGIC ## Copy the content of this folder and subfolders to a temporary location on dbfs
# MAGIC ## Recursively read all the files in this temp folder into one dataframe

# COMMAND ----------


# all files will be copied to this directory
#dbutils.fs.mkdirs("FileStore/CAP/QoS_QoE/MUXBatchIngestionTestFiles")
dbutils.fs.ls("FileStore/CAP/QoS_QoE/MUXBatchIngestionTestFiles")
#dbutils.fs.rm("FileStore/CAP/QoS_QoE/MUXBatchIngestionTestFiles", True)

# COMMAND ----------

# copy files to new location
file_date = "2023/01/24"
# dbutils.fs.cp("s3://mux-dplusna-finalized-view-data/" + file_date, "FileStore/CAP/QoS_QoE/MUXBatchIngestionTestFiles",True)

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
        list_of_field_names: List[str],
        preserving_proto_field_name: bool = False,
        use_integers_for_enums: bool = False,
    ) -> DataType:
        """Generate a spark schema from a message type or descriptor
        Given a message type generated from protoc (or its descriptor),
        create a spark schema derived from the protobuf schema when
        serializing with ``MessageToDict``.
        Args:
            descriptor: A message type or its descriptor
            list_of_field_names: A list of all fields to be retained
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
                        list_of_field_names= list_of_field_names,
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
            if field_name not in list_of_field_names:
              continue
            if field_name in ['view_start', 'view_end']:
              schema.append((field_name, IntegerType(), True))
            else:
              schema.append((field_name, spark_type, True))
        struct_args = [StructField(*entry) for entry in schema]
        return StructType(fields=struct_args)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Load a test file and use it to derive the schema

# COMMAND ----------

# MAGIC %run /Repos/harold.nikoue@warnermedia.com/MUX_Quality_Discovery_EDA/ancillary/eventStream_pb2

# COMMAND ----------

view_object = ExternalVideoViewRecord()
list_of_test_files = list(dbutils.fs.ls("FileStore/CAP/QoS_QoE/test"))
chosen_test_file = np.random.choice([path_record[0].replace("dbfs:/", "/dbfs/") for path_record in list_of_test_files])
#print(chosen_test_file)
# read a test file
f = open(chosen_test_file, "rb")
view_object.ParseFromString(f.read())
#print(view_object)
view_dic = MessageToDict(view_object)

# COMMAND ----------

import re
pattern = re.compile(r'(?<!^)(?=[A-Z])')
list_of_keys = list(view_dic.keys())
underscore_list_of_keys = [pattern.sub('_', name).lower() for name in list_of_keys]
underscore_list_of_keys = [re.sub(r'custom(\d)', r'custom_\1', item_name) for item_name in underscore_list_of_keys]

# COMMAND ----------

view_object.view_start

# COMMAND ----------

list_of_non_event_columns = [item_name for item_name in underscore_list_of_keys if item_name != 'events']

# COMMAND ----------

list_of_non_event_columns

# COMMAND ----------

flat_dic = {key: value for key, value in view_dic.items() if key!= 'events'}

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Automatically define the schema

# COMMAND ----------

def return_flatten_dic_short_fun(a_view_object) -> dict:
  a_view_object_dict_perso = {}
  for item_name in list_of_non_event_columns:
    expression_str = 'a_view_object.{}'.format(item_name)
    #print(expression_str)
    a_view_object_dict_perso[item_name] = eval(expression_str)
  return a_view_object_dict_perso

# COMMAND ----------

def return_list_of_view_object_values(a_view_object) -> dict:
  list_of_values = []
  for item_name in list_of_non_event_columns:
    if item_name in ['view_start', 'view_end']:
      expression_str = 'a_view_object.{}.seconds'.format(item_name)
    else:
      expression_str = 'a_view_object.{}'.format(item_name)
    list_of_values.append(eval(expression_str))
  return [list_of_values]

# COMMAND ----------

short_flatten_list_of_values = return_list_of_view_object_values(view_object)

# COMMAND ----------

spark_schema = get_spark_schema(view_object.DESCRIPTOR, list_of_non_event_columns, True)

# COMMAND ----------

spark_schema.fieldNames()

# COMMAND ----------

len(short_flatten_list_of_values[0])

# COMMAND ----------

len(spark_schema)

# COMMAND ----------

my_view_df = spark.createDataFrame(short_flatten_list_of_values, spark_schema)
display(my_view_df)

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
  my_spark_schema = get_spark_schema(my_view_object.DESCRIPTOR, list_of_non_event_columns, True)
  my_view_object.ParseFromString(f.read())
  # get the flattend dictionary
  a_list_of_values = return_list_of_view_object_values(my_view_object)
  a_view_spark_df = spark.createDataFrame(a_list_of_values, my_spark_schema)
  return a_view_spark_df

# COMMAND ----------

def get_dir_content(ls_path):
  dir_paths = dbutils.fs.ls(ls_path)
  subdir_paths = [get_dir_content(p.path) for p in dir_paths if p.isDir() and p.path != ls_path]
  flat_subdir_paths = [p for subdir in subdir_paths for p in subdir]
  return list(map(lambda p: p.path, dir_paths)) + flat_subdir_paths

# COMMAND ----------

def get_sampled_dir_content(ls_path, prop=0.25):
  """
  Read a sample of each subdirectory so that you don't have to wait for ever to get the data needed
  """
  dir_paths = dbutils.fs.ls(ls_path)
  subdir_paths = [get_dir_content(p.path) for p in dir_paths if p.isDir() and p.path != ls_path]
  flat_subdir_paths = [p for subdir in subdir_paths for p in np.random.choice(subdir, int(prop * len(subdir)))]
  return list(map(lambda p: p.path, dir_paths)) + flat_subdir_paths

# COMMAND ----------

# list my files
list_of_all_files = get_dir_content("FileStore/CAP/QoS_QoE/MUXBatchIngestionTestFiles")
list_of_all_sampled_files = get_sampled_dir_content("FileStore/CAP/QoS_QoE/MUXBatchIngestionTestFiles", prop=0.05)
list_of_test_files = [path_record.replace("dbfs:/", "/dbfs/") for path_record in list_of_all_sampled_files if 'discovery-mux-dplusna' in path_record]
#chosen_test_file = np.random.choice(list_of_test_files)
#print(chosen_test_file)

# COMMAND ----------

num_files = len(list_of_test_files)
print("There are {} files to ingest out of {}".format(num_files, len(list_of_all_files)))
print(" 20% of flights is {}".format(0.2 * len(list_of_all_files)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read all files into a dataframe and merge (union) them

# COMMAND ----------

#reduced_list = np.random.choice(list_of_test_files, int(0.25 * num_files) )
for i, chosen_test_file in enumerate(list_of_test_files):
  print(f"file ({i}): {chosen_test_file} ")
  new_spark_df = convert_proto_file_name_to_spark_df(chosen_test_file)
  if i > 0:
    mux_spark_df = mux_spark_df.union(new_spark_df)
  else:
    mux_spark_df = new_spark_df 
print(f"{i + 1} files were read")


# COMMAND ----------

display(mux_spark_df.take(10))

# COMMAND ----------

# save the table somwhere safe
file_date = "2023/01/24"
dbutils.fs.mkdirs("/FileStore/CAP/QoS_QoE/MUXBatchTable/")
file_to_be_saved = "/FileStore/CAP/QoS_QoE/MUXBatchTable/" + file_date.replace("/", '_')
print("file to be saved: {}".format(file_to_be_saved))
mux_spark_df.write.format("delta").mode("overwrite").save(file_to_be_saved)
#Below we are listing the data in destination path
display(dbutils.fs.ls("/FileStore/tables/delta_train/"))

# COMMAND ----------

# save to data table 
file_date = "2023/01/24"
table_name = "mux_daily_sampled_data" + file_date.replace("/", '_')
mux_spark_df.write.saveAsTable(table_name)

# COMMAND ----------

print((mux_spark_df.count(), len(mux_spark_df.columns)))

# COMMAND ----------

display(spark.sql(f'DESCRIBE DETAIL {table_name}'))

# COMMAND ----------

# Read a table
mux_delta_table_df = spark.read.table(table_name)
display(mux_delta_table
