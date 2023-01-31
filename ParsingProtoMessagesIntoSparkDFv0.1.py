# Databricks notebook source
# MAGIC %md
# MAGIC # Ingesting MUX Proto Messages on Databricks
# MAGIC ## Reading proto messages into Spark Dataframe
# MAGIC The purpose of this notebook is to study different methods to ingest folders of proto messages, convert them to pyspark dataframes or RDDs and potentially save them to a Databricks compliant format (Delta) for ease of use
# MAGIC 
# MAGIC ## Current Status
# MAGIC **Can read a file into a Python object**
# MAGIC Questions:
# MAGIC 1. How do we generalize that to multiple files?
# MAGIC 2. How do we create a spark dataframe from these objects?
# MAGIC 
# MAGIC *This file was presented to the Databricks team on January 19th 2023*

# COMMAND ----------

# MAGIC %md
# MAGIC ## What are Protobufs ?
# MAGIC Protocol buffer is Google’s language neutral extensible mechanism for serializing structured data in a forward and backward-compatible ways. It means that the data can deviate from the original schema and still be read using the protocol. It also means that any point the schema may evolve, but the old configuration files will still be accessible. The protocol similarly to json or xml is used for both temporary message passing and long-term storage. The format is programming language agnostic. The .proto files are human-readable while still being to summarize complex classes and architectures. It takes less memory than json or xml but can’t really be compressed. 
# MAGIC 
# MAGIC 1.	Protobuf defines nested data structure easily
# MAGIC 2.	Protobuf doesn't constraint you to the 22 fields limit in case class (no longer true once we upgrade to 2.11+)
# MAGIC 3.	Protobuf is language agnostic and generates code that gives you native objects hence you get all the benefit of type checking and code completion unlike operating Row in Spark/SparkSQL
# MAGIC 
# MAGIC Go to	[ScalaPb Documentation](https://scalapb.github.io/docs/getting-started) for a great explanation of Protobuf files and formats

# COMMAND ----------

# MAGIC %md
# MAGIC ## What are the issues with reading Protobufs at scale?
# MAGIC 
# MAGIC Protobuf files are not natively supported by Spark.
# MAGIC a. You cannot just read an entire folder to create one dataframe
# MAGIC b. Reading a single file requires low level scala or python
# MAGIC 
# MAGIC 
# MAGIC The standard approach to read a protobuf file in Python is as follows
# MAGIC 
# MAGIC .proto file +  protoc compiler -> _pb2.py file with compiled class -> ParseFromString the content of the message file -> message data which is an object of that class
# MAGIC All the message objects still need to be converted into a dataframe
# MAGIC 
# MAGIC I have found 3 distinct libraries that would help with reading proto messages:
# MAGIC 
# MAGIC -	~~sparksql-protobuf: use spark to process parquet or RDD protos.~~ *This library is out of date and didn't work for me* 
# MAGIC -	Pbspark – convert between protobufs and spark dataframe  *For some reason the module failed with a Pickling error
# MAGIC -	Scalapb – *Most promising but in Scala*

# COMMAND ----------

# MAGIC %pip install protobuf==4.21.12
# MAGIC %pip install pbspark==0.8.0

# COMMAND ----------

import os
import sys
import numpy as np
import typing as t
from google.protobuf import json_format
from google.protobuf.json_format import MessageToJson, MessageToDict
from typing import Iterator, Dict
from pbspark import MessageConverter
from pbspark import from_protobuf
from google.protobuf.message import Message
from google.protobuf.descriptor import Descriptor
from google.protobuf.descriptor import FieldDescriptor
from google.protobuf.descriptor_pool import DescriptorPool
from google.protobuf.timestamp_pb2 import Timestamp
import inspect
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

# COMMAND ----------

# MAGIC %md
# MAGIC <span style="color:red">Direct imports doesn't work on Databricks</span>.
# MAGIC So I ended up running the python file containing the proto class

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read one file using direct method

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pick a file

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

# MAGIC %md
# MAGIC ### Convert proto message to a python object of the same message class

# COMMAND ----------

# MAGIC %run /Repos/harold.nikoue@warnermedia.com/MUX_Quality_Discovery_EDA/ancillary/eventStream_pb2

# COMMAND ----------

view_object = ExternalVideoViewRecord()

# COMMAND ----------

# read a test file
f = open(chosen_test_file, "rb")
view_object.ParseFromString(f.read())
print(view_object)

# COMMAND ----------

# I can also convert it to a dictionary
view_dic = MessageToDict(view_object)
display(view_dic)

# COMMAND ----------

view_object.DESCRIPTOR # message_type descriptor, hence view_object is a message_type of type: t.Type[Message]

# COMMAND ----------

descriptor_ = view_object.DESCRIPTOR

# COMMAND ----------

print(descriptor_.full_name)
for field in descriptor_.fields:
  print(field.message_type.full_name, field.cpp_type)

# COMMAND ----------

# I can also convert that to a json file
view_json= MessageToJson(view_object)
display(view_json)

# COMMAND ----------

# MAGIC %md
# MAGIC We have just read one file and converted that into a Python object!!!
# MAGIC What's next?
# MAGIC 1. How do we generalize that to multiple files?
# MAGIC 2. How do we create a spark dataframe from these objects?

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use PbSpark to convert the Protobuf message to Spark Dataframe
# MAGIC - Link to [PBSpark](https://pypi.org/project/pbspark/)
# MAGIC - Pbspark uses udf function to convert the serialized string (data) of the message to a dataframe 
# MAGIC - It transforms the message_type.DESCRIPTOR to a more specific schema for the dataframe

# COMMAND ----------

# create a dataframe of the encoded data without schema info
view_data = [{"value": view_object.SerializeToString()}] # this is a message
example_encoded_df = spark.createDataFrame(view_data)
display(example_encoded_df)

# COMMAND ----------

from pbspark import from_protobuf
from pbspark import to_protobuf
from pbspark import df_from_protobuf

# COMMAND ----------

df_expanded = df_from_protobuf(example_encoded_df, ExternalVideoViewRecord, expanded=True)
df_expanded.show()

# COMMAND ----------

# see pbspark documentation
mc = MessageConverter()
# There is a Pickle error.
# According to the documentation is due to the incorrect import statement
# from ancillary.from example.example_pb2 import ExternalVideoViewRecord
example_decoded_df = example_encoded_df.select(from_protobuf(example_encoded_df.value, ExternalVideoViewRecord).alias("value"))
example_expanded_df = example_decoded_df.select("value.*")
example_expanded_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use ChatGPT to convert all the files in a folder to a dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Create a script to read a proto message file and transform it into a dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.a.1 Read the proto schema (Outside of the loop)

# COMMAND ----------

# MAGIC %run /Repos/harold.nikoue@warnermedia.com/MUX_Quality_Discovery_EDA/ancillary/eventStream_pb2

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creates an object of the class desired for the message

# COMMAND ----------

view_object = ExternalVideoViewRecord()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.b. Convert Proto schema class to a Pyspark StrucType schema
# MAGIC **TO DO**

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
        return StructType(struct_args)


# COMMAND ----------

spark_schema = get_spark_schema(view_object.DESCRIPTOR)

# COMMAND ----------

spark_schema

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.c. Read Proto message and convert it to a dictionary

# COMMAND ----------

# read a test file
f = open(chosen_test_file, "rb")
view_object.ParseFromString(f.read())
#print(view_object)
view_dic = MessageToDict(view_object)
display(view_dic)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.d. Create a DataFrame from the dictionary, using the defined schema

# COMMAND ----------

# try dataframe option
df = spark.createDataFrame(view_data, spark_schema)

# COMMAND ----------

display(df)

# COMMAND ----------

view_data

# COMMAND ----------

from pbspark._timestamp import _from_datetime
from pbspark._timestamp import _to_datetime
# region serde overrides
class _Printer(json_format._Printer):  # type: ignore
    """Printer override to handle custom messages and byte fields."""

    def __init__(self, custom_serializers=None, **kwargs):
        self._custom_serializers = custom_serializers or {}
        super().__init__(**kwargs)

    def _MessageToJsonObject(self, message):
        full_name = message.DESCRIPTOR.full_name
        if full_name in self._custom_serializers:
            return self._custom_serializers[full_name](message)
        return super()._MessageToJsonObject(message)

    def _FieldToJsonObject(self, field, value):
        # specially handle bytes before protobuf's method does
        if (
            field.cpp_type == FieldDescriptor.CPPTYPE_STRING
            and field.type == FieldDescriptor.TYPE_BYTES
        ):
            return value
        # don't convert int64s to string (protobuf does this for js precision compat)
        elif field.cpp_type in json_format._INT64_TYPES:
            return value
        return super()._FieldToJsonObject(field, value)
custom_serializers = {}

def register_serializer(
        message: t.Type[Message],
        serializer: t.Callable,
        return_type: DataType,
    ):
        """Map a message type to a custom serializer and spark output type.
        The serializer should be a function which returns an object which
        can be coerced into the spark return type.
        """
        full_name = message.DESCRIPTOR.full_name
        custom_serializers[full_name] = serializer
        _message_type_to_spark_type_map[full_name] = return_type

def register_timestamp_serializer():
      register_serializer(Timestamp, _to_datetime, TimestampType())



register_timestamp_serializer()

# COMMAND ----------

# try to get a udf first
def get_decoder(
    message_type: t.Type[Message],
    including_default_value_fields: bool = False,
    preserving_proto_field_name: bool = False,
    use_integers_for_enums: bool = False,
    float_precision: t.Optional[int] = None,
) -> t.Callable:
    """Create a deserialization function for a message type.
    Create a function that accepts a serialized message bytestring
    and returns a dictionary representing the message.
    Args:
        message_type: The message type for decoding.
        including_default_value_fields: If True, singular primitive fields,
            repeated fields, and map fields will always be serialized.  If
            False, only serialize non-empty fields.  Singular message fields
            and oneof fields are not affected by this option.
        preserving_proto_field_name: If True, use the original proto field
            names as defined in the .proto file. If False, convert the field
            names to lowerCamelCase.
        use_integers_for_enums: If true, print integers instead of enum names.
        float_precision: If set, use this to specify float field valid digits.
    """

    def decoder(s: bytes) -> dict:
        if isinstance(s, bytearray):
            s = bytes(s)
        return message_to_dict(
            message_type.FromString(s),
            including_default_value_fields=including_default_value_fields,
            preserving_proto_field_name=preserving_proto_field_name,
            use_integers_for_enums=use_integers_for_enums,
            float_precision=float_precision,
        )

    return decoder

def message_to_dict(
    message: Message,
    including_default_value_fields: bool = False,
    preserving_proto_field_name: bool = False,
    use_integers_for_enums: bool = False,
    descriptor_pool: t.Optional[DescriptorPool] = None,
    float_precision: t.Optional[int] = None,
):
  """Custom MessageToDict using overridden printer.
  Args:
      message: The protocol buffers message instance to serialize.
      including_default_value_fields: If True, singular primitive fields,
          repeated fields, and map fields will always be serialized.  If
          False, only serialize non-empty fields.  Singular message fields
          and oneof fields are not affected by this option.
      preserving_proto_field_name: If True, use the original proto field
          names as defined in the .proto file. If False, convert the field
          names to lowerCamelCase.
      use_integers_for_enums: If true, print integers instead of enum names.
      descriptor_pool: A Descriptor Pool for resolving types. If None use the
          default.
      float_precision: If set, use this to specify float field valid digits.
  """
  printer = _Printer(
    custom_serializers,
    including_default_value_fields=including_default_value_fields,
    preserving_proto_field_name=preserving_proto_field_name,
    use_integers_for_enums=use_integers_for_enums,
    descriptor_pool=descriptor_pool,
    float_precision=float_precision,
  )
  return printer._MessageToJsonObject(message=message)



# COMMAND ----------

protobuf_decoder_udf = udf(get_decoder(message_type=view_object), spark_schema)

# COMMAND ----------

data = [{"value": view_object.SerializeToString()}]
df_encoded = spark.createDataFrame(data)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create a function to read a proto message file and transform it into a dataframe

# COMMAND ----------

def convert_file_to_df(file_path):
    # Function that reads a file and returns a PySpark DataFrame
    f = open(file_path, "rb")
    view_object.ParseFromString(f.read())
    view_dic = MessageToDict(view_object)
    view_data = [{"value": view_object.SerializeToString()}] # this is a message
    df = spark.createDataFrame(view_data, spark_schema)
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Create a script to apply the same function to multiple files and collect the data into one dataframe

# COMMAND ----------




sc = spark.sparkContext

# Set the folder location
folder_path = "FileStore/CAP/QoS_QoE/test"

# Read all files in the folder
files = sc.wholeTextFiles(folder_path)

# Apply the function to each file and union the resulting DataFrames
df = files.flatMap(lambda x: convert_file_to_df(x[1])).reduce(lambda x, y: x.union(y))

df.show()

# COMMAND ----------

## TODO TRY NEXT
from MyProject.Proto import MyProtoMessage
from google.protobuf.json_format import MessageToJson
import pyspark.sql.functions as F

def proto_deserialize(body):
  msg = MyProtoMessage()
  msg.ParseFromString(body)
  return MessageToJson(msg)

from_proto = F.udf(lambda s: proto_deserialize(s))

base.withColumn("content", from_proto(F.col("body")))
