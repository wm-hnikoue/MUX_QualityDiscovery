# Databricks notebook source
# MAGIC %md
# MAGIC ## Protobuf Connector Demo with Schema Registry 
# MAGIC 
# MAGIC #### Outline
# MAGIC This notebook iuncludes an end-to-end example of using builtin Protobuf functions to work with Protobuf messages. The schema is read from a schema-registry.  
# MAGIC 
# MAGIC #### Protobuf Schema
# MAGIC The example below uses the the following Protobuf to define an 'AppEvent':
# MAGIC  
# MAGIC     syntax = "proto3"; // "proto2" is also supported. 
# MAGIC     
# MAGIC     message AppEvent {
# MAGIC         string name = 1;
# MAGIC         int64 id = 2;
# MAGIC         string context = 3;  
# MAGIC     }

# COMMAND ----------

# MAGIC %md
# MAGIC #### Setup: In-Memory Schema Registry
# MAGIC 
# MAGIC The following section adds `AppEvent` Protobuf to an in-memory schema-registry. The details can be safely ignored.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // Set up Protobuf in an in memory schema registry 
# MAGIC 
# MAGIC import java.util.Arrays;
# MAGIC import java.util.Collections;
# MAGIC import org.sparkproject.connector.protobuf.confluent.kafka.schemaregistry._
# MAGIC import scala.concurrent.duration.DurationInt
# MAGIC 
# MAGIC def registerProtobuf(): Int = {
# MAGIC     val inMemoryClient = client.SchemaRegistryClientFactory.newClient(
# MAGIC         Arrays.asList("mock://protobuf-demo"),
# MAGIC         10,
# MAGIC         Arrays.asList(new protobuf.ProtobufSchemaProvider()),
# MAGIC         Collections.emptyMap(),
# MAGIC         Collections.emptyMap()
# MAGIC     )
# MAGIC     val protobufStr = """
# MAGIC         | syntax = "proto3"; // "proto2" is also supported.
# MAGIC         | 
# MAGIC         | message AppEvent {
# MAGIC         |     string name = 1;
# MAGIC         |     int64 id = 2;
# MAGIC         |     string context = 3;  
# MAGIC         | }
# MAGIC     """.stripMargin
# MAGIC 
# MAGIC     inMemoryClient.register("app-events-value", new protobuf.ProtobufSchema(protobufStr))
# MAGIC }
# MAGIC 
# MAGIC val schemaIdOnDriver = registerProtobuf()
# MAGIC 
# MAGIC println(s"=== Schema Id: ${schemaIdOnDriver} ===")
# MAGIC 
# MAGIC spark.sparkContext.runOnEachExecutor(() => {
# MAGIC     val schemaIdOnExecutor = registerProtobuf()
# MAGIC     assert(schemaIdOnExecutor == schemaIdOnDriver)
# MAGIC }, 60.seconds)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Display documentation (includes complete examples)
# MAGIC Use Python's built in `help()` to display documentation for Protobuf functions. The documentation includes complete example using schema-registry. 
# MAGIC 
# MAGIC See the following sections from `help(from_protobuf)` below.
# MAGIC   * Descripton of schema registry options in `Parameters` section.
# MAGIC   * Complete example under `Schema Registry Example`.

# COMMAND ----------

from pyspark.sql.protobuf.functions import to_protobuf, from_protobuf

help(from_protobuf)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Prepare Input DataFrame with 3 records
# MAGIC This is used to demo `to_protobuf()`. Each row has 3 fields correpsonding to the fields in the above protobuf.

# COMMAND ----------

from pyspark.sql.types import Row

input_rows = [
  Row(name = "Click", id = 1, context = "Profile"),
  Row(name = "Notification", id = 2, context = "New Friend"),
  Row(name = "Render", id = 3, context = "Leader Board"),
]

input_df = spark.createDataFrame(input_rows)
input_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Demo `to_protobuf()`
# MAGIC Convert `input_df` to Protobuf binary records using `to_protobuf()`. The binary data includes schema-registry header followed by AppEvent Protobuf. These records can be written to a Kafka topic.

# COMMAND ----------

# Provide needed options to fetch schema from schema-registry. 
schema_registry_options = {
  "schema.registry.subject" : "app-events-value",
  "schema.registry.address" : "mock://protobuf-demo", # In memory schema-registry. In production, it is something like https://schema-registry:8081/
}

# Create protobuf binaries using to_protobuf()

protobuf_binary_df = (
  input_df
    .selectExpr("struct(name, id, context) as event")
    .select(
      to_protobuf("event", options = schema_registry_options)
        .alias("proto_bytes") 
    )
)
protobuf_binary_df.printSchema()

# COMMAND ----------

# Materialize above DataFrame

protobuf_binary_rows = protobuf_binary_df.collect()
protobuf_binary_rows

# COMMAND ----------

# MAGIC %md
# MAGIC #### Demo `from_protobuf()`
# MAGIC Process binary data in `protobuf_binary_rows` using `from_protobuf()`. In practice this binary data is read from a message bus like Kafka.
# MAGIC 
# MAGIC Prints the schema and the actual events.

# COMMAND ----------

proto_events_df = (
    spark
        .createDataFrame(protobuf_binary_rows)
        .select(
            from_protobuf("proto_bytes", options = schema_registry_options)
                .alias("proto_event")
        )
)
proto_events_df.printSchema() # Schema matches the Protobuf definition.

# COMMAND ----------

# Print the events.
proto_events_df.select("proto_event.*").show()
