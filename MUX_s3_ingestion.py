# Databricks notebook source
# MAGIC %pip install pbspark

# COMMAND ----------

import os
import sys
import pbspark
import numpy as np

# COMMAND ----------

sys.path.append(os.path.abspath("/Repos/harold.nikoue@warnermedia.com/MUX_Quality_Discovery_EDA/ancillary"))

# COMMAND ----------

print(sys.path)

# COMMAND ----------

# MAGIC %run /Repos/harold.nikoue@warnermedia.com/MUX_Quality_Discovery_EDA/ancillary/eventStream_pb2

# COMMAND ----------

# read one file
files_on_one_day = dbutils.fs.ls("s3://mux-dplusna-finalized-view-data/2023/01/10")
files_in_one_hour = list(dbutils.fs.ls(files_on_one_day[20].path))


# COMMAND ----------

# read one file at random
list_of_full_session_paths = [session_path_record[0] for session_path_record in files_in_one_hour]

# COMMAND ----------

selected_file = np.random.choice(list_of_full_session_paths)

# COMMAND ----------

selected_file

# COMMAND ----------

from pbspark import from_protobuf
from pbspark import to_protobuf

# COMMAND ----------


