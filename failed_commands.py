# Databricks notebook source
# Mount s3
mount_name = 'mux_s3_random_day'
dbutils.fs.mount("s3a://mux-dplusna-finalized-view-data/2023/01/10", f'/mnt/{mount_name}')
dbutils.fs.ls(f'/mnt/{mount_name}')

# COMMAND ----------

# choose a test file
selected_hour = 20
list_of_test_files = list(dbutils.fs.ls('/mnt/{0}/{1:02d}'.format(mount_name, selected_hour)))
mnt_test_file = np.random.choice([path_record[0].replace("dbfs:/", "/dbfs/") for path_record in list_of_test_files])
print(mnt_test_file)
# THIS DOESN'T WORKs
