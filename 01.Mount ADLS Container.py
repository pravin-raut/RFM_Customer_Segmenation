# Databricks notebook source
import os


# COMMAND ----------


def monunt_container(containerName,storageName):
    dbutils.fs.mount(
        source="wasbs://"+containerName+"@"+storageName+".blob.core.windows.net",
        mount_point="/mnt/"+containerName,
        extra_configs={
        "fs.azure.sas."+containerName+"."+storageName+".blob.core.windows.net":dbutils.secrets.get(scope="RFMContainer", key="RFMSource")
    }

)


# COMMAND ----------

if os.path.exists("/mnt/rfm"):
    print("Container already mounted")
else:
    monunt_container('rfm','demosstorage007')
