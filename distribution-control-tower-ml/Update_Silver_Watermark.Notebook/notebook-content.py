# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "eeca8fd3-76a3-4c12-929b-e93d065aae10",
# META       "default_lakehouse_name": "LH_Silver",
# META       "default_lakehouse_workspace_id": "6260829b-8914-43e1-bae0-f1defd01461c",
# META       "known_lakehouses": [
# META         {
# META           "id": "eeca8fd3-76a3-4c12-929b-e93d065aae10"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from notebookutils import mssparkutils
from pyspark.sql.functions import max as spark_max
import json
from datetime import datetime, date

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

workspace_id = mssparkutils.notebook.entrywidget("workspace_id")
silver_lakehouse_id = mssparkutils.notebook.entrywidget("silver_lakehouse_id")
watermark_path = mssparkutils.notebook.entrywidget("watermark_path")
environment = mssparkutils.notebook.entrywidget("environment")
current_time = mssparkutils.notebook.entrywidget("current_time")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


try:
    folder_path = "/".join(watermark_path.split("/")[:-1])
    file_name = watermark_path.split("/")[-1]
    
    watermark_content = f'{{"environment": "{environment}", "lastModified": "{current_time}"}}'
    
    mssparkutils.fs.put(
        f"abfss://Files@{silver_lakehouse_id}.dfs.fabric.microsoft.com/{watermark_path}",
        watermark_content,
        overwrite=True
    )
    
    print(f"SILVER WATERMARK UPDATED FOR {environment.upper()} ENVIRONMENT:")
    print(f"Path: abfss://Files@{silver_lakehouse_id}.dfs.fabric.microsoft.com/{watermark_path}")
    print(f"Content: {watermark_content}")
    
    updated_content = mssparkutils.fs.text.read(
        f"abfss://Files@{silver_lakehouse_id}.dfs.fabric.microsoft.com/{watermark_path}"
    )
    print(f"Verified watermark content: {updated_content}")
    
except Exception as e:
    print(f"ERROR UPDATING SILVER WATERMARK: {str(e)}")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
