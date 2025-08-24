# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "533579e9-d31a-499a-ac78-9af0b1582937",
# META       "default_lakehouse_name": "LH_Bronze",
# META       "default_lakehouse_workspace_id": "6260829b-8914-43e1-bae0-f1defd01461c",
# META       "known_lakehouses": [
# META         {
# META           "id": "533579e9-d31a-499a-ac78-9af0b1582937"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from notebookutils import mssparkutils
import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

workspace_id = mssparkutils.notebook.entrywidget("workspace_id")
bronze_lakehouse_id = mssparkutils.notebook.entrywidget("bronze_lakehouse_id")
watermark_path = mssparkutils.notebook.entrywidget("watermark_path")
environment = mssparkutils.notebook.entrywidget("environment")
current_time = mssparkutils.notebook.entrywidget("current_time")
last_modified = mssparkutils.notebook.entrywidget("last_modified")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    folder_path = "/".join(watermark_path.split("/")[:-1])
    file_name = watermark_path.split("/")[-1]
    
    watermark_content = f'{{"environment": "{environment}", "lastModified": "{last_modified}"}}'
    
    mssparkutils.fs.put(
        f"abfss://Files@{bronze_lakehouse_id}.dfs.fabric.microsoft.com/{watermark_path}",
        watermark_content,
        overwrite=True
    )
    
    print(f"BRONZE WATERMARK UPDATED FOR {environment.upper()} ENVIRONMENT:")
    print(f"Path: abfss://Files@{bronze_lakehouse_id}.dfs.fabric.microsoft.com/{watermark_path}")
    print(f"Content: {watermark_content}")
    
    updated_content = mssparkutils.fs.text.read(
        f"abfss://Files@{bronze_lakehouse_id}.dfs.fabric.microsoft.com/{watermark_path}"
    )
    print(f"Verified watermark content: {updated_content}")
    
except Exception as e:
    print(f"ERROR UPDATING BRONZE WATERMARK: {str(e)}")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
