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

lastModified = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from notebookutils import mssparkutils
import json

payload = json.dumps({"lastModified": lastModified}, indent=2)

folder = "Files/watermarks"
file   = f"{folder}/Watermark.json"

mssparkutils.fs.mkdirs(folder)
mssparkutils.fs.put(file, payload, overwrite=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
