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

TABLE = "SalesOrders"  

row = (spark.read.table(TABLE)
             .select(spark_max("OrderDate").alias("LastOrderDate"))
             .first())
val = row["LastOrderDate"] if row else None

if isinstance(val, datetime):
    lastModified = val.strftime("%Y-%m-%dT%H:%M:%S") + "Z"
elif isinstance(val, date):
    lastModified = f"{val.isoformat()}T00:00:00Z"
else:
    lastModified = "1970-01-01T00:00:00Z"

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
