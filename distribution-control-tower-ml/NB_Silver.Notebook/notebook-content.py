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

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_stockitemholdings = spark.read.table("LH_Silver.WarehouseStockItemHoldings")
df_stockitems = spark.read.table("LH_Silver.WarehouseStockItems")
df_deliverymethods = spark.read.table("LH_Silver.ApplicationDeliveryMethods")
df_packagetypes =  spark.read.table("LH_Silver.WarehousePackageTypes")
df_cities = spark.read.table("LH_Silver.ApplicationCities")
df_countries = spark.read.table("LH_Silver.ApplicationCountries")
df_provinces = spark.read.table("LH_Silver.ApplicationStateProvinces")
df_people = spark.read.table("LH_Silver.ApplicationPeople")
df_customers = spark.read.table("LH_Silver.SalesCustomers")
df_customercategories = spark.read.table("LH_Silver.SalesCustomerCategories")
df_invoices = spark.read.table("LH_Silver.SalesInvoices")
df_orders = spark.read.table("LH_Silver.SalesOrders")
df_orderlines = spark.read.table("LH_Silver.SalesOrderLines")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_customers = df_customers.withColumn("CreditLimit", col("CreditLimit").cast(DecimalType(18,2))) \
                           .withColumn("StandardDiscountPercentage", col("StandardDiscountPercentage").cast(DecimalType()))

df_orderlines = df_orderlines.withColumn("UnitPrice", col("UnitPrice").cast(DecimalType(18,2))) \
                             .withColumn("TaxRate", col("TaxRate").cast(DecimalType(9,4)))

df_stockitemholdings = df_stockitemholdings.withColumn("LastCostPrice", col("LastCostPrice").cast(DecimalType(18,4)))

df_stockitems = df_stockitems.withColumn("TaxRate", col("TaxRate").cast(DecimalType(9,4))) \
                             .withColumn("UnitPrice", col("UnitPrice").cast(DecimalType(18,2))) \
                             .withColumn("RecommendedRetailPrice", col("RecommendedRetailPrice").cast(DecimalType(18,2))) \
                             .withColumn("TypicalWeightPerUnit", col("TypicalWeightPerUnit").cast(DecimalType(18,3)))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

start_date = "20100101"
end_date   = "20401231"

df_date_range = spark.createDataFrame([(start_date, end_date)], ["start_date", "end_date"])
df_dates = df_date_range.select(
    explode(sequence(to_date(col("start_date"), "yyyyMMdd"),
                     to_date(col("end_date"), "yyyyMMdd"),
                     expr("interval 1 day"))).alias("Date"))

window_dates = Window.orderBy("Date")

df_dates = df_dates.withColumn("Year", year(col("Date"))) \
                   .withColumn("DateID", row_number().over(window_dates)) \
                   .withColumn("Month", month(col("Date"))) \
                   .withColumn("Day", dayofmonth(col("Date"))) \
                   .withColumn("MonthName", date_format(col("Date"), "MMMM")) \
                   .withColumn("DayName", date_format(col("Date"), "EEEE")) \
                   .select("DateID", "Date", "Year", "Month", "Day", "MonthName", "DayName") 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_stockitemholdings = df_stockitemholdings.dropDuplicates(subset = ["StockItemID"])
df_stockitems = df_stockitems.dropDuplicates(subset = ["StockItemID"])
df_deliverymethods = df_deliverymethods.dropDuplicates(subset = ["DeliveryMethodID"])
df_packagetypes =  df_packagetypes.dropDuplicates(subset = ["PackageTypeID"])
df_cities = df_cities.dropDuplicates(subset = ["CityID"])
df_countries = df_countries.dropDuplicates(subset = ["CountryID"])
df_provinces = df_provinces.dropDuplicates(subset = ["StateProvinceID"])
df_people = df_people.dropDuplicates(subset = ["PersonID"])
df_customers = df_customers.dropDuplicates(subset = ["CustomerID"])
df_customercategories = df_customercategories.dropDuplicates(subset = ["CustomerCategoryID"])
df_invoices = df_invoices.dropDuplicates(subset = ["InvoiceID"])
df_orders = df_orders.dropDuplicates(subset = ["OrderID"])
df_orderlines = df_orderlines.dropDuplicates(subset = ["OrderLineID"])
df_dates = df_dates.dropDuplicates(subset = ["DateID"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

overwrite_tables = {
    "WarehouseStockItems": df_stockitems,
    "ApplicationDeliveryMethods": df_deliverymethods,
    "WarehousePackageTypes": df_packagetypes,
    "ApplicationCities": df_cities,
    "ApplicationCountries": df_countries,
    "ApplicationStateProvinces": df_provinces,
    "ApplicationPeople": df_people,
    "SalesCustomers": df_customers,
    "SalesCustomerCategories": df_customercategories,
}

append_tables = {
    "WarehouseStockItemHoldings": df_stockitemholdings,
    "SalesInvoices": df_invoices,
    "SalesOrders": df_orders,
    "SalesOrderLines": df_orderlines,
    "Dates": df_dates
}

fact_key_cols = {
    "SalesInvoices": ["InvoiceID", "InvoiceDate", "CustomerID", "OrderID", "DeliveryMethodID", "ContactPersonID", "AccountsPersonID", "PackedByPersonID"],
    "SalesOrders": ["OrderID", "CustomerID", "SalespersonPersonID", "PickedByPersonID", "OrderDate"],
    "SalesOrderLines": ["OrderLineID", "OrderID", "StockItemID", "PackageTypeID"],
    "WarehouseStockItemHoldings": ["StockItemID"],
    "Dates": ["DateID", "Date"]
}

for table_name, overwrite_df in overwrite_tables.items():
    try:
        overwrite_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{table_name}")
        print(f"Table {table_name} overwritten successfully.")
    except Exception as e:
        print(f"Error overwriting table {table_name}: {e}")

def make_merge_condition(keys):
    return " AND ".join([f"t.{col} = s.{col}" for col in keys])

for table_name, append_df in append_tables.items():
    keys = fact_key_cols.get(table_name)
    if not keys:
        raise ValueError(f"No business key defined for table {table_name}")

    merge_condition = make_merge_condition(keys)

    try:
        target = DeltaTable.forName(spark, table_name)
        (target.alias("t").merge(append_df.alias("s"), merge_condition).whenMatchedUpdateAll().execute())
        (target.alias("t").merge(append_df.alias("s"), merge_condition).whenNotMatchedInsertAll().execute())
        print(f"Upsert completed for '{table_name}' using key columns {keys}")
    except Exception as e:
        if "is not a Delta table" in e.desc:
            append_df.write.mode("overwrite").saveAsTable(f"{table_name}")
            print(f"Created new Delta table {table_name}")
        else:
            print(f"Error upserting '{table_name}': {e}")

df = spark.sql("SELECT * FROM LH_Silver.warehousepackagetypes LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
