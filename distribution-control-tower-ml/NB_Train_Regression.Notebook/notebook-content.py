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

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import mlflow

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_orders = spark.read.table("LH_Silver.SalesOrders")
df_orderlines = spark.read.table("LH_Silver.SalesOrderLines")
df_customers = spark.read.table("LH_Silver.salescustomers")
df_customercategories = spark.read.table("LH_Silver.salescustomercategories")

df =  df_customercategories.join(df_customers, on = "CustomerCategoryID", how = "inner") \
                           .join(df_orders, on = "CustomerID", how = "inner") \
                           .join(df_orderlines, on = "OrderID", how = "inner") \
                           .filter(col("CustomerCategoryName") == "Novelty Shop")
                          
df = df.withColumn("YearMonth", to_date(date_trunc("month", col("OrderDate")))) \
       .withColumn("TotalSales", col("Quantity") * col("UnitPrice")) 


df = df.groupBy("YearMonth").agg(sum("TotalSales").alias("SalesM"),
                                 sum("Quantity").alias("UnitsM"),
                                 countDistinct("OrderID").alias("OrdersM")) \
       .orderBy("YearMonth") 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bounds = df.agg(min("YearMonth").alias("start"),
                max("YearMonth").alias("end")).first()
start, end = bounds["start"], bounds["end"]

month_spine = (spark.createDataFrame([(start, end)], ["start","end"]).select(expr("sequence(start, end, interval 1 month) AS m")).select(explode("m").alias("YearMonth"))
)

missing = (month_spine.join(df.select("YearMonth").distinct(), on="YearMonth", how="left_anti"))
print(f"Expected months: {month_spine.count()}, present: {df.select('YearMonth').distinct().count()}, missing: {missing.count()}")

df = month_spine.join(df, on="YearMonth", how="left").fillna({"SalesM": 0.0, "UnitsM": 0.0, "OrdersM": 0}).orderBy("YearMonth")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

w = Window.orderBy("YearMonth")
w_ma3 = w.rowsBetween(2, 0)
w_ma6 = w.rowsBetween(5, 0)
w_ma12 = w.rowsBetween(11, 0)

df_train = df.withColumn("SalesLag1", lag(col("SalesM"), 1).over(w)) \
             .withColumn("SalesLag3", lag(col("SalesM"), 3).over(w)) \
             .withColumn("SalesLag6", lag(col("SalesM"), 6).over(w)) \
             .withColumn("SalesLag12", lag(col("SalesM"), 12).over(w)) \
             .withColumn("SalesMA3", avg("SalesM").over(w_ma3)) \
             .withColumn("SalesMA6", avg("SalesM").over(w_ma6)) \
             .withColumn("SalesMA12", avg("SalesM").over(w_ma12)) \
             .withColumn("Year", year(col("YearMonth"))) \
             .withColumn("Month", month(col("YearMonth"))) \
             .withColumn("MonthSin", sin(2 * pi() * col("Month") / lit(12))) \
             .withColumn("MonthCos", cos(2 * pi() * col("Month") / lit(12))) \
             .withColumn("YearMonthKey", year(col("YearMonth")) * 100 + month(col("YearMonth"))) \
             .withColumn("SalesNextMonth", lead(col("SalesM"), 1).over(w)) \
             .withColumn("AverageOrderValueM", when(col("OrdersM") > 0, col("SalesM")/ col("OrdersM")).otherwise(lit(0.0))) \
             .withColumn("AverageUnitPriceM", when(col("UnitsM") > 0, col("SalesM")/ col("UnitsM")).otherwise(lit(0.0)))

cols_needed = ["YearMonth","Year","Month","YearMonthKey",
               "SalesM", "UnitsM","OrdersM",
               "SalesLag1","SalesLag3", "SalesLag6", "SalesLag12",
               "SalesMA3","SalesMA6", "SalesMA12", 
               "AverageOrderValueM", "AverageUnitPriceM",
               "MonthCos", "MonthSin",
               "SalesNextMonth"]

df_train = df_train.select(cols_needed).dropna(subset=cols_needed)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pdf = df_train.toPandas().sort_values("YearMonth").reset_index(drop=True)

X_cols = ["Year","Month", "MonthCos", "MonthSin", "SalesM", "UnitsM","OrdersM","SalesLag1","SalesLag3","SalesLag6","SalesLag12","SalesMA3","SalesMA6","SalesMA12", "AverageOrderValueM", "AverageUnitPriceM"]
X = pdf[X_cols].values
y = pdf["SalesNextMonth"].values

split_idx = int(len(pdf) * 0.80) 

print(f"Training on {split_idx} months, testing on {len(pdf)-split_idx} months")

X_train, X_test = X[:split_idx], X[split_idx:]
y_train, y_test = y[:split_idx], y[split_idx:]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

experiment_name = "next-month-sales-regression"
mlflow.set_experiment(experiment_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

with mlflow.start_run(run_name = "Baseline (Lag1)"):
    y_pred = X_test[:, X_cols.index("SalesLag1")]
    mlflow.log_param("estimator", "Baseline-Lag1")
    mlflow.log_metric("r2_test",  float(r2_score(y_test, y_pred)))
    mlflow.log_metric("rmse_test", float(mean_squared_error(y_test, y_pred, squared=False)))
    mlflow.log_metric("mae_test",  float(mean_absolute_error(y_test, y_pred)))

with mlflow.start_run(run_name = "Baseline (Lag12)"):
    y_pred = X_test[:, X_cols.index("SalesLag12")]
    mlflow.log_param("estimator", "Baseline-Lag12")
    mlflow.log_metric("r2_test",  float(r2_score(y_test, y_pred)))
    mlflow.log_metric("rmse_test", float(mean_squared_error(y_test, y_pred, squared=False)))
    mlflow.log_metric("mae_test",  float(mean_absolute_error(y_test, y_pred)))

with mlflow.start_run(run_name = "LinearRegression"):
     mlflow.autolog()
     model = LinearRegression()
     model.fit(X_train, y_train)
     y_pred = model.predict(X_test)
     mlflow.log_param("estimator", "LinearRegression")
     mlflow.log_metric("r2_test",  float(r2_score(y_test, y_pred)))
     mlflow.log_metric("rmse_test", float(mean_squared_error(y_test, y_pred, squared=False)))
     mlflow.log_metric("mae_test",  float(mean_absolute_error(y_test, y_pred)))

with mlflow.start_run(run_name = "DecisionTreeRegressor"):
     mlflow.autolog()
     model = DecisionTreeRegressor(max_depth = 3, random_state = 42)
     model.fit(X_train, y_train)
     y_pred = model.predict(X_test)
     mlflow.log_param("estimator", "DecisionTreeRegressor")
     mlflow.log_metric("r2_test",  float(r2_score(y_test, y_pred)))
     mlflow.log_metric("rmse_test", float(mean_squared_error(y_test, y_pred, squared=False)))
     mlflow.log_metric("mae_test",  float(mean_absolute_error(y_test, y_pred)))

with mlflow.start_run(run_name = "RandomForestRegressor"):
    mlflow.autolog()
    rf = RandomForestRegressor(n_estimators=200, min_samples_leaf=5, min_samples_split=10, max_features="sqrt", random_state=42)
    rf.fit(X_train, y_train)
    y_pred = rf.predict(X_test)
    mlflow.log_param("estimator", "RandomForestRegressor")
    mlflow.log_metric("r2_test",  float(r2_score(y_test, y_pred)))
    mlflow.log_metric("rmse_test", float(mean_squared_error(y_test, y_pred, squared=False)))
    mlflow.log_metric("mae_test",  float(mean_absolute_error(y_test, y_pred)))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"Supervised rows: {len(pdf)} | Train: {len(y_train)} | Test: {len(y_test)}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
