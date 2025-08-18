-- Auto Generated (Do not modify) B8EEDE97295A4E4FEB5D3621D28AC051E673BFB1E3918FF91E969B1819CB7BA0
CREATE VIEW WHS.Staging_StockItemHoldings 
AS SELECT DISTINCT
StockItemID,
QuantityOnHand,
BinLocation,
LastStocktakeQuantity,
LastCostPrice,
ReorderLevel,
TargetStockLevel,
LastEditedBy,
LastEditedWhen
FROM LH_Silver.dbo.WarehouseStockItemHoldings
;