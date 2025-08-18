CREATE TABLE [WHS].[StockItemHoldings] (

	[StockItemID] int NULL, 
	[QuantityOnHand] int NULL, 
	[BinLocation] varchar(50) NULL, 
	[LastStocktakeQuantity] int NULL, 
	[LastCostPrice] float NULL, 
	[ReorderLevel] int NULL, 
	[TargetStockLevel] int NULL, 
	[LastEditedBy] int NULL, 
	[LastEditedWhen] datetime2(0) NULL
);