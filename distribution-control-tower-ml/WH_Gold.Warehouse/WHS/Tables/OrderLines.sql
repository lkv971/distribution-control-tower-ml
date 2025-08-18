CREATE TABLE [WHS].[OrderLines] (

	[OrderLineID] int NULL, 
	[OrderID] int NULL, 
	[StockItemID] int NULL, 
	[Description] varchar(200) NULL, 
	[PackageTypeID] int NULL, 
	[Quantity] int NULL, 
	[UnitPrice] float NULL, 
	[TaxRate] float NULL, 
	[PickedQuantity] int NULL, 
	[PickingCompletedWhen] datetime2(0) NULL, 
	[LastEditedBy] int NULL, 
	[LastEditedWhen] datetime2(0) NULL
);