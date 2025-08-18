CREATE TABLE [WHS].[StockItems] (

	[StockItemID] int NULL, 
	[StockItemName] varchar(100) NULL, 
	[UnitPackageID] int NULL, 
	[OuterPackageID] int NULL, 
	[LeadTimeDays] int NULL, 
	[QuantityPerOuter] int NULL, 
	[IsChillerStock] varchar(50) NULL, 
	[TaxRate] float NULL, 
	[UnitPrice] float NULL, 
	[RecommendedRetailPrice] float NULL, 
	[TypicalWeightPerUnit] float NULL, 
	[CountryOfManufacture] varchar(100) NULL, 
	[ProductTags] varchar(100) NULL, 
	[LastEditedBy] int NULL, 
	[ValidFrom] datetime2(0) NULL, 
	[ValidTo] datetime2(0) NULL
);