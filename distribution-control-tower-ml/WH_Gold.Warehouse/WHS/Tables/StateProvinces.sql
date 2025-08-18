CREATE TABLE [WHS].[StateProvinces] (

	[StateProvinceID] int NULL, 
	[StateProvinceCode] varchar(10) NULL, 
	[StateProvinceName] varchar(100) NULL, 
	[CountryID] int NULL, 
	[SalesTerritory] varchar(100) NULL, 
	[LatestRecordedPopulation] int NULL, 
	[LastEditedBy] int NULL, 
	[ValidFrom] datetime2(0) NULL, 
	[ValidTo] datetime2(0) NULL
);