CREATE TABLE [WHS].[Countries] (

	[CountryID] int NULL, 
	[CountryName] varchar(100) NULL, 
	[FormalName] varchar(100) NULL, 
	[IsoAlpha3Code] varchar(10) NULL, 
	[CountryType] varchar(100) NULL, 
	[LatestRecordedPopulation] int NULL, 
	[Continent] varchar(100) NULL, 
	[Region] varchar(100) NULL, 
	[Subregion] varchar(100) NULL, 
	[LastEditedBy] int NULL, 
	[ValidFrom] datetime2(0) NULL, 
	[ValidTo] datetime2(0) NULL
);