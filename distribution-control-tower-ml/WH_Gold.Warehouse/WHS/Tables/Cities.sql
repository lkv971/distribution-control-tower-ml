CREATE TABLE [WHS].[Cities] (

	[CityID] int NULL, 
	[CityName] varchar(100) NULL, 
	[Longitude] float NULL, 
	[Latitude] float NULL, 
	[StateProvinceID] int NULL, 
	[LatestRecordedPopulation] int NULL, 
	[LastEditedBy] int NULL, 
	[ValidFrom] datetime2(0) NULL, 
	[ValidTo] datetime2(0) NULL
);