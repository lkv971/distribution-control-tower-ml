-- Auto Generated (Do not modify) 9C061E0D1BA9C0E6528394DC62EBA43DC53C250505BC15E07DE1FCCBB15C035D
CREATE VIEW WHS.Staging_StateProvinces
AS SELECT DISTINCT
StateProvinceID,
StateProvinceCode,
StateProvinceName,
CountryID,
SalesTerritory,
LatestRecordedPopulation,
LastEditedBy,
ValidFrom,
ValidTo
FROM LH_Silver.dbo.ApplicationStateProvinces
;