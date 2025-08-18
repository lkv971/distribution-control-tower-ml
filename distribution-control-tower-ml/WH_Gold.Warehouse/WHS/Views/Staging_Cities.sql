-- Auto Generated (Do not modify) F9F30AA7C218D826E79DEDC62F6A663D27FDF5E75ECB6DF96E81408629054C1C
CREATE View WHS.Staging_Cities
AS SELECT DISTINCT
CityID,
CityName,
Longitude,
Latitude,
StateProvinceID,
LatestRecordedPopulation,
LastEditedBy,
ValidFrom,
ValidTo
FROM LH_Silver.dbo.ApplicationCities
;