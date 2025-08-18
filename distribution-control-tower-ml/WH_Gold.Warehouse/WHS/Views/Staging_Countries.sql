-- Auto Generated (Do not modify) BE701A647A9B33349C42E0209D401DEAD0E14BE8FD5D21C733D19C8AA8147D6A
CREATE VIEW WHS.Staging_Countries
AS SELECT DISTINCT
CountryID,
CountryName,
FormalName,
IsoAlpha3Code,
CountryType,
LatestRecordedPopulation,
Continent,
Region,
Subregion,
LastEditedBy,
ValidFrom,
ValidTo
FROM LH_Silver.dbo.ApplicationCountries
;