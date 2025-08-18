-- Auto Generated (Do not modify) 95B4B883DAFC6281D988211546360D3536B94E2EBEA987864E5B31B177993289
CREATE VIEW WHS.Staging_PackageTypes
AS SELECT DISTINCT
PackageTypeID,
PackageTypeName,
LastEditedBy,
ValidFrom,
ValidTo
FROM LH_Silver.dbo.WarehousePackageTypes
;