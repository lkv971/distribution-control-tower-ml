-- Auto Generated (Do not modify) CBFF2374BDCC0F3041B0587FB03E4FF1F4FAA48AB6959F8857E9813214CEA7BB
CREATE VIEW WHS.Staging_StockItems
AS SELECT DISTINCT
StockItemID,
StockItemName,
UnitPackageID,
OuterPackageID,
LeadTimeDays,
QuantityPerOuter,
IsChillerStock,
TaxRate,
UnitPrice,
RecommendedRetailPrice,
TypicalWeightPerUnit,
CountryOfManufacture,
ProductTags,
LastEditedBy,
ValidFrom,
ValidTo
FROM LH_Silver.dbo.WarehouseStockItems
;