-- Auto Generated (Do not modify) 882F1B96A3C3430C249B48FC2F5D894E11C9FDAD0B2A1309A9F8726B54A58E82
CREATE VIEW WHS.Staging_OrderLines 
AS SELECT DISTINCT
OrderLineID,
OrderID,
StockItemID,
Description,
PackageTypeID,
Quantity,
UnitPrice,
TaxRate,
PickedQuantity,
PickingCompletedWhen,
LastEditedBy,
LastEditedWhen
FROM LH_Silver.dbo.SalesOrderLines
;