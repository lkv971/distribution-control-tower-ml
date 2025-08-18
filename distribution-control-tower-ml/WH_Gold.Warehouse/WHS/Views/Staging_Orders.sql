-- Auto Generated (Do not modify) EB74BB6A15D5D230556A79E1A0714762F34E03AA370F27D5DE77EB791C364A3C
CREATE VIEW WHS.Staging_Orders
AS SELECT DISTINCT
OrderID,
CustomerID,
SalespersonPersonID,
PickedByPersonID,
ContactPersonID,
OrderDate,
ExpectedDeliveryDate,
PickingCompletedWhen,
LastEditedBy,
LastEditedWhen
FROM LH_Silver.dbo.SalesOrders
;