-- Auto Generated (Do not modify) 0D626479B059116C5035399FF353B85B53506111E6087BE9EC9A42DDA29BB72F
CREATE VIEW WHS.Staging_Invoices 
AS SELECT DISTINCT
InvoiceID,
CustomerID,
BillToCustomerID,
OrderID,
DeliveryMethodID,
ContactPersonID,
AccountsPersonID,
SalespersonPersonID,
PackedByPersonID,
InvoiceDate,
IsCreditNote,
DeliveryInstructions,
TotalDryItems,
TotalChillerItems,
ConfirmedDeliveryTime,
ConfirmedReceivedBy,
LastEditedBy,
LastEditedWhen
FROM LH_Silver.dbo.SalesInvoices
;