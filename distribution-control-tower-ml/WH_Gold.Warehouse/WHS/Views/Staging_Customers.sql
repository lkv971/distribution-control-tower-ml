-- Auto Generated (Do not modify) 756957B4118F490366127372869D8E6D2258964D80BF99A9A3DCA42F330AFE4C
CREATE VIEW WHS.Staging_Customers
AS SELECT DISTINCT
CustomerID,
CustomerName,
BillToCustomerID,
CustomerCategoryID,
PrimaryContactPersonID,
DeliveryMethodID,
DeliveryCityID,
CreditLimit,
AccountOpenedDate,
StandardDiscountPercentage,
PaymentDays,
PhoneNumber,
FaxNumber,
DeliveryAddressLine1,
DeliveryAddressLine2,
DeliveryPostalCode,
PostalAddressLine1,
LastEditedBy,
ValidFrom,
ValidTo
FROM LH_Silver.dbo.SalesCustomers
;