CREATE TABLE [WHS].[Customers] (

	[CustomerID] int NULL, 
	[CustomerName] varchar(100) NULL, 
	[BillToCustomerID] int NULL, 
	[CustomerCategoryID] int NULL, 
	[PrimaryContactPersonID] int NULL, 
	[DeliveryMethodID] int NULL, 
	[DeliveryCityID] int NULL, 
	[CreditLimit] float NULL, 
	[AccountOpenedDate] date NULL, 
	[StandardDiscountPercentage] float NULL, 
	[PaymentDays] int NULL, 
	[PhoneNumber] varchar(20) NULL, 
	[FaxNumber] varchar(20) NULL, 
	[DeliveryAddressLine1] varchar(200) NULL, 
	[DeliveryAddressLine2] varchar(200) NULL, 
	[DeliveryPostalCode] varchar(100) NULL, 
	[PostalAddressLine1] varchar(200) NULL, 
	[LastEditedBy] int NULL, 
	[ValidFrom] datetime2(0) NULL, 
	[ValidTo] datetime2(0) NULL
);