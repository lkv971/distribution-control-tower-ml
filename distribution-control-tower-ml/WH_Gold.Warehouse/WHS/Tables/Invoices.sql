CREATE TABLE [WHS].[Invoices] (

	[InvoiceID] int NULL, 
	[CustomerID] int NULL, 
	[BillToCustomerID] int NULL, 
	[OrderID] int NULL, 
	[DeliveryMethodID] int NULL, 
	[ContactPersonID] int NULL, 
	[AccountsPersonID] int NULL, 
	[SalespersonPersonID] int NULL, 
	[PackedByPersonID] int NULL, 
	[InvoiceDate] date NULL, 
	[IsCreditNote] varchar(50) NULL, 
	[DeliveryInstructions] varchar(500) NULL, 
	[TotalDryItems] int NULL, 
	[TotalChillerItems] int NULL, 
	[ConfirmedDeliveryTime] datetime2(0) NULL, 
	[ConfirmedReceivedBy] varchar(100) NULL, 
	[LastEditedBy] int NULL, 
	[LastEditedWhen] datetime2(0) NULL
);