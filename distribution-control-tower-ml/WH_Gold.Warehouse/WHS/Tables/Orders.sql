CREATE TABLE [WHS].[Orders] (

	[OrderID] int NULL, 
	[CustomerID] int NULL, 
	[SalespersonPersonID] int NULL, 
	[PickedByPersonID] int NULL, 
	[ContactPersonID] int NULL, 
	[OrderDate] date NULL, 
	[ExpectedDeliveryDate] date NULL, 
	[PickingCompletedWhen] datetime2(0) NULL, 
	[LastEditedBy] int NULL, 
	[LastEditedWhen] datetime2(0) NULL
);