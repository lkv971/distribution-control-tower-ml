CREATE TABLE [WHS].[People] (

	[PersonID] int NULL, 
	[FullName] varchar(100) NULL, 
	[PreferredName] varchar(100) NULL, 
	[SearchName] varchar(100) NULL, 
	[IsPermittedToLogon] varchar(10) NULL, 
	[LogonName] varchar(100) NULL, 
	[IsExternalLogonProvider] varchar(10) NULL, 
	[IsSystemUser] varchar(10) NULL, 
	[IsEmployee] varchar(10) NULL, 
	[IsSalesperson] varchar(10) NULL, 
	[PhoneNumber] varchar(20) NULL, 
	[FaxNumber] varchar(20) NULL, 
	[EmailAddress] varchar(100) NULL, 
	[LastEditedBy] int NULL, 
	[ValidFrom] datetime2(0) NULL, 
	[ValidTo] datetime2(0) NULL
);