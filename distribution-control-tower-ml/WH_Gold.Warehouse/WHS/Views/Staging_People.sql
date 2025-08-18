-- Auto Generated (Do not modify) 4DEE39006BFD3A38C26D8D20D3F96686A38C983B9FA92535F5503A7165E641C2
CREATE VIEW WHS.Staging_People
AS SELECT DISTINCT
PersonID,
FullName,
PreferredName,
SearchName,
IsPermittedToLogon,
LogonName,
IsExternalLogonProvider,
IsSystemUser,
IsEmployee,
IsSalesperson,
PhoneNumber,
FaxNumber,
EmailAddress,
LastEditedBy,
ValidFrom,
ValidTo
FROM LH_Silver.dbo.ApplicationPeople
;