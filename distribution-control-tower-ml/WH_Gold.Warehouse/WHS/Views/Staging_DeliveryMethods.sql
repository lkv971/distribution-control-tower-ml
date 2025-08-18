-- Auto Generated (Do not modify) 7FF82B27119858075A8523009FB2124D9F1CD6C08B4003E1624A2F4B6556D523
CREATE VIEW WHS.Staging_DeliveryMethods
AS SELECT DISTINCT
DeliveryMethodID,
DeliveryMethodName,
LastEditedBy,
ValidFrom,
ValidTo
FROM LH_Silver.dbo.ApplicationDeliveryMethods
;