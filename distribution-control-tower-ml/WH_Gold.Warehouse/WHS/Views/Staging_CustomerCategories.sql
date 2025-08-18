-- Auto Generated (Do not modify) 75FC4361DB735EE119A14CB93852839A3336E064D11C16E585C570EB9CF2E150
CREATE VIEW WHS.Staging_CustomerCategories
AS SELECT DISTINCT
CustomerCategoryID,
CustomerCategoryName,
LastEditedBy,
ValidFrom,
ValidTo
FROM LH_Silver.dbo.SalesCustomerCategories
;