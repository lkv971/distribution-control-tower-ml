CREATE PROCEDURE WHS.sp_Upsert_Simple
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Now     DATETIME2(3) = SYSUTCDATETIME();
    DECLARE @MaxDate DATETIME2(3) = '9999-12-31 23:59:59.997';
    DECLARE @NowPrev DATETIME2(3) = DATEADD(millisecond, -1, @Now);

    UPDATE d
    SET d.ValidTo = @NowPrev
    FROM WHS.Countries d
    JOIN WHS.Staging_Countries s ON d.CountryID = s.CountryID
    WHERE d.ValidTo = @MaxDate AND (
        ISNULL(d.CountryName,'') <> ISNULL(s.CountryName,'') OR
        ISNULL(d.FormalName,'') <> ISNULL(s.FormalName,'') OR
        ISNULL(d.IsoAlpha3Code,'') <> ISNULL(s.IsoAlpha3Code,'') OR
        ISNULL(d.CountryType,'') <> ISNULL(s.CountryType,'') OR
        ISNULL(d.LatestRecordedPopulation,0) <> ISNULL(s.LatestRecordedPopulation,0) OR
        ISNULL(d.Continent,'') <> ISNULL(s.Continent,'') OR
        ISNULL(d.Region,'') <> ISNULL(s.Region,'') OR
        ISNULL(d.Subregion,'') <> ISNULL(s.Subregion,'')
    );

    INSERT INTO WHS.Countries (
        CountryID, CountryName, FormalName, IsoAlpha3Code, CountryType,
        LatestRecordedPopulation, Continent, Region, Subregion,
        LastEditedBy, ValidFrom, ValidTo)
    SELECT s.CountryID, s.CountryName, s.FormalName, s.IsoAlpha3Code, s.CountryType,
           s.LatestRecordedPopulation, s.Continent, s.Region, s.Subregion,
           s.LastEditedBy, @Now, @MaxDate
    FROM WHS.Staging_Countries s
    JOIN WHS.Countries d ON d.CountryID = s.CountryID AND d.ValidTo = @MaxDate
    WHERE (
        ISNULL(d.CountryName,'') <> ISNULL(s.CountryName,'') OR
        ISNULL(d.FormalName,'') <> ISNULL(s.FormalName,'') OR
        ISNULL(d.IsoAlpha3Code,'') <> ISNULL(s.IsoAlpha3Code,'') OR
        ISNULL(d.CountryType,'') <> ISNULL(s.CountryType,'') OR
        ISNULL(d.LatestRecordedPopulation,0) <> ISNULL(s.LatestRecordedPopulation,0) OR
        ISNULL(d.Continent,'') <> ISNULL(s.Continent,'') OR
        ISNULL(d.Region,'') <> ISNULL(s.Region,'') OR
        ISNULL(d.Subregion,'') <> ISNULL(s.Subregion,'')
    );

    INSERT INTO WHS.Countries (
        CountryID, CountryName, FormalName, IsoAlpha3Code, CountryType,
        LatestRecordedPopulation, Continent, Region, Subregion,
        LastEditedBy, ValidFrom, ValidTo)
    SELECT s.CountryID, s.CountryName, s.FormalName, s.IsoAlpha3Code, s.CountryType,
           s.LatestRecordedPopulation, s.Continent, s.Region, s.Subregion,
           s.LastEditedBy, @Now, @MaxDate
    FROM WHS.Staging_Countries s
    WHERE NOT EXISTS (SELECT 1 FROM WHS.Countries d WHERE d.CountryID = s.CountryID);

    UPDATE d
    SET d.ValidTo = @NowPrev
    FROM WHS.StateProvinces d
    JOIN WHS.Staging_StateProvinces s ON d.StateProvinceID = s.StateProvinceID
    WHERE d.ValidTo = @MaxDate AND (
        ISNULL(d.StateProvinceCode,'') <> ISNULL(s.StateProvinceCode,'') OR
        ISNULL(d.StateProvinceName,'') <> ISNULL(s.StateProvinceName,'') OR
        ISNULL(d.CountryID,-1) <> ISNULL(s.CountryID,-1) OR
        ISNULL(d.SalesTerritory,'') <> ISNULL(s.SalesTerritory,'') OR
        ISNULL(d.LatestRecordedPopulation,0) <> ISNULL(s.LatestRecordedPopulation,0)
    );

    INSERT INTO WHS.StateProvinces (
        StateProvinceID, StateProvinceCode, StateProvinceName, CountryID,
        SalesTerritory, LatestRecordedPopulation, LastEditedBy, ValidFrom, ValidTo)
    SELECT s.StateProvinceID, s.StateProvinceCode, s.StateProvinceName, s.CountryID,
           s.SalesTerritory, s.LatestRecordedPopulation, s.LastEditedBy, @Now, @MaxDate
    FROM WHS.Staging_StateProvinces s
    JOIN WHS.StateProvinces d ON d.StateProvinceID = s.StateProvinceID AND d.ValidTo = @MaxDate
    WHERE (
        ISNULL(d.StateProvinceCode,'') <> ISNULL(s.StateProvinceCode,'') OR
        ISNULL(d.StateProvinceName,'') <> ISNULL(s.StateProvinceName,'') OR
        ISNULL(d.CountryID,-1) <> ISNULL(s.CountryID,-1) OR
        ISNULL(d.SalesTerritory,'') <> ISNULL(s.SalesTerritory,'') OR
        ISNULL(d.LatestRecordedPopulation,0) <> ISNULL(s.LatestRecordedPopulation,0)
    );

    INSERT INTO WHS.StateProvinces (
        StateProvinceID, StateProvinceCode, StateProvinceName, CountryID,
        SalesTerritory, LatestRecordedPopulation, LastEditedBy, ValidFrom, ValidTo)
    SELECT s.StateProvinceID, s.StateProvinceCode, s.StateProvinceName, s.CountryID,
           s.SalesTerritory, s.LatestRecordedPopulation, s.LastEditedBy, @Now, @MaxDate
    FROM WHS.Staging_StateProvinces s
    WHERE NOT EXISTS (SELECT 1 FROM WHS.StateProvinces d WHERE d.StateProvinceID = s.StateProvinceID);

    UPDATE d
    SET d.ValidTo = @NowPrev
    FROM WHS.Cities d
    JOIN WHS.Staging_Cities s ON d.CityID = s.CityID
    WHERE d.ValidTo = @MaxDate AND (
        ISNULL(d.CityName,'') <> ISNULL(s.CityName,'') OR
        ISNULL(d.Longitude,0.0) <> ISNULL(s.Longitude,0.0) OR
        ISNULL(d.Latitude,0.0) <> ISNULL(s.Latitude,0.0) OR
        ISNULL(d.StateProvinceID,-1) <> ISNULL(s.StateProvinceID,-1) OR
        ISNULL(d.LatestRecordedPopulation,0) <> ISNULL(s.LatestRecordedPopulation,0)
    );

    INSERT INTO WHS.Cities (
        CityID, CityName, Longitude, Latitude, StateProvinceID, LatestRecordedPopulation,
        LastEditedBy, ValidFrom, ValidTo)
    SELECT s.CityID, s.CityName, s.Longitude, s.Latitude, s.StateProvinceID, s.LatestRecordedPopulation,
           s.LastEditedBy, @Now, @MaxDate
    FROM WHS.Staging_Cities s
    JOIN WHS.Cities d ON d.CityID = s.CityID AND d.ValidTo = @MaxDate
    WHERE (
        ISNULL(d.CityName,'') <> ISNULL(s.CityName,'') OR
        ISNULL(d.Longitude,0.0) <> ISNULL(s.Longitude,0.0) OR
        ISNULL(d.Latitude,0.0) <> ISNULL(s.Latitude,0.0) OR
        ISNULL(d.StateProvinceID,-1) <> ISNULL(s.StateProvinceID,-1) OR
        ISNULL(d.LatestRecordedPopulation,0) <> ISNULL(s.LatestRecordedPopulation,0)
    );

    INSERT INTO WHS.Cities (
        CityID, CityName, Longitude, Latitude, StateProvinceID, LatestRecordedPopulation,
        LastEditedBy, ValidFrom, ValidTo)
    SELECT s.CityID, s.CityName, s.Longitude, s.Latitude, s.StateProvinceID, s.LatestRecordedPopulation,
           s.LastEditedBy, @Now, @MaxDate
    FROM WHS.Staging_Cities s
    WHERE NOT EXISTS (SELECT 1 FROM WHS.Cities d WHERE d.CityID = s.CityID);

    UPDATE d
    SET d.ValidTo = @NowPrev
    FROM WHS.DeliveryMethods d
    JOIN WHS.Staging_DeliveryMethods s ON d.DeliveryMethodID = s.DeliveryMethodID
    WHERE d.ValidTo = @MaxDate AND ISNULL(d.DeliveryMethodName,'') <> ISNULL(s.DeliveryMethodName,'');

    INSERT INTO WHS.DeliveryMethods (DeliveryMethodID, DeliveryMethodName, LastEditedBy, ValidFrom, ValidTo)
    SELECT s.DeliveryMethodID, s.DeliveryMethodName, s.LastEditedBy, @Now, @MaxDate
    FROM WHS.Staging_DeliveryMethods s
    JOIN WHS.DeliveryMethods d ON d.DeliveryMethodID = s.DeliveryMethodID AND d.ValidTo = @MaxDate
    WHERE ISNULL(d.DeliveryMethodName,'') <> ISNULL(s.DeliveryMethodName,'');

    INSERT INTO WHS.DeliveryMethods (DeliveryMethodID, DeliveryMethodName, LastEditedBy, ValidFrom, ValidTo)
    SELECT s.DeliveryMethodID, s.DeliveryMethodName, s.LastEditedBy, @Now, @MaxDate
    FROM WHS.Staging_DeliveryMethods s
    WHERE NOT EXISTS (SELECT 1 FROM WHS.DeliveryMethods d WHERE d.DeliveryMethodID = s.DeliveryMethodID);

    UPDATE d
    SET d.ValidTo = @NowPrev
    FROM WHS.People d
    JOIN WHS.Staging_People s ON d.PersonID = s.PersonID
    WHERE d.ValidTo = @MaxDate AND (
        ISNULL(d.FullName,'') <> ISNULL(s.FullName,'') OR
        ISNULL(d.PreferredName,'') <> ISNULL(s.PreferredName,'') OR
        ISNULL(d.SearchName,'') <> ISNULL(s.SearchName,'') OR
        ISNULL(d.IsPermittedToLogon,'') <> ISNULL(s.IsPermittedToLogon,'') OR
        ISNULL(d.LogonName,'') <> ISNULL(s.LogonName,'') OR
        ISNULL(d.IsExternalLogonProvider,'') <> ISNULL(s.IsExternalLogonProvider,'') OR
        ISNULL(d.IsSystemUser,'') <> ISNULL(s.IsSystemUser,'') OR
        ISNULL(d.IsEmployee,'') <> ISNULL(s.IsEmployee,'') OR
        ISNULL(d.IsSalesperson,'') <> ISNULL(s.IsSalesperson,'') OR
        ISNULL(d.PhoneNumber,'') <> ISNULL(s.PhoneNumber,'') OR
        ISNULL(d.FaxNumber,'') <> ISNULL(s.FaxNumber,'') OR
        ISNULL(d.EmailAddress,'') <> ISNULL(s.EmailAddress,'')
    );

    INSERT INTO WHS.People (
        PersonID, FullName, PreferredName, SearchName, IsPermittedToLogon, LogonName,
        IsExternalLogonProvider, IsSystemUser, IsEmployee, IsSalesperson,
        PhoneNumber, FaxNumber, EmailAddress,
        LastEditedBy, ValidFrom, ValidTo)
    SELECT s.PersonID, s.FullName, s.PreferredName, s.SearchName, s.IsPermittedToLogon, s.LogonName,
           s.IsExternalLogonProvider, s.IsSystemUser, s.IsEmployee, s.IsSalesperson,
           s.PhoneNumber, s.FaxNumber, s.EmailAddress,
           s.LastEditedBy, @Now, @MaxDate
    FROM WHS.Staging_People s
    JOIN WHS.People d ON d.PersonID = s.PersonID AND d.ValidTo = @MaxDate
    WHERE (
        ISNULL(d.FullName,'') <> ISNULL(s.FullName,'') OR
        ISNULL(d.PreferredName,'') <> ISNULL(s.PreferredName,'') OR
        ISNULL(d.SearchName,'') <> ISNULL(s.SearchName,'') OR
        ISNULL(d.IsPermittedToLogon,'') <> ISNULL(s.IsPermittedToLogon,'') OR
        ISNULL(d.LogonName,'') <> ISNULL(s.LogonName,'') OR
        ISNULL(d.IsExternalLogonProvider,'') <> ISNULL(s.IsExternalLogonProvider,'') OR
        ISNULL(d.IsSystemUser,'') <> ISNULL(s.IsSystemUser,'') OR
        ISNULL(d.IsEmployee,'') <> ISNULL(s.IsEmployee,'') OR
        ISNULL(d.IsSalesperson,'') <> ISNULL(s.IsSalesperson,'') OR
        ISNULL(d.PhoneNumber,'') <> ISNULL(s.PhoneNumber,'') OR
        ISNULL(d.FaxNumber,'') <> ISNULL(s.FaxNumber,'') OR
        ISNULL(d.EmailAddress,'') <> ISNULL(s.EmailAddress,'')
    );

    INSERT INTO WHS.People (
        PersonID, FullName, PreferredName, SearchName, IsPermittedToLogon, LogonName,
        IsExternalLogonProvider, IsSystemUser, IsEmployee, IsSalesperson,
        PhoneNumber, FaxNumber, EmailAddress,
        LastEditedBy, ValidFrom, ValidTo)
    SELECT s.PersonID, s.FullName, s.PreferredName, s.SearchName, s.IsPermittedToLogon, s.LogonName,
           s.IsExternalLogonProvider, s.IsSystemUser, s.IsEmployee, s.IsSalesperson,
           s.PhoneNumber, s.FaxNumber, s.EmailAddress,
           s.LastEditedBy, @Now, @MaxDate
    FROM WHS.Staging_People s
    WHERE NOT EXISTS (SELECT 1 FROM WHS.People d WHERE d.PersonID = s.PersonID);


    UPDATE d
    SET d.ValidTo = @NowPrev
    FROM WHS.CustomerCategories d
    JOIN WHS.Staging_CustomerCategories s ON d.CustomerCategoryID = s.CustomerCategoryID
    WHERE d.ValidTo = @MaxDate AND ISNULL(d.CustomerCategoryName,'') <> ISNULL(s.CustomerCategoryName,'');

    INSERT INTO WHS.CustomerCategories (CustomerCategoryID, CustomerCategoryName, LastEditedBy, ValidFrom, ValidTo)
    SELECT s.CustomerCategoryID, s.CustomerCategoryName, s.LastEditedBy, @Now, @MaxDate
    FROM WHS.Staging_CustomerCategories s
    JOIN WHS.CustomerCategories d ON d.CustomerCategoryID = s.CustomerCategoryID AND d.ValidTo = @MaxDate
    WHERE ISNULL(d.CustomerCategoryName,'') <> ISNULL(s.CustomerCategoryName,'');

    INSERT INTO WHS.CustomerCategories (CustomerCategoryID, CustomerCategoryName, LastEditedBy, ValidFrom, ValidTo)
    SELECT s.CustomerCategoryID, s.CustomerCategoryName, s.LastEditedBy, @Now, @MaxDate
    FROM WHS.Staging_CustomerCategories s
    WHERE NOT EXISTS (SELECT 1 FROM WHS.CustomerCategories d WHERE d.CustomerCategoryID = s.CustomerCategoryID);

    UPDATE d
    SET d.ValidTo = @NowPrev
    FROM WHS.Customers d
    JOIN WHS.Staging_Customers s ON d.CustomerID = s.CustomerID
    WHERE d.ValidTo = @MaxDate AND (
        ISNULL(d.CustomerName,'') <> ISNULL(s.CustomerName,'') OR
        ISNULL(d.BillToCustomerID,-1) <> ISNULL(s.BillToCustomerID,-1) OR
        ISNULL(d.CustomerCategoryID,-1) <> ISNULL(s.CustomerCategoryID,-1) OR
        ISNULL(d.PrimaryContactPersonID,-1) <> ISNULL(s.PrimaryContactPersonID,-1) OR
        ISNULL(d.DeliveryMethodID,-1) <> ISNULL(s.DeliveryMethodID,-1) OR
        ISNULL(d.DeliveryCityID,-1) <> ISNULL(s.DeliveryCityID,-1) OR
        ISNULL(d.CreditLimit,0.0) <> ISNULL(s.CreditLimit,0.0) OR
        ISNULL(d.AccountOpenedDate,'19000101') <> ISNULL(s.AccountOpenedDate,'19000101') OR
        ISNULL(d.StandardDiscountPercentage,0.0) <> ISNULL(s.StandardDiscountPercentage,0.0) OR
        ISNULL(d.PaymentDays,0) <> ISNULL(s.PaymentDays,0) OR
        ISNULL(d.PhoneNumber,'') <> ISNULL(s.PhoneNumber,'') OR
        ISNULL(d.FaxNumber,'') <> ISNULL(s.FaxNumber,'') OR
        ISNULL(d.DeliveryAddressLine1,'') <> ISNULL(s.DeliveryAddressLine1,'') OR
        ISNULL(d.DeliveryAddressLine2,'') <> ISNULL(s.DeliveryAddressLine2,'') OR
        ISNULL(d.DeliveryPostalCode,'') <> ISNULL(s.DeliveryPostalCode,'') OR
        ISNULL(d.PostalAddressLine1,'') <> ISNULL(s.PostalAddressLine1,'')
    );

    INSERT INTO WHS.Customers (
        CustomerID, CustomerName, BillToCustomerID, CustomerCategoryID, PrimaryContactPersonID,
        DeliveryMethodID, DeliveryCityID, CreditLimit, AccountOpenedDate,
        StandardDiscountPercentage, PaymentDays, PhoneNumber, FaxNumber,
        DeliveryAddressLine1, DeliveryAddressLine2, DeliveryPostalCode, PostalAddressLine1,
        LastEditedBy, ValidFrom, ValidTo)
    SELECT s.CustomerID, s.CustomerName, s.BillToCustomerID, s.CustomerCategoryID, s.PrimaryContactPersonID,
           s.DeliveryMethodID, s.DeliveryCityID, s.CreditLimit, s.AccountOpenedDate,
           s.StandardDiscountPercentage, s.PaymentDays, s.PhoneNumber, s.FaxNumber,
           s.DeliveryAddressLine1, s.DeliveryAddressLine2, s.DeliveryPostalCode, s.PostalAddressLine1,
           s.LastEditedBy, @Now, @MaxDate
    FROM WHS.Staging_Customers s
    JOIN WHS.Customers d ON d.CustomerID = s.CustomerID AND d.ValidTo = @MaxDate
    WHERE (
        ISNULL(d.CustomerName,'') <> ISNULL(s.CustomerName,'') OR
        ISNULL(d.BillToCustomerID,-1) <> ISNULL(s.BillToCustomerID,-1) OR
        ISNULL(d.CustomerCategoryID,-1) <> ISNULL(s.CustomerCategoryID,-1) OR
        ISNULL(d.PrimaryContactPersonID,-1) <> ISNULL(s.PrimaryContactPersonID,-1) OR
        ISNULL(d.DeliveryMethodID,-1) <> ISNULL(s.DeliveryMethodID,-1) OR
        ISNULL(d.DeliveryCityID,-1) <> ISNULL(s.DeliveryCityID,-1) OR
        ISNULL(d.CreditLimit,0.0) <> ISNULL(s.CreditLimit,0.0) OR
        ISNULL(d.AccountOpenedDate,'19000101') <> ISNULL(s.AccountOpenedDate,'19000101') OR
        ISNULL(d.StandardDiscountPercentage,0.0) <> ISNULL(s.StandardDiscountPercentage,0.0) OR
        ISNULL(d.PaymentDays,0) <> ISNULL(s.PaymentDays,0) OR
        ISNULL(d.PhoneNumber,'') <> ISNULL(s.PhoneNumber,'') OR
        ISNULL(d.FaxNumber,'') <> ISNULL(s.FaxNumber,'') OR
        ISNULL(d.DeliveryAddressLine1,'') <> ISNULL(s.DeliveryAddressLine1,'') OR
        ISNULL(d.DeliveryAddressLine2,'') <> ISNULL(s.DeliveryAddressLine2,'') OR
        ISNULL(d.DeliveryPostalCode,'') <> ISNULL(s.DeliveryPostalCode,'') OR
        ISNULL(d.PostalAddressLine1,'') <> ISNULL(s.PostalAddressLine1,'')
    );

    INSERT INTO WHS.Customers (
        CustomerID, CustomerName, BillToCustomerID, CustomerCategoryID, PrimaryContactPersonID,
        DeliveryMethodID, DeliveryCityID, CreditLimit, AccountOpenedDate,
        StandardDiscountPercentage, PaymentDays, PhoneNumber, FaxNumber,
        DeliveryAddressLine1, DeliveryAddressLine2, DeliveryPostalCode, PostalAddressLine1,
        LastEditedBy, ValidFrom, ValidTo)
    SELECT s.CustomerID, s.CustomerName, s.BillToCustomerID, s.CustomerCategoryID, s.PrimaryContactPersonID,
           s.DeliveryMethodID, s.DeliveryCityID, s.CreditLimit, s.AccountOpenedDate,
           s.StandardDiscountPercentage, s.PaymentDays, s.PhoneNumber, s.FaxNumber,
           s.DeliveryAddressLine1, s.DeliveryAddressLine2, s.DeliveryPostalCode, s.PostalAddressLine1,
           s.LastEditedBy, @Now, @MaxDate
    FROM WHS.Staging_Customers s
    WHERE NOT EXISTS (SELECT 1 FROM WHS.Customers d WHERE d.CustomerID = s.CustomerID);

    UPDATE d
    SET d.ValidTo = @NowPrev
    FROM WHS.PackageTypes d
    JOIN WHS.Staging_PackageTypes s ON d.PackageTypeID = s.PackageTypeID
    WHERE d.ValidTo = @MaxDate AND ISNULL(d.PackageTypeName,'') <> ISNULL(s.PackageTypeName,'');

    INSERT INTO WHS.PackageTypes (PackageTypeID, PackageTypeName, LastEditedBy, ValidFrom, ValidTo)
    SELECT s.PackageTypeID, s.PackageTypeName, s.LastEditedBy, @Now, @MaxDate
    FROM WHS.Staging_PackageTypes s
    JOIN WHS.PackageTypes d ON d.PackageTypeID = s.PackageTypeID AND d.ValidTo = @MaxDate
    WHERE ISNULL(d.PackageTypeName,'') <> ISNULL(s.PackageTypeName,'');

    INSERT INTO WHS.PackageTypes (PackageTypeID, PackageTypeName, LastEditedBy, ValidFrom, ValidTo)
    SELECT s.PackageTypeID, s.PackageTypeName, s.LastEditedBy, @Now, @MaxDate
    FROM WHS.Staging_PackageTypes s
    WHERE NOT EXISTS (SELECT 1 FROM WHS.PackageTypes d WHERE d.PackageTypeID = s.PackageTypeID);

    UPDATE d
    SET d.ValidTo = @NowPrev
    FROM WHS.StockItems d
    JOIN WHS.Staging_StockItems s ON d.StockItemID = s.StockItemID
    WHERE d.ValidTo = @MaxDate AND (
        ISNULL(d.StockItemName,'') <> ISNULL(s.StockItemName,'') OR
        ISNULL(d.UnitPackageID,-1) <> ISNULL(s.UnitPackageID,-1) OR
        ISNULL(d.OuterPackageID,-1) <> ISNULL(s.OuterPackageID,-1) OR
        ISNULL(d.LeadTimeDays,0) <> ISNULL(s.LeadTimeDays,0) OR
        ISNULL(d.QuantityPerOuter,0) <> ISNULL(s.QuantityPerOuter,0) OR
        ISNULL(d.IsChillerStock,'') <> ISNULL(s.IsChillerStock,'') OR
        ISNULL(d.TaxRate,0.0) <> ISNULL(s.TaxRate,0.0) OR
        ISNULL(d.UnitPrice,0.0) <> ISNULL(s.UnitPrice,0.0) OR
        ISNULL(d.RecommendedRetailPrice,0.0) <> ISNULL(s.RecommendedRetailPrice,0.0) OR
        ISNULL(d.TypicalWeightPerUnit,0.0) <> ISNULL(s.TypicalWeightPerUnit,0.0) OR
        ISNULL(d.CountryOfManufacture,'') <> ISNULL(s.CountryOfManufacture,'') OR
        ISNULL(d.ProductTags,'') <> ISNULL(s.ProductTags,'')
    );

    INSERT INTO WHS.StockItems (
        StockItemID, StockItemName, UnitPackageID, OuterPackageID, LeadTimeDays, QuantityPerOuter,
        IsChillerStock, TaxRate, UnitPrice, RecommendedRetailPrice, TypicalWeightPerUnit,
        CountryOfManufacture, ProductTags, LastEditedBy, ValidFrom, ValidTo)
    SELECT s.StockItemID, s.StockItemName, s.UnitPackageID, s.OuterPackageID, s.LeadTimeDays, s.QuantityPerOuter,
           s.IsChillerStock, s.TaxRate, s.UnitPrice, s.RecommendedRetailPrice, s.TypicalWeightPerUnit,
           s.CountryOfManufacture, s.ProductTags, s.LastEditedBy, @Now, @MaxDate
    FROM WHS.Staging_StockItems s
    JOIN WHS.StockItems d ON d.StockItemID = s.StockItemID AND d.ValidTo = @MaxDate
    WHERE (
        ISNULL(d.StockItemName,'') <> ISNULL(s.StockItemName,'') OR
        ISNULL(d.UnitPackageID,-1) <> ISNULL(s.UnitPackageID,-1) OR
        ISNULL(d.OuterPackageID,-1) <> ISNULL(s.OuterPackageID,-1) OR
        ISNULL(d.LeadTimeDays,0) <> ISNULL(s.LeadTimeDays,0) OR
        ISNULL(d.QuantityPerOuter,0) <> ISNULL(s.QuantityPerOuter,0) OR
        ISNULL(d.IsChillerStock,'') <> ISNULL(s.IsChillerStock,'') OR
        ISNULL(d.TaxRate,0.0) <> ISNULL(s.TaxRate,0.0) OR
        ISNULL(d.UnitPrice,0.0) <> ISNULL(s.UnitPrice,0.0) OR
        ISNULL(d.RecommendedRetailPrice,0.0) <> ISNULL(s.RecommendedRetailPrice,0.0) OR
        ISNULL(d.TypicalWeightPerUnit,0.0) <> ISNULL(s.TypicalWeightPerUnit,0.0) OR
        ISNULL(d.CountryOfManufacture,'') <> ISNULL(s.CountryOfManufacture,'') OR
        ISNULL(d.ProductTags,'') <> ISNULL(s.ProductTags,'')
    );

    INSERT INTO WHS.StockItems (
        StockItemID, StockItemName, UnitPackageID, OuterPackageID, LeadTimeDays, QuantityPerOuter,
        IsChillerStock, TaxRate, UnitPrice, RecommendedRetailPrice, TypicalWeightPerUnit,
        CountryOfManufacture, ProductTags, LastEditedBy, ValidFrom, ValidTo)
    SELECT s.StockItemID, s.StockItemName, s.UnitPackageID, s.OuterPackageID, s.LeadTimeDays, s.QuantityPerOuter,
           s.IsChillerStock, s.TaxRate, s.UnitPrice, s.RecommendedRetailPrice, s.TypicalWeightPerUnit,
           s.CountryOfManufacture, s.ProductTags, s.LastEditedBy, @Now, @MaxDate
    FROM WHS.Staging_StockItems s
    WHERE NOT EXISTS (SELECT 1 FROM WHS.StockItems d WHERE d.StockItemID = s.StockItemID);

    INSERT INTO WHS.Dates (DateID, Date, Year, Month, Day, MonthName, DayName)
    SELECT s.DateID, s.Date, s.Year, s.Month, s.Day, s.MonthName, s.DayName
    FROM WHS.Staging_Dates s
    WHERE NOT EXISTS (SELECT 1 FROM WHS.Dates d WHERE d.DateID = s.DateID);

    INSERT INTO WHS.Orders (
        OrderID, CustomerID, SalespersonPersonID, PickedByPersonID, ContactPersonID,
        OrderDate, ExpectedDeliveryDate, PickingCompletedWhen,
        LastEditedBy, LastEditedWhen)
    SELECT s.OrderID, s.CustomerID, s.SalespersonPersonID, s.PickedByPersonID, s.ContactPersonID,
           s.OrderDate, s.ExpectedDeliveryDate, s.PickingCompletedWhen,
           s.LastEditedBy, COALESCE(s.LastEditedWhen, @Now)
    FROM WHS.Staging_Orders s
    WHERE NOT EXISTS (SELECT 1 FROM WHS.Orders d WHERE d.OrderID = s.OrderID);

    INSERT INTO WHS.OrderLines (
        OrderLineID, OrderID, StockItemID, Description, PackageTypeID, Quantity,
        UnitPrice, TaxRate, PickedQuantity, PickingCompletedWhen,
        LastEditedBy, LastEditedWhen)
    SELECT s.OrderLineID, s.OrderID, s.StockItemID, s.Description, s.PackageTypeID, s.Quantity,
           s.UnitPrice, s.TaxRate, s.PickedQuantity, s.PickingCompletedWhen,
           s.LastEditedBy, COALESCE(s.LastEditedWhen, @Now)
    FROM WHS.Staging_OrderLines s
    WHERE NOT EXISTS (SELECT 1 FROM WHS.OrderLines d WHERE d.OrderLineID = s.OrderLineID);

    INSERT INTO WHS.Invoices (
        InvoiceID, CustomerID, BillToCustomerID, OrderID, DeliveryMethodID,
        ContactPersonID, AccountsPersonID, SalespersonPersonID, PackedByPersonID,
        InvoiceDate, IsCreditNote, DeliveryInstructions,
        TotalDryItems, TotalChillerItems, ConfirmedDeliveryTime, ConfirmedReceivedBy,
        LastEditedBy, LastEditedWhen)
    SELECT s.InvoiceID, s.CustomerID, s.BillToCustomerID, s.OrderID, s.DeliveryMethodID,
           s.ContactPersonID, s.AccountsPersonID, s.SalespersonPersonID, s.PackedByPersonID,
           s.InvoiceDate, s.IsCreditNote, s.DeliveryInstructions,
           s.TotalDryItems, s.TotalChillerItems, s.ConfirmedDeliveryTime, s.ConfirmedReceivedBy,
           s.LastEditedBy, COALESCE(s.LastEditedWhen, @Now)
    FROM WHS.Staging_Invoices s
    WHERE NOT EXISTS (SELECT 1 FROM WHS.Invoices d WHERE d.InvoiceID = s.InvoiceID);

    UPDATE d
    SET d.QuantityOnHand = s.QuantityOnHand,
        d.BinLocation = s.BinLocation,
        d.LastStocktakeQuantity = s.LastStocktakeQuantity,
        d.LastCostPrice = s.LastCostPrice,
        d.ReorderLevel = s.ReorderLevel,
        d.TargetStockLevel = s.TargetStockLevel,
        d.LastEditedBy = s.LastEditedBy,
        d.LastEditedWhen = COALESCE(s.LastEditedWhen, @Now)
    FROM WHS.StockItemHoldings d
    JOIN WHS.Staging_StockItemHoldings s ON d.StockItemID = s.StockItemID
    WHERE (
        ISNULL(d.QuantityOnHand,0) <> ISNULL(s.QuantityOnHand,0) OR
        ISNULL(d.BinLocation,'') <> ISNULL(s.BinLocation,'') OR
        ISNULL(d.LastStocktakeQuantity,0) <> ISNULL(s.LastStocktakeQuantity,0) OR
        ISNULL(d.LastCostPrice,0.0) <> ISNULL(s.LastCostPrice,0.0) OR
        ISNULL(d.ReorderLevel,0) <> ISNULL(s.ReorderLevel,0) OR
        ISNULL(d.TargetStockLevel,0) <> ISNULL(s.TargetStockLevel,0)
    );

    INSERT INTO WHS.StockItemHoldings (
        StockItemID, QuantityOnHand, BinLocation, LastStocktakeQuantity,
        LastCostPrice, ReorderLevel, TargetStockLevel, LastEditedBy, LastEditedWhen)
    SELECT s.StockItemID, s.QuantityOnHand, s.BinLocation, s.LastStocktakeQuantity,
           s.LastCostPrice, s.ReorderLevel, s.TargetStockLevel, s.LastEditedBy, COALESCE(s.LastEditedWhen, @Now)
    FROM WHS.Staging_StockItemHoldings s
    WHERE NOT EXISTS (SELECT 1 FROM WHS.StockItemHoldings d WHERE d.StockItemID = s.StockItemID);
END