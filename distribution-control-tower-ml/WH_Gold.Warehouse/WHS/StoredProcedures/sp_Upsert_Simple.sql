CREATE PROCEDURE WHS.sp_Upsert_Simple
    @SchemaName NVARCHAR(128) = 'WHS'
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Now DATETIME2(3) = SYSUTCDATETIME();
    DECLARE @MaxDate DATETIME2(3) = '9999-12-31 23:59:59.997';
    DECLARE @NowPrev DATETIME2(3) = DATEADD(millisecond, -1, @Now);
    DECLARE @SQL NVARCHAR(MAX);

    SET @SQL = N'
    UPDATE d
    SET d.ValidTo = @NowPrev
    FROM ' + QUOTENAME(@SchemaName) + N'.Countries d
    JOIN ' + QUOTENAME(@SchemaName) + N'.Staging_Countries s ON d.CountryID = s.CountryID
    WHERE d.ValidTo = @MaxDate AND (
        ISNULL(d.CountryName, '''') <> ISNULL(s.CountryName, '''') OR
        ISNULL(d.FormalName, '''') <> ISNULL(s.FormalName, '''') OR
        ISNULL(d.IsoAlpha3Code, '''') <> ISNULL(s.IsoAlpha3Code, '''') OR
        ISNULL(d.CountryType, '''') <> ISNULL(s.CountryType, '''') OR
        ISNULL(d.LatestRecordedPopulation, 0) <> ISNULL(s.LatestRecordedPopulation, 0) OR
        ISNULL(d.Continent, '''') <> ISNULL(s.Continent, '''') OR
        ISNULL(d.Region, '''') <> ISNULL(s.Region, '''') OR
        ISNULL(d.Subregion, '''') <> ISNULL(s.Subregion, '''')
    );
    ';
    EXEC sp_executesql @SQL, 
        N'@NowPrev DATETIME2(3), @MaxDate DATETIME2(3)', 
        @NowPrev = @NowPrev, @MaxDate = @MaxDate;

    SET @SQL = N'
    INSERT INTO ' + QUOTENAME(@SchemaName) + N'.Countries (
        CountryID, CountryName, FormalName, IsoAlpha3Code, CountryType,
        LatestRecordedPopulation, Continent, Region, Subregion,
        LastEditedBy, ValidFrom, ValidTo)
    SELECT s.CountryID, s.CountryName, s.FormalName, s.IsoAlpha3Code, s.CountryType,
           s.LatestRecordedPopulation, s.Continent, s.Region, s.Subregion,
           s.LastEditedBy, @Now, @MaxDate
    FROM ' + QUOTENAME(@SchemaName) + N'.Staging_Countries s
    JOIN ' + QUOTENAME(@SchemaName) + N'.Countries d ON d.CountryID = s.CountryID AND d.ValidTo = @MaxDate
    WHERE (
        ISNULL(d.CountryName, '''') <> ISNULL(s.CountryName, '''') OR
        ISNULL(d.FormalName, '''') <> ISNULL(s.FormalName, '''') OR
        ISNULL(d.IsoAlpha3Code, '''') <> ISNULL(s.IsoAlpha3Code, '''') OR
        ISNULL(d.CountryType, '''') <> ISNULL(s.CountryType, '''') OR
        ISNULL(d.LatestRecordedPopulation, 0) <> ISNULL(s.LatestRecordedPopulation, 0) OR
        ISNULL(d.Continent, '''') <> ISNULL(s.Continent, '''') OR
        ISNULL(d.Region, '''') <> ISNULL(s.Region, '''') OR
        ISNULL(d.Subregion, '''') <> ISNULL(s.Subregion, '''')
    );
    ';
    EXEC sp_executesql @SQL, 
        N'@Now DATETIME2(3), @MaxDate DATETIME2(3)', 
        @Now = @Now, @MaxDate = @MaxDate;

    SET @SQL = N'
    INSERT INTO ' + QUOTENAME(@SchemaName) + N'.Countries (
        CountryID, CountryName, FormalName, IsoAlpha3Code, CountryType,
        LatestRecordedPopulation, Continent, Region, Subregion,
        LastEditedBy, ValidFrom, ValidTo)
    SELECT s.CountryID, s.CountryName, s.FormalName, s.IsoAlpha3Code, s.CountryType,
           s.LatestRecordedPopulation, s.Continent, s.Region, s.Subregion,
           s.LastEditedBy, @Now, @MaxDate
    FROM ' + QUOTENAME(@SchemaName) + N'.Staging_Countries s
    WHERE NOT EXISTS (SELECT 1 FROM ' + QUOTENAME(@SchemaName) + N'.Countries d WHERE d.CountryID = s.CountryID);
    ';
    EXEC sp_executesql @SQL, 
        N'@Now DATETIME2(3), @MaxDate DATETIME2(3)', 
        @Now = @Now, @MaxDate = @MaxDate;

    SET @SQL = N'
    UPDATE d
    SET d.ValidTo = @NowPrev
    FROM ' + QUOTENAME(@SchemaName) + N'.StateProvinces d
    JOIN ' + QUOTENAME(@SchemaName) + N'.Staging_StateProvinces s ON d.StateProvinceID = s.StateProvinceID
    WHERE d.ValidTo = @MaxDate AND (
        ISNULL(d.StateProvinceCode, '''') <> ISNULL(s.StateProvinceCode, '''') OR
        ISNULL(d.StateProvinceName, '''') <> ISNULL(s.StateProvinceName, '''') OR
        ISNULL(d.CountryID, -1) <> ISNULL(s.CountryID, -1) OR
        ISNULL(d.SalesTerritory, '''') <> ISNULL(s.SalesTerritory, '''') OR
        ISNULL(d.LatestRecordedPopulation, 0) <> ISNULL(s.LatestRecordedPopulation, 0)
    );
    ';
    EXEC sp_executesql @SQL, 
        N'@NowPrev DATETIME2(3), @MaxDate DATETIME2(3)', 
        @NowPrev = @NowPrev, @MaxDate = @MaxDate;

    SET @SQL = N'
    INSERT INTO ' + QUOTENAME(@SchemaName) + N'.StateProvinces (
        StateProvinceID, StateProvinceCode, StateProvinceName, CountryID,
        SalesTerritory, LatestRecordedPopulation, LastEditedBy, ValidFrom, ValidTo)
    SELECT s.StateProvinceID, s.StateProvinceCode, s.StateProvinceName, s.CountryID,
           s.SalesTerritory, s.LatestRecordedPopulation, s.LastEditedBy, @Now, @MaxDate
    FROM ' + QUOTENAME(@SchemaName) + N'.Staging_StateProvinces s
    JOIN ' + QUOTENAME(@SchemaName) + N'.StateProvinces d ON d.StateProvinceID = s.StateProvinceID AND d.ValidTo = @MaxDate
    WHERE (
        ISNULL(d.StateProvinceCode, '''') <> ISNULL(s.StateProvinceCode, '''') OR
        ISNULL(d.StateProvinceName, '''') <> ISNULL(s.StateProvinceName, '''') OR
        ISNULL(d.CountryID, -1) <> ISNULL(s.CountryID, -1) OR
        ISNULL(d.SalesTerritory, '''') <> ISNULL(s.SalesTerritory, '''') OR
        ISNULL(d.LatestRecordedPopulation, 0) <> ISNULL(s.LatestRecordedPopulation, 0)
    );
    ';
    EXEC sp_executesql @SQL, 
        N'@Now DATETIME2(3), @MaxDate DATETIME2(3)', 
        @Now = @Now, @MaxDate = @MaxDate;

    SET @SQL = N'
    INSERT INTO ' + QUOTENAME(@SchemaName) + N'.StateProvinces (
        StateProvinceID, StateProvinceCode, StateProvinceName, CountryID,
        SalesTerritory, LatestRecordedPopulation, LastEditedBy, ValidFrom, ValidTo)
    SELECT s.StateProvinceID, s.StateProvinceCode, s.StateProvinceName, s.CountryID,
           s.SalesTerritory, s.LatestRecordedPopulation, s.LastEditedBy, @Now, @MaxDate
    FROM ' + QUOTENAME(@SchemaName) + N'.Staging_StateProvinces s
    WHERE NOT EXISTS (SELECT 1 FROM ' + QUOTENAME(@SchemaName) + N'.StateProvinces d WHERE d.StateProvinceID = s.StateProvinceID);
    ';
    EXEC sp_executesql @SQL, 
        N'@Now DATETIME2(3), @MaxDate DATETIME2(3)', 
        @Now = @Now, @MaxDate = @MaxDate;

    SET @SQL = N'
    UPDATE d
    SET d.ValidTo = @NowPrev
    FROM ' + QUOTENAME(@SchemaName) + N'.Cities d
    JOIN ' + QUOTENAME(@SchemaName) + N'.Staging_Cities s ON d.CityID = s.CityID
    WHERE d.ValidTo = @MaxDate AND (
        ISNULL(d.CityName, '''') <> ISNULL(s.CityName, '''') OR
        ISNULL(d.Longitude, 0.0) <> ISNULL(s.Longitude, 0.0) OR
        ISNULL(d.Latitude, 0.0) <> ISNULL(s.Latitude, 0.0) OR
        ISNULL(d.StateProvinceID, -1) <> ISNULL(s.StateProvinceID, -1) OR
        ISNULL(d.LatestRecordedPopulation, 0) <> ISNULL(s.LatestRecordedPopulation, 0)
    );
    ';
    EXEC sp_executesql @SQL, 
        N'@NowPrev DATETIME2(3), @MaxDate DATETIME2(3)', 
        @NowPrev = @NowPrev, @MaxDate = @MaxDate;

    SET @SQL = N'
    INSERT INTO ' + QUOTENAME(@SchemaName) + N'.Cities (
        CityID, CityName, Longitude, Latitude, StateProvinceID, LatestRecordedPopulation,
        LastEditedBy, ValidFrom, ValidTo)
    SELECT s.CityID, s.CityName, s.Longitude, s.Latitude, s.StateProvinceID, s.LatestRecordedPopulation,
           s.LastEditedBy, @Now, @MaxDate
    FROM ' + QUOTENAME(@SchemaName) + N'.Staging_Cities s
    JOIN ' + QUOTENAME(@SchemaName) + N'.Cities d ON d.CityID = s.CityID AND d.ValidTo = @MaxDate
    WHERE (
        ISNULL(d.CityName, '''') <> ISNULL(s.CityName, '''') OR
        ISNULL(d.Longitude, 0.0) <> ISNULL(s.Longitude, 0.0) OR
        ISNULL(d.Latitude, 0.0) <> ISNULL(s.Latitude, 0.0) OR
        ISNULL(d.StateProvinceID, -1) <> ISNULL(s.StateProvinceID, -1) OR
        ISNULL(d.LatestRecordedPopulation, 0) <> ISNULL(s.LatestRecordedPopulation, 0)
    );
    ';
    EXEC sp_executesql @SQL, 
        N'@Now DATETIME2(3), @MaxDate DATETIME2(3)', 
        @Now = @Now, @MaxDate = @MaxDate;

    SET @SQL = N'
    INSERT INTO ' + QUOTENAME(@SchemaName) + N'.Cities (
        CityID, CityName, Longitude, Latitude, StateProvinceID, LatestRecordedPopulation,
        LastEditedBy, ValidFrom, ValidTo)
    SELECT s.CityID, s.CityName, s.Longitude, s.Latitude, s.StateProvinceID, s.LatestRecordedPopulation,
           s.LastEditedBy, @Now, @MaxDate
    FROM ' + QUOTENAME(@SchemaName) + N'.Staging_Cities s
    WHERE NOT EXISTS (SELECT 1 FROM ' + QUOTENAME(@SchemaName) + N'.Cities d WHERE d.CityID = s.CityID);
    ';
    EXEC sp_executesql @SQL, 
        N'@Now DATETIME2(3), @MaxDate DATETIME2(3)', 
        @Now = @Now, @MaxDate = @MaxDate;

    SET @SQL = N'
    UPDATE d
    SET d.ValidTo = @NowPrev
    FROM ' + QUOTENAME(@SchemaName) + N'.DeliveryMethods d
    JOIN ' + QUOTENAME(@SchemaName) + N'.Staging_DeliveryMethods s ON d.DeliveryMethodID = s.DeliveryMethodID
    WHERE d.ValidTo = @MaxDate AND ISNULL(d.DeliveryMethodName, '''') <> ISNULL(s.DeliveryMethodName, '''');
    ';
    EXEC sp_executesql @SQL, 
        N'@NowPrev DATETIME2(3), @MaxDate DATETIME2(3)', 
        @NowPrev = @NowPrev, @MaxDate = @MaxDate;

    SET @SQL = N'
    INSERT INTO ' + QUOTENAME(@SchemaName) + N'.DeliveryMethods (DeliveryMethodID, DeliveryMethodName, LastEditedBy, ValidFrom, ValidTo)
    SELECT s.DeliveryMethodID, s.DeliveryMethodName, s.LastEditedBy, @Now, @MaxDate
    FROM ' + QUOTENAME(@SchemaName) + N'.Staging_DeliveryMethods s
    JOIN ' + QUOTENAME(@SchemaName) + N'.DeliveryMethods d ON d.DeliveryMethodID = s.DeliveryMethodID AND d.ValidTo = @MaxDate
    WHERE ISNULL(d.DeliveryMethodName, '''') <> ISNULL(s.DeliveryMethodName, '''');
    ';
    EXEC sp_executesql @SQL, 
        N'@Now DATETIME2(3), @MaxDate DATETIME2(3)', 
        @Now = @Now, @MaxDate = @MaxDate;

    SET @SQL = N'
    INSERT INTO ' + QUOTENAME(@SchemaName) + N'.DeliveryMethods (DeliveryMethodID, DeliveryMethodName, LastEditedBy, ValidFrom, ValidTo)
    SELECT s.DeliveryMethodID, s.DeliveryMethodName, s.LastEditedBy, @Now, @MaxDate
    FROM ' + QUOTENAME(@SchemaName) + N'.Staging_DeliveryMethods s
    WHERE NOT EXISTS (SELECT 1 FROM ' + QUOTENAME(@SchemaName) + N'.DeliveryMethods d WHERE d.DeliveryMethodID = s.DeliveryMethodID);
    ';
    EXEC sp_executesql @SQL, 
        N'@Now DATETIME2(3), @MaxDate DATETIME2(3)', 
        @Now = @Now, @MaxDate = @MaxDate;

    SET @SQL = N'
    UPDATE d
    SET d.ValidTo = @NowPrev
    FROM ' + QUOTENAME(@SchemaName) + N'.People d
    JOIN ' + QUOTENAME(@SchemaName) + N'.Staging_People s ON d.PersonID = s.PersonID
    WHERE d.ValidTo = @MaxDate AND (
        ISNULL(d.FullName, '''') <> ISNULL(s.FullName, '''') OR
        ISNULL(d.PreferredName, '''') <> ISNULL(s.PreferredName, '''') OR
        ISNULL(d.SearchName, '''') <> ISNULL(s.SearchName, '''') OR
        ISNULL(d.IsPermittedToLogon, '''') <> ISNULL(s.IsPermittedToLogon, '''') OR
        ISNULL(d.LogonName, '''') <> ISNULL(s.LogonName, '''') OR
        ISNULL(d.IsExternalLogonProvider, '''') <> ISNULL(s.IsExternalLogonProvider, '''') OR
        ISNULL(d.IsSystemUser, '''') <> ISNULL(s.IsSystemUser, '''') OR
        ISNULL(d.IsEmployee, '''') <> ISNULL(s.IsEmployee, '''') OR
        ISNULL(d.IsSalesperson, '''') <> ISNULL(s.IsSalesperson, '''') OR
        ISNULL(d.PhoneNumber, '''') <> ISNULL(s.PhoneNumber, '''') OR
        ISNULL(d.FaxNumber, '''') <> ISNULL(s.FaxNumber, '''') OR
        ISNULL(d.EmailAddress, '''') <> ISNULL(s.EmailAddress, '''')
    );
    ';
    EXEC sp_executesql @SQL, 
        N'@NowPrev DATETIME2(3), @MaxDate DATETIME2(3)', 
        @NowPrev = @NowPrev, @MaxDate = @MaxDate;

    SET @SQL = N'
    INSERT INTO ' + QUOTENAME(@SchemaName) + N'.People (
        PersonID, FullName, PreferredName, SearchName, IsPermittedToLogon, LogonName,
        IsExternalLogonProvider, IsSystemUser, IsEmployee, IsSalesperson,
        PhoneNumber, FaxNumber, EmailAddress,
        LastEditedBy, ValidFrom, ValidTo)
    SELECT s.PersonID, s.FullName, s.PreferredName, s.SearchName, s.IsPermittedToLogon, s.LogonName,
           s.IsExternalLogonProvider, s.IsSystemUser, s.IsEmployee, s.IsSalesperson,
           s.PhoneNumber, s.FaxNumber, s.EmailAddress,
           s.LastEditedBy, @Now, @MaxDate
    FROM ' + QUOTENAME(@SchemaName) + N'.Staging_People s
    JOIN ' + QUOTENAME(@SchemaName) + N'.People d ON d.PersonID = s.PersonID AND d.ValidTo = @MaxDate
    WHERE (
        ISNULL(d.FullName, '''') <> ISNULL(s.FullName, '''') OR
        ISNULL(d.PreferredName, '''') <> ISNULL(s.PreferredName, '''') OR
        ISNULL(d.SearchName, '''') <> ISNULL(s.SearchName, '''') OR
        ISNULL(d.IsPermittedToLogon, '''') <> ISNULL(s.IsPermittedToLogon, '''') OR
        ISNULL(d.LogonName, '''') <> ISNULL(s.LogonName, '''') OR
        ISNULL(d.IsExternalLogonProvider, '''') <> ISNULL(s.IsExternalLogonProvider, '''') OR
        ISNULL(d.IsSystemUser, '''') <> ISNULL(s.IsSystemUser, '''') OR
        ISNULL(d.IsEmployee, '''') <> ISNULL(s.IsEmployee, '''') OR
        ISNULL(d.IsSalesperson, '''') <> ISNULL(s.IsSalesperson, '''') OR
        ISNULL(d.PhoneNumber, '''') <> ISNULL(s.PhoneNumber, '''') OR
        ISNULL(d.FaxNumber, '''') <> ISNULL(s.FaxNumber, '''') OR
        ISNULL(d.EmailAddress, '''') <> ISNULL(s.EmailAddress, '''')
    );
    ';
    EXEC sp_executesql @SQL, 
        N'@Now DATETIME2(3), @MaxDate DATETIME2(3)', 
        @Now = @Now, @MaxDate = @MaxDate;

    SET @SQL = N'
    INSERT INTO ' + QUOTENAME(@SchemaName) + N'.People (
        PersonID, FullName, PreferredName, SearchName, IsPermittedToLogon, LogonName,
        IsExternalLogonProvider, IsSystemUser, IsEmployee, IsSalesperson,
        PhoneNumber, FaxNumber, EmailAddress,
        LastEditedBy, ValidFrom, ValidTo)
    SELECT s.PersonID, s.FullName, s.PreferredName, s.SearchName, s.IsPermittedToLogon, s.LogonName,
           s.IsExternalLogonProvider, s.IsSystemUser, s.IsEmployee, s.IsSalesperson,
           s.PhoneNumber, s.FaxNumber, s.EmailAddress,
           s.LastEditedBy, @Now, @MaxDate
    FROM ' + QUOTENAME(@SchemaName) + N'.Staging_People s
    WHERE NOT EXISTS (SELECT 1 FROM ' + QUOTENAME(@SchemaName) + N'.People d WHERE d.PersonID = s.PersonID);
    ';
    EXEC sp_executesql @SQL, 
        N'@Now DATETIME2(3), @MaxDate DATETIME2(3)', 
        @Now = @Now, @MaxDate = @MaxDate;

    SET @SQL = N'
    UPDATE d
    SET d.ValidTo = @NowPrev
    FROM ' + QUOTENAME(@SchemaName) + N'.CustomerCategories d
    JOIN ' + QUOTENAME(@SchemaName) + N'.Staging_CustomerCategories s ON d.CustomerCategoryID = s.CustomerCategoryID
    WHERE d.ValidTo = @MaxDate AND ISNULL(d.CustomerCategoryName, '''') <> ISNULL(s.CustomerCategoryName, '''');
    ';
    EXEC sp_executesql @SQL, 
        N'@NowPrev DATETIME2(3), @MaxDate DATETIME2(3)', 
        @NowPrev = @NowPrev, @MaxDate = @MaxDate;

    SET @SQL = N'
    INSERT INTO ' + QUOTENAME(@SchemaName) + N'.CustomerCategories (CustomerCategoryID, CustomerCategoryName, LastEditedBy, ValidFrom, ValidTo)
    SELECT s.CustomerCategoryID, s.CustomerCategoryName, s.LastEditedBy, @Now, @MaxDate
    FROM ' + QUOTENAME(@SchemaName) + N'.Staging_CustomerCategories s
    JOIN ' + QUOTENAME(@SchemaName) + N'.CustomerCategories d ON d.CustomerCategoryID = s.CustomerCategoryID AND d.ValidTo = @MaxDate
    WHERE ISNULL(d.CustomerCategoryName, '''') <> ISNULL(s.CustomerCategoryName, '''');
    ';
    EXEC sp_executesql @SQL, 
        N'@Now DATETIME2(3), @MaxDate DATETIME2(3)', 
        @Now = @Now, @MaxDate = @MaxDate;

    SET @SQL = N'
    INSERT INTO ' + QUOTENAME(@SchemaName) + N'.CustomerCategories (CustomerCategoryID, CustomerCategoryName, LastEditedBy, ValidFrom, ValidTo)
    SELECT s.CustomerCategoryID, s.CustomerCategoryName, s.LastEditedBy, @Now, @MaxDate
    FROM ' + QUOTENAME(@SchemaName) + N'.Staging_CustomerCategories s
    WHERE NOT EXISTS (SELECT 1 FROM ' + QUOTENAME(@SchemaName) + N'.CustomerCategories d WHERE d.CustomerCategoryID = s.CustomerCategoryID);
    ';
    EXEC sp_executesql @SQL, 
        N'@Now DATETIME2(3), @MaxDate DATETIME2(3)', 
        @Now = @Now, @MaxDate = @MaxDate;

    SET @SQL = N'
    UPDATE d
    SET d.ValidTo = @NowPrev
    FROM ' + QUOTENAME(@SchemaName) + N'.Customers d
    JOIN ' + QUOTENAME(@SchemaName) + N'.Staging_Customers s ON d.CustomerID = s.CustomerID
    WHERE d.ValidTo = @MaxDate AND (
        ISNULL(d.CustomerName, '''') <> ISNULL(s.CustomerName, '''') OR
        ISNULL(d.BillToCustomerID, -1) <> ISNULL(s.BillToCustomerID, -1) OR
        ISNULL(d.CustomerCategoryID, -1) <> ISNULL(s.CustomerCategoryID, -1) OR
        ISNULL(d.PrimaryContactPersonID, -1) <> ISNULL(s.PrimaryContactPersonID, -1) OR
        ISNULL(d.DeliveryMethodID, -1) <> ISNULL(s.DeliveryMethodID, -1) OR
        ISNULL(d.DeliveryCityID, -1) <> ISNULL(s.DeliveryCityID, -1) OR
        ISNULL(d.CreditLimit, 0.0) <> ISNULL(s.CreditLimit, 0.0) OR
        ISNULL(d.AccountOpenedDate, ''19000101'') <> ISNULL(s.AccountOpenedDate, ''19000101'') OR
        ISNULL(d.StandardDiscountPercentage, 0.0) <> ISNULL(s.StandardDiscountPercentage, 0.0) OR
        ISNULL(d.PaymentDays, 0) <> ISNULL(s.PaymentDays, 0) OR
        ISNULL(d.PhoneNumber, '''') <> ISNULL(s.PhoneNumber, '''') OR
        ISNULL(d.FaxNumber, '''') <> ISNULL(s.FaxNumber, '''') OR
        ISNULL(d.DeliveryAddressLine1, '''') <> ISNULL(s.DeliveryAddressLine1, '''') OR
        ISNULL(d.DeliveryAddressLine2, '''') <> ISNULL(s.DeliveryAddressLine2, '''') OR
        ISNULL(d.DeliveryPostalCode, '''') <> ISNULL(s.DeliveryPostalCode, '''') OR
        ISNULL(d.PostalAddressLine1, '''') <> ISNULL(s.PostalAddressLine1, '''')
    );
    ';
    EXEC sp_executesql @SQL, 
        N'@NowPrev DATETIME2(3), @MaxDate DATETIME2(3)', 
        @NowPrev = @NowPrev, @MaxDate = @MaxDate;

    SET @SQL = N'
    INSERT INTO ' + QUOTENAME(@SchemaName) + N'.Customers (
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
    FROM ' + QUOTENAME(@SchemaName) + N'.Staging_Customers s
    JOIN ' + QUOTENAME(@SchemaName) + N'.Customers d ON d.CustomerID = s.CustomerID AND d.ValidTo = @MaxDate
    WHERE (
        ISNULL(d.CustomerName, '''') <> ISNULL(s.CustomerName, '''') OR
        ISNULL(d.BillToCustomerID, -1) <> ISNULL(s.BillToCustomerID, -1) OR
        ISNULL(d.CustomerCategoryID, -1) <> ISNULL(s.CustomerCategoryID, -1) OR
        ISNULL(d.PrimaryContactPersonID, -1) <> ISNULL(s.PrimaryContactPersonID, -1) OR
        ISNULL(d.DeliveryMethodID, -1) <> ISNULL(s.DeliveryMethodID, -1) OR
        ISNULL(d.DeliveryCityID, -1) <> ISNULL(s.DeliveryCityID, -1) OR
        ISNULL(d.CreditLimit, 0.0) <> ISNULL(s.CreditLimit, 0.0) OR
        ISNULL(d.AccountOpenedDate, ''19000101'') <> ISNULL(s.AccountOpenedDate, ''19000101'') OR
        ISNULL(d.StandardDiscountPercentage, 0.0) <> ISNULL(s.StandardDiscountPercentage, 0.0) OR
        ISNULL(d.PaymentDays, 0) <> ISNULL(s.PaymentDays, 0) OR
        ISNULL(d.PhoneNumber, '''') <> ISNULL(s.PhoneNumber, '''') OR
        ISNULL(d.FaxNumber, '''') <> ISNULL(s.FaxNumber, '''') OR
        ISNULL(d.DeliveryAddressLine1, '''') <> ISNULL(s.DeliveryAddressLine1, '''') OR
        ISNULL(d.DeliveryAddressLine2, '''') <> ISNULL(s.DeliveryAddressLine2, '''') OR
        ISNULL(d.DeliveryPostalCode, '''') <> ISNULL(s.DeliveryPostalCode, '''') OR
        ISNULL(d.PostalAddressLine1, '''') <> ISNULL(s.PostalAddressLine1, '''')
    );
    ';
    EXEC sp_executesql @SQL, 
        N'@Now DATETIME2(3), @MaxDate DATETIME2(3)', 
        @Now = @Now, @MaxDate = @MaxDate;

    SET @SQL = N'
    INSERT INTO ' + QUOTENAME(@SchemaName) + N'.Customers (
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
    FROM ' + QUOTENAME(@SchemaName) + N'.Staging_Customers s
    WHERE NOT EXISTS (SELECT 1 FROM ' + QUOTENAME(@SchemaName) + N'.Customers d WHERE d.CustomerID = s.CustomerID);
    ';
    EXEC sp_executesql @SQL, 
        N'@Now DATETIME2(3), @MaxDate DATETIME2(3)', 
        @Now = @Now, @MaxDate = @MaxDate;

    SET @SQL = N'
    UPDATE d
    SET d.ValidTo = @NowPrev
    FROM ' + QUOTENAME(@SchemaName) + N'.PackageTypes d
    JOIN ' + QUOTENAME(@SchemaName) + N'.Staging_PackageTypes s ON d.PackageTypeID = s.PackageTypeID
    WHERE d.ValidTo = @MaxDate AND ISNULL(d.PackageTypeName, '''') <> ISNULL(s.PackageTypeName, '''');
    ';
    EXEC sp_executesql @SQL, 
        N'@NowPrev DATETIME2(3), @MaxDate DATETIME2(3)', 
        @NowPrev = @NowPrev, @MaxDate = @MaxDate;

    SET @SQL = N'
    INSERT INTO ' + QUOTENAME(@SchemaName) + N'.PackageTypes (PackageTypeID, PackageTypeName, LastEditedBy, ValidFrom, ValidTo)
    SELECT s.PackageTypeID, s.PackageTypeName, s.LastEditedBy, @Now, @MaxDate
    FROM ' + QUOTENAME(@SchemaName) + N'.Staging_PackageTypes s
    JOIN ' + QUOTENAME(@SchemaName) + N'.PackageTypes d ON d.PackageTypeID = s.PackageTypeID AND d.ValidTo = @MaxDate
    WHERE ISNULL(d.PackageTypeName, '''') <> ISNULL(s.PackageTypeName, '''');
    ';
    EXEC sp_executesql @SQL, 
        N'@Now DATETIME2(3), @MaxDate DATETIME2(3)', 
        @Now = @Now, @MaxDate = @MaxDate;

    SET @SQL = N'
    INSERT INTO ' + QUOTENAME(@SchemaName) + N'.PackageTypes (PackageTypeID, PackageTypeName, LastEditedBy, ValidFrom, ValidTo)
    SELECT s.PackageTypeID, s.PackageTypeName, s.LastEditedBy, @Now, @MaxDate
    FROM ' + QUOTENAME(@SchemaName) + N'.Staging_PackageTypes s
    WHERE NOT EXISTS (SELECT 1 FROM ' + QUOTENAME(@SchemaName) + N'.PackageTypes d WHERE d.PackageTypeID = s.PackageTypeID);
    ';
    EXEC sp_executesql @SQL, 
        N'@Now DATETIME2(3), @MaxDate DATETIME2(3)', 
        @Now = @Now, @MaxDate = @MaxDate;

    SET @SQL = N'
    UPDATE d
    SET d.ValidTo = @NowPrev
    FROM ' + QUOTENAME(@SchemaName) + N'.StockItems d
    JOIN ' + QUOTENAME(@SchemaName) + N'.Staging_StockItems s ON d.StockItemID = s.StockItemID
    WHERE d.ValidTo = @MaxDate AND (
        ISNULL(d.StockItemName, '''') <> ISNULL(s.StockItemName, '''') OR
        ISNULL(d.UnitPackageID, -1) <> ISNULL(s.UnitPackageID, -1) OR
        ISNULL(d.OuterPackageID, -1) <> ISNULL(s.OuterPackageID, -1) OR
        ISNULL(d.LeadTimeDays, 0) <> ISNULL(s.LeadTimeDays, 0) OR
        ISNULL(d.QuantityPerOuter, 0) <> ISNULL(s.QuantityPerOuter, 0) OR
        ISNULL(d.IsChillerStock, '''') <> ISNULL(s.IsChillerStock, '''') OR
        ISNULL(d.TaxRate, 0.0) <> ISNULL(s.TaxRate, 0.0) OR
        ISNULL(d.UnitPrice, 0.0) <> ISNULL(s.UnitPrice, 0.0) OR
        ISNULL(d.RecommendedRetailPrice, 0.0) <> ISNULL(s.RecommendedRetailPrice, 0.0) OR
        ISNULL(d.TypicalWeightPerUnit, 0.0) <> ISNULL(s.TypicalWeightPerUnit, 0.0) OR
        ISNULL(d.CountryOfManufacture, '''') <> ISNULL(s.CountryOfManufacture, '''') OR
        ISNULL(d.ProductTags, '''') <> ISNULL(s.ProductTags, '''')
    );
    ';
    EXEC sp_executesql @SQL, 
        N'@NowPrev DATETIME2(3), @MaxDate DATETIME2(3)', 
        @NowPrev = @NowPrev, @MaxDate = @MaxDate;

    SET @SQL = N'
    INSERT INTO ' + QUOTENAME(@SchemaName) + N'.StockItems (
        StockItemID, StockItemName, UnitPackageID, OuterPackageID, LeadTimeDays, QuantityPerOuter,
        IsChillerStock, TaxRate, UnitPrice, RecommendedRetailPrice, TypicalWeightPerUnit,
        CountryOfManufacture, ProductTags, LastEditedBy, ValidFrom, ValidTo)
    SELECT s.StockItemID, s.StockItemName, s.UnitPackageID, s.OuterPackageID, s.LeadTimeDays, s.QuantityPerOuter,
           s.IsChillerStock, s.TaxRate, s.UnitPrice, s.RecommendedRetailPrice, s.TypicalWeightPerUnit,
           s.CountryOfManufacture, s.ProductTags, s.LastEditedBy, @Now, @MaxDate
    FROM ' + QUOTENAME(@SchemaName) + N'.Staging_StockItems s
    JOIN ' + QUOTENAME(@SchemaName) + N'.StockItems d ON d.StockItemID = s.StockItemID AND d.ValidTo = @MaxDate
    WHERE (
        ISNULL(d.StockItemName, '''') <> ISNULL(s.StockItemName, '''') OR
        ISNULL(d.UnitPackageID, -1) <> ISNULL(s.UnitPackageID, -1) OR
        ISNULL(d.OuterPackageID, -1) <> ISNULL(s.OuterPackageID, -1) OR
        ISNULL(d.LeadTimeDays, 0) <> ISNULL(s.LeadTimeDays, 0) OR
        ISNULL(d.QuantityPerOuter, 0) <> ISNULL(s.QuantityPerOuter, 0) OR
        ISNULL(d.IsChillerStock, '''') <> ISNULL(s.IsChillerStock, '''') OR
        ISNULL(d.TaxRate, 0.0) <> ISNULL(s.TaxRate, 0.0) OR
        ISNULL(d.UnitPrice, 0.0) <> ISNULL(s.UnitPrice, 0.0) OR
        ISNULL(d.RecommendedRetailPrice, 0.0) <> ISNULL(s.RecommendedRetailPrice, 0.0) OR
        ISNULL(d.TypicalWeightPerUnit, 0.0) <> ISNULL(s.TypicalWeightPerUnit, 0.0) OR
        ISNULL(d.CountryOfManufacture, '''') <> ISNULL(s.CountryOfManufacture, '''') OR
        ISNULL(d.ProductTags, '''') <> ISNULL(s.ProductTags, '''')
    );
    ';
    EXEC sp_executesql @SQL, 
        N'@Now DATETIME2(3), @MaxDate DATETIME2(3)', 
        @Now = @Now, @MaxDate = @MaxDate;

    SET @SQL = N'
    INSERT INTO ' + QUOTENAME(@SchemaName) + N'.StockItems (
        StockItemID, StockItemName, UnitPackageID, OuterPackageID, LeadTimeDays, QuantityPerOuter,
        IsChillerStock, TaxRate, UnitPrice, RecommendedRetailPrice, TypicalWeightPerUnit,
        CountryOfManufacture, ProductTags, LastEditedBy, ValidFrom, ValidTo)
    SELECT s.StockItemID, s.StockItemName, s.UnitPackageID, s.OuterPackageID, s.LeadTimeDays, s.QuantityPerOuter,
           s.IsChillerStock, s.TaxRate, s.UnitPrice, s.RecommendedRetailPrice, s.TypicalWeightPerUnit,
           s.CountryOfManufacture, s.ProductTags, s.LastEditedBy, @Now, @MaxDate
    FROM ' + QUOTENAME(@SchemaName) + N'.Staging_StockItems s
    WHERE NOT EXISTS (SELECT 1 FROM ' + QUOTENAME(@SchemaName) + N'.StockItems d WHERE d.StockItemID = s.StockItemID);
    ';
    EXEC sp_executesql @SQL, 
        N'@Now DATETIME2(3), @MaxDate DATETIME2(3)', 
        @Now = @Now, @MaxDate = @MaxDate;

    SET @SQL = N'
    INSERT INTO ' + QUOTENAME(@SchemaName) + N'.Dates (DateID, Date, Year, Month, Day, MonthName, DayName)
    SELECT s.DateID, s.Date, s.Year, s.Month, s.Day, s.MonthName, s.DayName
    FROM ' + QUOTENAME(@SchemaName) + N'.Staging_Dates s
    WHERE NOT EXISTS (SELECT 1 FROM ' + QUOTENAME(@SchemaName) + N'.Dates d WHERE d.DateID = s.DateID);
    ';
    EXEC sp_executesql @SQL;

    SET @SQL = N'
    INSERT INTO ' + QUOTENAME(@SchemaName) + N'.Orders (
        OrderID, CustomerID, SalespersonPersonID, PickedByPersonID, ContactPersonID,
        OrderDate, ExpectedDeliveryDate, PickingCompletedWhen,
        LastEditedBy, LastEditedWhen)
    SELECT s.OrderID, s.CustomerID, s.SalespersonPersonID, s.PickedByPersonID, s.ContactPersonID,
           s.OrderDate, s.ExpectedDeliveryDate, s.PickingCompletedWhen,
           s.LastEditedBy, COALESCE(s.LastEditedWhen, @Now)
    FROM ' + QUOTENAME(@SchemaName) + N'.Staging_Orders s
    WHERE NOT EXISTS (SELECT 1 FROM ' + QUOTENAME(@SchemaName) + N'.Orders d WHERE d.OrderID = s.OrderID);
    ';
    EXEC sp_executesql @SQL, 
        N'@Now DATETIME2(3)', 
        @Now = @Now;

    SET @SQL = N'
    INSERT INTO ' + QUOTENAME(@SchemaName) + N'.OrderLines (
        OrderLineID, OrderID, StockItemID, Description, PackageTypeID, Quantity,
        UnitPrice, TaxRate, PickedQuantity, PickingCompletedWhen,
        LastEditedBy, LastEditedWhen)
    SELECT s.OrderLineID, s.OrderID, s.StockItemID, s.Description, s.PackageTypeID, s.Quantity,
           s.UnitPrice, s.TaxRate, s.PickedQuantity, s.PickingCompletedWhen,
           s.LastEditedBy, COALESCE(s.LastEditedWhen, @Now)
    FROM ' + QUOTENAME(@SchemaName) + N'.Staging_OrderLines s
    WHERE NOT EXISTS (SELECT 1 FROM ' + QUOTENAME(@SchemaName) + N'.OrderLines d WHERE d.OrderLineID = s.OrderLineID);
    ';
    EXEC sp_executesql @SQL, 
        N'@Now DATETIME2(3)', 
        @Now = @Now;

    SET @SQL = N'
    INSERT INTO ' + QUOTENAME(@SchemaName) + N'.Invoices (
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
    FROM ' + QUOTENAME(@SchemaName) + N'.Staging_Invoices s
    WHERE NOT EXISTS (SELECT 1 FROM ' + QUOTENAME(@SchemaName) + N'.Invoices d WHERE d.InvoiceID = s.InvoiceID);
    ';
    EXEC sp_executesql @SQL, 
        N'@Now DATETIME2(3)', 
        @Now = @Now;

    SET @SQL = N'
    UPDATE d
    SET d.QuantityOnHand = s.QuantityOnHand,
        d.BinLocation = s.BinLocation,
        d.LastStocktakeQuantity = s.LastStocktakeQuantity,
        d.LastCostPrice = s.LastCostPrice,
        d.ReorderLevel = s.ReorderLevel,
        d.TargetStockLevel = s.TargetStockLevel,
        d.LastEditedBy = s.LastEditedBy,
        d.LastEditedWhen = COALESCE(s.LastEditedWhen, @Now)
    FROM ' + QUOTENAME(@SchemaName) + N'.StockItemHoldings d
    JOIN ' + QUOTENAME(@SchemaName) + N'.Staging_StockItemHoldings s ON d.StockItemID = s.StockItemID
    WHERE (
        ISNULL(d.QuantityOnHand, 0) <> ISNULL(s.QuantityOnHand, 0) OR
        ISNULL(d.BinLocation, '''') <> ISNULL(s.BinLocation, '''') OR
        ISNULL(d.LastStocktakeQuantity, 0) <> ISNULL(s.LastStocktakeQuantity, 0) OR
        ISNULL(d.LastCostPrice, 0.0) <> ISNULL(s.LastCostPrice, 0.0) OR
        ISNULL(d.ReorderLevel, 0) <> ISNULL(s.ReorderLevel, 0) OR
        ISNULL(d.TargetStockLevel, 0) <> ISNULL(s.TargetStockLevel, 0)
    );
    ';
    EXEC sp_executesql @SQL, 
        N'@Now DATETIME2(3)', 
        @Now = @Now;

    SET @SQL = N'
    INSERT INTO ' + QUOTENAME(@SchemaName) + N'.StockItemHoldings (
        StockItemID, QuantityOnHand, BinLocation, LastStocktakeQuantity,
        LastCostPrice, ReorderLevel, TargetStockLevel, LastEditedBy, LastEditedWhen)
    SELECT s.StockItemID, s.QuantityOnHand, s.BinLocation, s.LastStocktakeQuantity,
           s.LastCostPrice, s.ReorderLevel, s.TargetStockLevel, s.LastEditedBy, COALESCE(s.LastEditedWhen, @Now)
    FROM ' + QUOTENAME(@SchemaName) + N'.Staging_StockItemHoldings s
    WHERE NOT EXISTS (SELECT 1 FROM ' + QUOTENAME(@SchemaName) + N'.StockItemHoldings d WHERE d.StockItemID = s.StockItemID);
    ';
    EXEC sp_executesql @SQL, 
        N'@Now DATETIME2(3)', 
        @Now = @Now;
END