CREATE FUNCTION Security.tvf_IsSalespersonForPeople (@SalespersonPersonID INT)
RETURNS TABLE
WITH SCHEMABINDING 
AS 
RETURN 
    SELECT 1 tvf_securitypredicate_result
    WHERE USER_NAME() IN ('manager@wideworldimporters.com')
    OR EXISTS (
        SELECT 1 FROM WHS.People
        WHERE PersonID = @SalespersonPersonID
        AND EmailAddress = USER_NAME()
        AND IsSalesperson = 'true'
);