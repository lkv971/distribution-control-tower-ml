CREATE FUNCTION Security.tvf_IsSalespersonForOrders (@OrderID INT)
RETURNS TABLE 
WITH SCHEMABINDING
AS
RETURN
    SELECT 1 tvf_securitypredicate_result
    WHERE USER_NAME() IN ('manager@wideworldimporters.com')
    OR EXISTS (
        SELECT 1 FROM WHS.People AS P 
        JOIN WHS.Orders AS O 
        ON P.PersonID = O.SalespersonPersonID 
        WHERE @OrderID = OrderID
        AND EmailAddress = USER_NAME()
        AND IsSalesperson = 'true'
);