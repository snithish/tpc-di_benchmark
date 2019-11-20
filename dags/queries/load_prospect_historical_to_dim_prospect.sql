CREATE TEMPORARY FUNCTION
  construct_MarketingNameplate(Income FLOAT64,
    NumberCars FLOAT64,
    NumberChildren FLOAT64,
    Age FLOAT64,
    CreditRating FLOAT64,
    NumberCreditCards FLOAT64,
    NetWorth FLOAT64)
  RETURNS STRING
  LANGUAGE js AS """
result = [];
if ((NetWorth !== null && NetWorth > 1000000) || (Income !== null && Income > 200000)) result.push("HighValue");
if ((NumberChildren !== null && NumberChildren > 3) || (NumberCreditCards !== null && NumberCreditCards > 5)) result.push("Expenses");
if ((Income !== null && Income < 50000) || (CreditRating !== null && CreditRating < 600) || (NetWorth !== null && NetWorth < 100000)) result.push("MoneyAlert");
if ((NumberCars !== null && NumberCars > 3) || (NumberCreditCards !== null && NumberCreditCards > 7)) result.push("Spender");
if ((Age !== null && Age < 25) && (NetWorth != null && NetWorth > 1000000)) result.push("Inherited");
if (result.length == 0)
    return null;
return result.join("+");
    """;
WITH
    batch_date AS (
        SELECT
            SK_DateID
        FROM
            master.dim_date d
                JOIN
            staging.batch_date bd
            ON
                    d.DateValue = bd.BatchDate)
SELECT
    p.AgencyID AS AgencyID,
    bd.SK_DateID AS SK_RecordDateID,
    bd.SK_DateID AS SK_UpdateDateID,
    1 AS BatchID,
    c.CustomerID IS NOT NULL AS IsCustomer,
    p.LastName AS LastName,
    p.FirstName AS FirstName,
    p.MiddleInitial AS MiddleInitial,
    p.Gender AS Gender,
    p.AddressLine1 AS AddressLine1,
    p.AddressLine2 AS AddressLine2,
    p.PostalCode AS PostalCode,
    p.City AS City,
    p.State AS State,
    p.Country AS Country,
    p.Phone AS Phone,
    p.Income AS Income,
    p.NumberCars AS NumberCars,
    p.NumberChildren AS NumberChildren,
    p.MaritalStatus AS MaritalStatus,
    p.Age AS Age,
    p.CreditRating AS CreditRating,
    p.OwnOrRentFlag AS OwnOrRentFlag,
    p.Employer AS Employer,
    p.NumberCreditCards AS NumberCreditCards,
    p.NetWorth AS NetWorth,
    construct_MarketingNameplate(p.Income,
                                 p.NumberCars,
                                 p.NumberChildren,
                                 p.Age,
                                 p.CreditRating,
                                 p.NumberCreditCards,
                                 p.NetWorth) AS MarketingNameplate
FROM (staging.prospect p
    LEFT OUTER JOIN
    staging.customer_historical c
    ON
                UPPER(p.FirstName) = UPPER(c.FirstName)
            AND UPPER(p.LastName) = UPPER(c.LastName)
            AND UPPER(p.AddressLine1) = UPPER(c.AddressLine1)
            AND UPPER(p.AddressLine2) = UPPER(c.AddressLine2)
            AND UPPER(p.PostalCode) = UPPER(c.PostalCode)
            AND c.EndDate = DATE('9999-12-31')
            AND c.Status = 'ACTIVE')
         CROSS JOIN
     batch_date bd;