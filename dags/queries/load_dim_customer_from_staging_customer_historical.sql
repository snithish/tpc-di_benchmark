SELECT CAST(CONCAT(FORMAT_DATE('%E4Y%m%d',
                               c.EffectiveDate), '', CAST(c.CustomerID AS STRING)) AS INT64) AS SK_CustomerID,
       c.CustomerID                                                                          AS CustomerID,
       c.TaxID                                                    AS TaxID,
       c.Status                                                   AS Status,
       c.LastName                                                 AS LastName,
       c.FirstName                                                AS FirstName,
       c.MiddleInitial                                            AS MiddleInitial,
       IF((UPPER(c.Gender)) IN ("M", "F"), UPPER(c.Gender), "U") AS Gender,
       c.Tier                                                     AS Tier,
       c.DOB                                                      AS DOB,
       c.AddressLine1                                             AS AddressLine1,
       c.AddressLine2                                             AS AddressLine2,
       c.PostalCode                                               AS PostalCode,
       c.City                                                                                AS City,
       c.State_Prov                                                                          AS StateProv,
       c.Country                                                                             AS Country,
       c.Phone1                                                                              AS Phone1,
       c.Phone2                                                                              AS Phone2,
       c.Phone3                                                                              AS Phone3,
       c.Email1                                                                              AS Email1,
       c.Email2                                                                              AS Email2,
       national.TX_NAME                                                                      AS NationalTaxRateDesc,
       national.TX_RATE                                                                      AS NationalTaxRate,
       local.TX_NAME                                                                         AS LocalTaxRateDesc,
       local.TX_RATE                                                                         AS LocalTaxRate,
       p.AgencyID                                                                            AS AgencyID,
       p.CreditRating                                                                        AS CreditRating,
       p.NetWorth                                                                            AS NetWorth,
       p.MarketingNameplate                                                                  AS MarketingNameplate,
       c.EndDate = DATE('9999-12-31')                                                        AS IsCurrent,
       1                                                                                     AS BatchID,
       c.EffectiveDate                                                                       AS EffectiveDate,
       c.EndDate                                                                             AS EndDate
FROM ((staging.customer_historical c
    LEFT OUTER JOIN master.prospect p ON UPPER(p.FirstName) = UPPER(c.FirstName)
        AND UPPER(p.LastName) = UPPER(c.LastName)
        AND UPPER(p.AddressLine1) = UPPER(c.AddressLine1)
        AND UPPER(p.AddressLine2) = UPPER(c.AddressLine2)
        AND UPPER(p.PostalCode) = UPPER(c.PostalCode) AND c.EndDate = DATE('9999-12-31'))
    JOIN master.tax_rate national ON national.TX_ID = NationalTaxID)
         JOIN master.tax_rate local ON local.TX_ID = LocalTaxID;