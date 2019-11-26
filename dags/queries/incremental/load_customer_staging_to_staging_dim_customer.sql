CREATE
TEMPORARY
FUNCTION
    as_string(val ANY TYPE) AS (IFNULL(SAFE_CAST(val AS STRING),
    ""));
CREATE TEMPORARY
FUNCTION
    constructPhoneNumber(C_CTRY_CODE STRING, C_AREA_CODE STRING, C_LOCAL STRING, C_EXT STRING) AS (
    CASE
    WHEN C_CTRY_CODE IS NOT NULL AND C_AREA_CODE IS NOT NULL AND C_LOCAL IS NOT NULL THEN CONCAT(as_STRING(C_CTRY_CODE), "(", as_STRING(C_AREA_CODE), ")", as_STRING(C_LOCAL), as_STRING(C_EXT))
    WHEN C_CTRY_CODE IS NULL
    AND C_AREA_CODE IS NOT NULL
    AND C_LOCAL IS NOT NULL THEN CONCAT("(", as_STRING(C_AREA_CODE), ")", as_STRING(C_LOCAL), as_STRING(C_EXT))
    WHEN C_CTRY_CODE IS NULL AND C_AREA_CODE IS NULL AND C_LOCAL IS NOT NULL THEN CONCAT(as_STRING(C_LOCAL), as_STRING(C_EXT))
    ELSE
    NULL
    END
    );
WITH recent_customer AS (
    SELECT a.*
    FROM (SELECT *, row_number() over (PARTITION BY C_ID ORDER BY CDC_DSN DESC ) as row_num
          FROM staging.customer) a
    WHERE a.row_num = 1
),
     enriched AS (SELECT C_ID                                                         AS CustomerID,
                         C_TAX_ID                                                     AS TaxID,
                         st.ST_NAME                                                   AS Status,
                         C_L_NAME                                                     AS LastName,
                         C_F_NAME                                                     AS FirstName,
                         C_M_NAME                                                     AS MiddleInitial,
                         IF((UPPER(C_GNDR)) IN ("M", "F"), UPPER(C_GNDR), "U")        AS Gender,
                         C_TIER                                                       AS Tier,
                         C_DOB                                                        AS DOB,
                         C_ADLINE1                                                    AS AddressLine1,
                         C_ADLINE2                                                    AS AddressLine2,
                         C_ZIPCODE                                                    AS PostalCode,
                         C_CITY                                                       AS City,
                         C_STATE_PRO                                                  AS StateProv,
                         C_CTRY                                                       AS Country,
                         constructPhoneNumber(C_CTRY_1, C_AREA_1, C_LOCAL_1, C_EXT_1) AS Phone1,
                         constructPhoneNumber(C_CTRY_2, C_AREA_2, C_LOCAL_2, C_EXT_2) AS Phone2,
                         constructPhoneNumber(C_CTRY_3, C_AREA_3, C_LOCAL_3, C_EXT_3) AS Phone3,
                         C_EMAIL_1                                                    AS Email1,
                         C_EMAIL_2                                                    AS Email2,
                         national.TX_NAME                                             AS NationalTaxRateDesc,
                         national.TX_RATE                                             AS NationalTaxRate,
                         lo.TX_NAME                                                   AS LocalTaxRateDesc,
                         lo.TX_RATE                                                   AS LocalTaxRate
                  FROM recent_customer c
                           JOIN master.tax_rate national ON national.TX_ID = c.C_NAT_TX_ID
                           JOIN master.tax_rate lo
                                ON lo.TX_ID = c.C_LCL_TX_ID
                           JOIN master.status_type st ON c.C_ST_ID = st.ST_ID),
     prospect AS (
         SELECT p.AgencyID,
                p.CreditRating,
                p.NetWorth,
                p.FirstName,
                p.LastName,
                p.AddressLine1,
                p.AddressLine2,
                p.PostalCode,
                p.MarketingNameplate
         FROM master.prospect p
     )
SELECT CAST(CONCAT(FORMAT_DATE('%E4Y%m%d',
                               bd.BatchDate), '', CAST(c.CustomerID AS STRING)) AS INT64) AS SK_CustomerID,
       c.CustomerID,
       c.TaxID,
       c.Status,
       c.LastName,
       c.FirstName,
       c.MiddleInitial,
       c.Gender,
       c.Tier,
       c.DOB,
       c.AddressLine1,
       c.AddressLine2,
       c.PostalCode,
       c.City,
       c.StateProv,
       c.Country,
       c.Phone1,
       c.Phone2,
       c.Phone3,
       c.Email1,
       c.Email2,
       c.NationalTaxRateDesc,
       c.NationalTaxRate,
       c.LocalTaxRateDesc,
       c.LocalTaxRate,
       p.AgencyID,
       p.CreditRating,
       p.NetWorth,
       p.MarketingNameplate,
       True                                                                               AS IsCurrent,
       bn.batch_id                                                                        AS BatchID,
       bd.BatchDate                                                                       AS EffectiveDate,
       DATE('9999-12-31')                                                                    EndDate
FROM enriched c
         LEFT JOIN prospect p ON UPPER(p.FirstName) = UPPER(c.FirstName)
    AND UPPER(p.LastName) = UPPER(c.LastName)
    AND UPPER(p.AddressLine1) = UPPER(c.AddressLine1)
    AND UPPER(p.AddressLine2) = UPPER(c.AddressLine2)
    AND UPPER(p.PostalCode) = UPPER(c.PostalCode)
         CROSS JOIN staging.batch_date bd
         CROSS JOIN staging.batch_number bn;