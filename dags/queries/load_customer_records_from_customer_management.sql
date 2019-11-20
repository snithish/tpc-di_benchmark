CREATE TEMP FUNCTION
  as_string (val ANY TYPE) AS (IFNULL(SAFE_CAST(val AS STRING),
      ""));
CREATE TEMP FUNCTION
  constructPhoneNumber(phone ANY TYPE) AS (
    CASE
      WHEN phone IS NULL THEN NULL
    ELSE
    CASE
      WHEN phone.C_CTRY_CODE IS NOT NULL AND phone.C_AREA_CODE IS NOT NULL AND phone.C_LOCAL IS NOT NULL THEN CONCAT(as_STRING(phone.C_CTRY_CODE), "(", as_STRING(phone.C_AREA_CODE), ")", as_STRING(phone.C_LOCAL), as_STRING(phone.C_EXT))
      WHEN phone.C_CTRY_CODE IS NULL
    AND phone.C_AREA_CODE IS NOT NULL
    AND phone.C_LOCAL IS NOT NULL THEN CONCAT("(", as_STRING(phone.C_AREA_CODE), ")", as_STRING(phone.C_LOCAL), as_STRING(phone.C_EXT))
      WHEN phone.C_CTRY_CODE IS NULL AND phone.C_AREA_CODE IS NULL AND phone.C_LOCAL IS NOT NULL THEN CONCAT(as_STRING(phone.C_LOCAL), as_STRING(phone.C_EXT))
    ELSE
    NULL
  END
  END
    );
CREATE TEMP FUNCTION
  latest_value( history_of_values ANY TYPE) AS ( (
    SELECT
      excluding_nulls[ORDINAL(ARRAY_LENGTH(excluding_nulls))]
    FROM (
      SELECT
        ARRAY_AGG(x IGNORE NULLS) AS excluding_nulls
      FROM
        UNNEST(history_of_values) x)) );
WITH
  customers AS (
  SELECT
    Customer.ActionTYPE AS Action,
    Customer.ActionTS AS effective_time_stamp,
    constructPhoneNumber(Customer.ContactInfo.C_PHONE_1) AS Phone1,
    constructPhoneNumber(Customer.ContactInfo.C_PHONE_2) AS Phone2,
    constructPhoneNumber(Customer.ContactInfo.C_PHONE_3) AS Phone3,
    Customer.attr_C_TIER AS Tier,
    Customer.attr_C_ID AS CustomerID,
    UPPER(Customer.attr_C_GNDR) AS Gender,
    Customer.ContactInfo.C_PRIM_EMAIL AS Email1,
    Customer.ContactInfo.C_ALT_EMAIL AS Email2,
    Customer.attr_C_TAX_ID AS TaxID,
    Customer.attr_C_DOB AS DOB,
    Customer.Name.C_F_NAME AS FirstName,
    Customer.Name.C_M_NAME AS MiddleInitial,
    Customer.Name.C_L_NAME AS LastName,
    Customer.Address.C_CTRY AS Country,
    Customer.Address.C_CITY AS City,
    Customer.Address.C_ZIPCODE AS PostalCode,
    Customer.Address.C_ADLINE1 AS AddressLine1,
    Customer.Address.C_ADLINE2 AS AddressLine2,
    Customer.Address.C_STATE_PROV AS State_Prov,
    CASE
      WHEN Customer.ActionTYPE = "INACT" THEN "INACTIVE"
    ELSE
    "ACTIVE"
  END
    AS Status,
    Customer.TaxInfo.C_NAT_TX_ID AS NationalTaxID,
    Customer.TaxInfo.C_LCL_TX_ID AS LocalTaxID
  FROM
    staging.customer_management
  WHERE
    Customer.ActionTYPE IN ("NEW",
      "UPDCUST",
      "INACT"))
SELECT
    CustomerID,
    latest_value(ARRAY_AGG(TaxID) OVER w) AS TaxID,
    Status,
    latest_value(ARRAY_AGG(LastName) OVER w) AS LastName,
    latest_value(ARRAY_AGG(FirstName) OVER w) AS FirstName,
    latest_value(ARRAY_AGG(MiddleInitial) OVER w) AS MiddleInitial,
    latest_value(ARRAY_AGG(Gender) OVER w) AS Gender,
    latest_value(ARRAY_AGG(Tier) OVER w) AS Tier,
    latest_value(ARRAY_AGG(DOB) OVER w) AS DOB,
    latest_value(ARRAY_AGG(AddressLine1) OVER w) AS AddressLine1,
    latest_value(ARRAY_AGG(AddressLine2) OVER w) AS AddressLine2,
    latest_value(ARRAY_AGG(PostalCode) OVER w) AS PostalCode,
    latest_value(ARRAY_AGG(City) OVER w) AS City,
    latest_value(ARRAY_AGG(State_Prov) OVER w) AS State_Prov,
    latest_value(ARRAY_AGG(Country) OVER w) AS Country,
    latest_value(ARRAY_AGG(Phone1) OVER w) AS Phone1,
    latest_value(ARRAY_AGG(Phone2) OVER w) AS Phone2,
    latest_value(ARRAY_AGG(Phone3) OVER w) AS Phone3,
    latest_value(ARRAY_AGG(Email1) OVER w) AS Email1,
    latest_value(ARRAY_AGG(Email2) OVER w) AS Email2,
    latest_value(ARRAY_AGG(NationalTaxID) OVER w) AS NationalTaxID,
    latest_value(ARRAY_AGG(LocalTaxID) OVER w) AS LocalTaxID,
    Action,
    DATE (effective_time_stamp) AS EffectiveDate,
    LEAD(DATE (effective_time_stamp), 1, DATE ('9999-12-31')) OVER w AS EndDate
FROM
    customers
    WINDOW
    w AS (
    PARTITION BY
    CustomerID
    ORDER BY
    effective_time_stamp ASC);