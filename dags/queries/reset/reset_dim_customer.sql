DROP TABLE IF EXISTS master.dim_customer;
CREATE TABLE
    master.dim_customer
(
    SK_CustomerID       INT64   NOT NULL,
    --Surrogate key for CustomerID
    CustomerID          INT64   NOT NULL,
    --Customer identifier
    TaxID               STRING  NOT NULL,
    --Customer’s tax identifier
    Status              STRING  NOT NULL,
    --Customer status type
    LastName            STRING  NOT NULL,
    --Customer's last name.
    FirstName           STRING  NOT NULL,
    --Customer's first name.
    MiddleInitial       STRING,
    --Customer's middle name initial
    Gender              STRING,
    --Gender of the customer
    Tier                INT64,
    --Customer tier
    DOB                 DATE    NOT NULL,
    --Customer’s date of birth.
    AddressLine1        STRING  NOT NULL,
    --Address Line 1
    AddressLine2        STRING,
    --Address Line 2
    PostalCode          STRING  NOT NULL,
    --Zip or Postal Code
    City                STRING  NOT NULL,
    --City
    StateProv           STRING  NOT NULL,
    --State or Province
    Country             STRING,
    --Country
    Phone1              STRING,
    --Phone number 1
    Phone2              STRING,
    --Phone number 2
    Phone3              STRING,
    --Phone number 3
    Email1              STRING,
    --Email address 1
    Email2              STRING,
    --Email address 2
    NationalTaxRateDesc STRING,
    --National Tax rate description
    NationalTaxRate     NUMERIC,
    --National Tax rate
    LocalTaxRateDesc    STRING,
    --Local Tax rate description
    LocalTaxRate        NUMERIC,
    --Local Tax rate
    AgencyID            STRING,
    --Agency identifier
    CreditRating        INT64,
    --Credit rating
    NetWorth            INT64,
    --Net worth
    MarketingNameplate  STRING,
    --Marketing nameplate
    IsCurrent           BOOLEAN NOT NULL,
    --True if this is the current record
    BatchID             INT64   NOT NULL,
    --Batch ID when this record was inserted
    EffectiveDate       DATE    NOT NULL,
    --Beginning of date range when this record was the current record
    EndDate             DATE    NOT NULL
    --Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31.
);