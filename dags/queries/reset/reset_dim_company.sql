DROP TABLE IF EXISTS master.dim_company;
CREATE TABLE
    master.dim_company
(
    SK_CompanyID  INT64   NOT NULL,
    --Surrogate key for CompanyID
    CompanyID     INT64   NOT NULL,
    --Company identifier (CIK number)
    Status        STRING  NOT NULL,
    --Company status
    Name          STRING  NOT NULL,
    --Company name
    Industry      STRING  NOT NULL,
    --Company’s industry
    SPrating      STRING,
    --Standard & Poor company’s rating
    isLowGrade    BOOLEAN,
    --True if this company is low grade
    CEO           STRING  NOT NULL,
    --CEO name
    AddressLine1  STRING,
    --Address Line 1
    AddressLine2  STRING,
    --Address Line 2
    PostalCode    STRING  NOT NULL,
    --Zip or postal code
    City          STRING  NOT NULL,
    --City
    StateProv     STRING  NOT NULL,
    --State or Province
    Country       STRING,
    Description   STRING  NOT NULL,
    --Company description
    FoundingDate  DATE,
    --the company was founded
    IsCurrent     BOOLEAN NOT NULL,
    --True if this is the current record
    BatchID       INT64   NOT NULL,
    --Batch ID when this record was inserted
    EffectiveDate DATE    NOT NULL,
    --Beginning of date range when this record was the current record
    EndDate       DATE    NOT NULL
    --Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31.
);