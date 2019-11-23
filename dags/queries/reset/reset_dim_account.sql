DROP TABLE IF EXISTS master.dim_account;
CREATE TABLE
    master.dim_account
(
    SK_AccountID  INT64   NOT NULL,
    --Surrogate key for AccountID
    AccountID     INT64   NOT NULL,
    --Customer account identifier
    SK_BrokerID   INT64   NOT NULL,
    --Surrogate key of managing broker
    SK_CustomerID INT64   NOT NULL,
    --Surrogate key of customer
    Status        STRING  NOT NULL,
    --Account status, active or closed
    AccountDesc   STRING,
    --Name of customer account
    TaxStatus     INT64   NOT NULL,
    --0, 1 or 2 Tax status of this account
    IsCurrent     BOOLEAN NOT NULL,
    --True if this is the current record
    BatchID       INT64   NOT NULL,
    --Batch ID when this record was inserted
    EffectiveDate DATE    NOT NULL,
    --Beginning of date range when this record was the current record
    EndDate       DATE    NOT NULL
    --Ending of date range when this record was the current record. A record that is not expired willuse the date 9999-12-31.
);