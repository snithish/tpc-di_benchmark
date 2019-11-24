DROP TABLE IF EXISTS master.fact_cash_balances;
CREATE TABLE
    master.fact_cash_balances
(
    SK_CustomerID INT64   NOT NULL,
    --Surrogate key for CustomerID
    SK_AccountID  INT64   NOT NULL,
    --Surrogate key for AccountID
    SK_DateID     INT64   NOT NULL,
    --Surrogate key for the date
    Cash          NUMERIC NOT NULL,
    --Cash balance for the account after applying changes for this day
    BatchID       INT64   NOT NULL
    --Batch ID when this record was inserted
);