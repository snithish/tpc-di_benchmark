DROP TABLE IF EXISTS master.dim_security;
CREATE TABLE
    master.dim_security
(
    SK_SecurityID        INT64   NOT NULL,
    --Surrogate key for Symbol
    Symbol               STRING  NOT NULL,
    --Identifies security on “ticker”
    Issue                STRING  NOT NULL,
    --Issue type
    Status               STRING  NOT NULL,
    --Status type
    Name                 STRING  NOT NULL,
    --Security name
    ExchangeID           STRING  NOT NULL,
    --Exchange the security is traded on
    SK_CompanyID         INT64   NOT NULL,
    --Company issuing security
    SharesOutstanding    INT64   NOT NULL,
    --Shares outstanding
    FirstTrade           DATE    NOT NULL,
    --Date of first trade
    FirstTradeOnExchange DATE    NOT NULL,
    --Date of first trade on this exchange
    Dividend             NUMERIC NOT NULL,
    --Annual dividend per share
    IsCurrent            BOOLEAN NOT NULL,
    --True if this is the current record
    BatchID              INT64   NOT NULL,
    --Batch ID when this record was inserted
    EffectiveDate        DATE    NOT NULL,
    --Beginning of date range when this record was the current record
    EndDate              DATE    NOT NULL
    --Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31.
);