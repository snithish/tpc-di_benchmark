DROP TABLE IF EXISTS master.dim_trade;
CREATE TABLE
    master.dim_trade
(
    TradeID         INT64   NOT NULL,
    --Trade identifier
    SK_BrokerID     INT64,
    --Surrogate key for BrokerID
    SK_CreateDateID INT64   NOT NULL,
    --Surrogate key for date created
    SK_CreateTimeID INT64   NOT NULL,
    --Surrogate key for time created
    SK_CloseDateID  INT64,
    --Surrogate key for date closed
    SK_CloseTimeID  INT64,
    --Surrogate key for time closed
    Status          STRING  NOT NULL,
    --Trade status
    Type            STRING  NOT NULL,
    --Trade type
    CashFlag        BOOLEAN NOT NULL,
    --Is this trade a cash (1) or margin (0) trade?
    SK_SecurityID   INT64   NOT NULL,
    --Surrogate key for SecurityID
    SK_CompanyID    INT64   NOT NULL,
    --Surrogate key for CompanyID
    Quantity        INT64 NOT NULL,
    --Quantity of securities traded.
    BidPrice        NUMERIC NOT NULL,
    --The requested unit price.
    SK_CustomerID   INT64   NOT NULL,
    --Surrogate key for CustomerID
    SK_AccountID    INT64   NOT NULL,
    --Surrogate key for AccountID
    ExecutedBy      STRING  NOT NULL,
    --Name of person executing the trade.
    TradePrice      NUMERIC,
    --Unit price at which the security was traded.
    Fee             NUMERIC,
    --Fee charged for placing this trade request
    Commission      NUMERIC,
    --Commission earned on this trade
    Tax             NUMERIC,
    --Amount of tax due on this trade
    BatchID         INT64   NOT NULL
    --Batch ID when this record was inserted
);