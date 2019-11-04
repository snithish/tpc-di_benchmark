-- Schema definitions file http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-di_v1.1.0.pdf

-- Schema of account table -> Refer Page 21 2.2.2.1.2

CREATE TABLE
  staging.account( CDC_FLAG STRING NOT NULL,
    CDC_DSN INT64 NOT NULL,
    CA_ID INT64 NOT NULL,
    -- Customer account identifier
    CA_B_ID INT64 NOT NULL,
    -- Identifier of the managing broker
    CA_C_ID INT64 NOT NULL,
    -- Owning customer identifier
    CA_NAME STRING,
    -- Name of customer account
    CA_TAX_ST INT64 NOT NULL,
    -- 0, 1 or 2 Tax status of this account
    CA_ST_ID STRING NOT NULL-- ‘ACTV’ or ‘INAC’ Customer status type identifier
    );

-- Schema of CashTransaction table -> Refer Page 22 2.2.2.3.1

CREATE TABLE
  staging.cash_transaction ( CDC_FLAG STRING NOT NULL,
    -- L ‘I’	Denotes insert
    CDC_DSN INT64 NOT NULL,
    --	Database Sequence Number
    CT_CA_ID INT64 NOT NULL,
    --	Customer account identifier
    CT_DTS DATETIME NOT NULL,
    --	Timestamp of when the trade took place
    CT_AMT FLOAT64 NOT NULL,
    --	Amount of the cash transaction.
    CT_NAME STRING NOT NULL --	Transaction name, or description: e.g. “Cash from sale of DuPont stock”.
    );