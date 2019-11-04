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