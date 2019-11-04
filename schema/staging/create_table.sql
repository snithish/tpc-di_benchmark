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

-- Schema of Customer table -> Refer Page 22 2.2.2.4.1

CREATE TABLE
  staging.customer ( CDC_FLAG STRING NOT NULL,
    --	‘I’ or ‘U’	Denotes insert or update
    CDC_DSN INT64 NOT NULL,
    --	Database Sequence Number
    C_ID INT64 NOT NULL,
    --	Customer identifier
    C_TAX_ID STRING NOT NULL,
    --	Customer’s tax identifier
    C_ST_ID STRING,
    --	‘ACTV’ or	Customer status type identifier ‘INAC’
    C_L_NAME STRING NOT NULL,
    --	Primary Customer's last name.
    C_F_NAME STRING NOT NULL,
    --	Primary Customer's first name.
    C_M_NAME STRING,
    --	Primary Customer's middle initial
    C_GNDR STRING,
    --	Gender of the primary customer
    C_TIER INT64,
    -- Customer tier
    C_DOB DATE NOT NULL,
    --	Customer’s date of birth, as YYYY-MM-DD.
    C_ADLINE1 STRING NOT NULL,
    --	Address Line 1
    C_ADLINE2 STRING,
    --	Address Line 2
    C_ZIPCODE STRING NOT NULL,
    --	Zip or postal code
    C_CITY STRING NOT NULL,
    --	City
    C_STATE_PRO STRING NOT NULL,
    --	State or province V
    C_CTRY STRING,
    --	Country
    C_CTRY_1 STRING,
    --	Country code for Customer's phone 1.
    C_AREA_1 STRING,
    --	Area code for customer’s phone 1.
    C_LOCAL_1 STRING,
    --	Local number for customer’s phone 1.
    C_EXT_1 STRING,
    --	Extension number for Customer’s phone 1.
    C_CTRY_2 STRING,
    --	Country code for Customer's phone 2.
    C_AREA_2 STRING,
    --	Area code for Customer’s phone 2.
    C_LOCAL_2 STRING,
    --	Local number for Customer’s phone 2.
    C_EXT_2 STRING,
    --	Extension number for Customer’s phone 2.
    C_CTRY_3 STRING,
    --	Country code for Customer's phone 3.
    C_AREA_3 STRING,
    --	Area code for Customer’s phone 3.
    C_LOCAL_3 STRING,
    --	Local number for Customer’s phone 3.
    C_EXT_3 STRING,
    --	Extension number for Customer’s phone 3.
    C_EMAIL_1 STRING,
    --	Customer's e-mail address 1.
    C_EMAIL_2 STRING,
    --	Customer's e-mail address 2.
    C_LCL_TX_ID STRING NOT NULL,
    --	Customer's local tax rate
    C_NAT_TX_ID STRING NOT NULL --	Customer's national tax rate
    );

-- Schema of DailyMarket table -> Refer Page 28 2.2.2.6.1

CREATE TABLE
  staging.daily_market (CDC_FLAG STRING NOT NULL,
    --	‘I’	Denotes insert
    CDC_DSN INT64 NOT NULL,
    --	Database Sequence Number
    DM_DATE DATE NOT NULL,
    --	Date of last completed trading day.
    DM_S_SYMB STRING NOT NULL,
    --	Security symbol of the security
    DM_CLOSE NUMERIC NOT NULL,
    --	Closing price of the security on this day.
    DM_HIGH NUMERIC NOT NULL,
    --	Highest price for the secuirity on this day.
    DM_LOW NUMERIC NOT NULL,
    --	Lowest price for the security on this day.
    DM_VOL INT64 NOT NULL --	Volume of the security on this day.
    );

-- Schema of HoldingHistory table -> Refer Page 31 2.2.2.9.1

CREATE TABLE
  staging.holding_history (CDC_FLAG STRING NOT NULL,
    -- ‘I’	Denotes insert
    CDC_DSN INT64 NOT NULL,
    --	Database Sequence Number
    HH_H_T_ID INT64 NOT NULL,
    --	Trade Identifier of the trade that originally created the holding row.
    HH_T_ID INT64 NOT NULL,
    --	Trade Identifier of the current trade
    HH_BEFORE_QTY INT64 NOT NULL,
    --	Quantity of this security held before the modifying trade.
    HH_AFTER_QTY INT64 NOT NULL --	Quantity of this security held after the modifying trade.
    );

-- Schema of Trade table -> Refer Page 34 2.2.2.17.1

CREATE TABLE
  staging.trade (CDC_FLAG STRING NOT NULL,
    -- ‘I’,‘U’Denotes insert, update
    CDC_DSN INT64 NOT NULL,
    -- Database Sequence Number
    T_ID INT64 NOT NULL,
    -- Trade identifier.
    T_DTS DATETIME NOT NULL,
    -- Date and time of trade.
    T_ST_ID STRING NOT NULL,
    -- Status type identifier
    T_TT_ID STRING NOT NULL,
    -- Trade type identifier
    T_IS_CASH BOOLEAN,
    -- ‘0’ or ’1’ Is thistrade a cash (‘1’) or margin (‘0’)trade?
    T_S_SYMB STRING NOT NULL,
    -- Security symbol of the security
    T_QTY INT64,
    -- >0Quantity of securities traded
    T_BID_PRICE NUMERIC,
    --> 0The requested unit price.
    T_CA_ID INT64 NOT NULL,
    -- Customer account identifier.
    T_EXEC_NAME STRING NOT NULL,
    -- Name of the person executing the trade.
    T_TRADE_PRICE NUMERIC,
    -- null except in CMPT records, then > 0Unit price at which the security wastraded.
    T_CHRG NUMERIC,
    -- Null except in CMPT records, then >= 0Fee charged forplacing this traderequest.
    T_COMM NUMERIC,
    -- Null except in CMPT records, then >= 0Commission earned on this trade
    T_TAX NUMERIC -- Null except in CMPT records, then >= 0Amount of tax due on this trade
    );