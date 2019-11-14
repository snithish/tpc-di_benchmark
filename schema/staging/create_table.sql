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

-- Schema of WatchHistory table -> Refer Page 35 2.2.2.19.1

CREATE TABLE
  staging.watch_history (CDC_FLAG STRING NOT NULL,
    --	‘I’	Rows are only added
    CDC_DSN INT64 NOT NULL,
    --	Database Sequence Number
    W_C_ID INT64 NOT NULL,
    --	Customer identifier
    W_S_SYMB STRING NOT NULL,
    --	Symbol of the security to watch
    W_DTS DATETIME NOT NULL,
    --	Date and Time Stamp for the action
    W_ACTION STRING --	‘ACTV’ or	Whether activating or canceling the watch ‘CNCL’
    );

  ---- Schema of hr table -> Refer Page 31 2.2.2.10.1
CREATE TABLE
  staging.hr ( EmployeeID INT64 NOT NULL,
    -- ID of employee
    ManagerID INT64 NOT NULL,
    -- ID of employee’s manager
    EmployeeFirstName STRING NOT NULL,
    -- First name
    EmployeeLastName STRING NOT NULL,
    -- Last name
    EmployeeMI STRING,
    -- Middle initial
    EmployeeJobCode INT64,
    -- Numeric job code
    EmployeeBranch STRING,
    -- Facility in which employee has office
    EmployeeOffice STRING,
    -- Office number or description
    EmployeePhone STRING --Employee phone number
    );

  ---- Schema of prospect table -> Refer Page 32 2.2.2.12
CREATE TABLE
  staging.prospect ( AgencyID STRING NOT NULL,
    -- Unique identifier from agency
    LastName STRING NOT NULL,
    -- Last name
    FirstName STRING NOT NULL,
    -- First name
    MiddleInitial STRING,
    -- Middle initial
    Gender STRING,
    -- ‘M’ or ‘F’ or ‘U’
    AddressLine1 STRING,
    -- Postal address
    AddressLine2 STRING,
    -- Postal address
    PostalCode STRING,
    -- Postal code
    City STRING NOT NULL,
    -- City
    State STRING NOT NULL,
    -- State OR province
    Country STRING,
    -- Postal country
    Phone STRING,
    -- Telephone number
    Income INT64,
    -- Annual income
    NumberCars INT64,
    -- Cars owned
    NumberChildren INT64,
    -- Dependent children
    MaritalStatus STRING,
    -- ‘S’ or ‘M’ or ‘D’ or ‘W’ or ‘U’
    Age INT64,
    -- Current age
    CreditRating INT64,
    -- Numeric rating
    OwnOrRentFlag STRING,
    -- ‘O’ or ‘R’ or ‘U’
    Employer STRING,
    -- Name of employer
    NumberCreditCards INT64,
    -- Credit cards
    NetWorth INT64 -- Estimated total net worth
    );

  ---- Schema of TradeHistory table -> Refer Page 33 2.2.2.16.1
CREATE TABLE
  staging.TradeHistory ( TH_T_ID INT64 NOT NULL,
    -- Trade identifier. Corresponds to T_ID in the Trade.txt file
    TH_DTS DATETIME NOT NULL,
    -- When the trade history was updated
    TH_ST_ID STRING NOT NULL -- Status type identifier
    );

  ---- Schema of FINWIRE_CMP -> Refer Page 30 2.2.2.8 -> Stores result of finwire_cmp query
CREATE TABLE
    staging.cmp_records(
    -- Posting date & time as YYYYMMDD-HHMMSS
                           PTS DATETIME NOT NULL,
    -- Name of the COMPANY
                           COMPANYNAME STRING NOT NULL,
    -- Company identification code from SEC
                           CIK INT64 NOT NULL,
    -- ‘ACTV’ for Active company, ‘INAC’ for inactive
                           STATUS STRING NOT NULL,
    -- Code for industry segment
                           INDUSTRYID STRING NOT NULL,
    -- S&P rating
                           SPRATING STRING NOT NULL,
    -- FOUNDINGDATE as YYYYMMDD
                           FOUNDINGDATE DATE,
    -- Mailing address Line 1
                           ADDRLINE1 STRING NOT NULL,
    -- Mailing address Line 2
                           ADDRLINE2 STRING,
    -- Mailing address PostalCode
                           POSTALCODE STRING NOT NULL,
    -- Mailing address CITY
                           CITY STRING NOT NULL,
    -- Mailing address STATEPROVINCE
                           STATEPROVINCE STRING NOT NULL,
    -- Mailing address COUNTRY
                           COUNTRY STRING,
    -- Name of company CEO
                           CEONAME STRING NOT NULL,
    -- Description of the company
                           DESCRIPTION STRING NOT NULL );
  ---- Schema of FINWIRE_SEC -> Refer Page 30 2.2.2.8
CREATE TABLE
  staging.sec_records(
    -- Posting date & time as YYYYMMDD-HHMMSS
    PTS STRING NOT NULL,
    -- "SEC” Type
    RECTYPE STRING NOT NULL,
    -- Security symbol
    SYMBOL STRING NOT NULL,
    -- Issue type
    ISSUETYPE STRING NOT NULL,
    -- ‘ACTV’ for Active security, ‘INAC’ for inactive
    STATUS STRING NOT NULL,
    -- Security name
    NAME STRING NOT NULL,
    -- ID of the exchange the security is traded on
    EXID STRING NOT NULL,
    -- Number of shares outstanding
    SHOUT STRING NOT NULL,
    -- Date of first trade as YYYYMMDD
    FIRSTTRADEDATE STRING NOT NULL,
    -- Date of first trade on exchange as YYYYMMDD
    FIRSTTRADEEXCHG STRING NOT NULL,
    -- Dividend as VALUE_T
    DIVIDEND STRING NOT NULL,
    -- Company CIK number (if only digits, 10 chars) or name (if not only digits, 60 chars)
    CONAMEORCIK STRING NOT NULL );
  ---- Schema of FINWIRE_FIN -> Refer Page 31 2.2.2.8
CREATE TABLE
  staging.fin_records(
    -- Posting date & time as YYYYMMDD-HHMMSS
    PTS STRING NOT NULL,
    -- "SEC” Type
    RECTYPE STRING NOT NULL,
    -- Year of the quarter end.
    YEAR STRING NOT NULL,
    -- QUARTER number: valid values are ‘1’, ‘2’, ‘3’, ‘4’
    QUARTER STRING NOT NULL,
    -- Start date of quarter, as YYYYMMDD
    QTRSTARTDATE STRING NOT NULL,
    -- Posting date of quarterly report as YYYYMMDD
    POSTINGDATE STRING NOT NULL,
    -- Reported revenue for the quarter
    REVENUE STRING NOT NULL,
    -- Net earnings reported for the quarter
    EARNINGS STRING NOT NULL,
    -- Basic earnings per share for the quarter
    EPS STRING NOT NULL,
    -- Diluted earnings per share for the quarter
    DILUTEDEPS STRING NOT NULL,
    -- Profit divided by revenues for the quarter
    MARGIN STRING NOT NULL,
    -- Value of inventory on hand at end of quarter
    INVENTORY STRING NOT NULL,
    -- Value of total assets at the end of quarter
    ASSETS STRING NOT NULL,
    -- Value of total liabilities at the end of quarter
    LIABILITIES STRING NOT NULL,
    -- Average number of shares outstanding
    SHOUT STRING NOT NULL,
    -- Average number of shares outstanding (diluted)
    DILUTEDSHOUT STRING NOT NULL,
    -- Company CIK number (if only digits, 10 chars) or name (if not only digits, 60 chars)
    CONAMEORCIK STRING NOT NULL );