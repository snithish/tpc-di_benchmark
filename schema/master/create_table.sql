---- Schema of FactMarketHistory table -> Refer Page 43 3.2.11
CREATE TABLE
  master.fact_market_history ( SK_SecurityID INT64 NOT NULL,
    --Surrogate key for SecurityID
    SK_CompanyID INT64 NOT NULL,
    -- Surrogate key for CompanyID
    SK_DateID INT64 NOT NULL,
    -- Surrogate key for the date
    PERatio NUMERIC,
    -- Price to earnings per share ratio
    Yield NUMERIC NOT NULL,
    -- Dividend to price ratio, as a percentage
    FiftyTwoWeekHigh NUMERIC NOT NULL,
    -- Security highest price in last 52 weeks from this day
    SK_FiftyTwoWeekHighDate INT64 NOT NULL,
    -- Earliest date on which the 52 week high price was set
    FiftyTwoWeekLow NUMERIC NOT NULL,
    -- Security lowest price in last 52 weeks from this day
    SK_FiftyTwoWeekLowDate INT64 NOT NULL,
    -- Earliest date on which the 52 week low price was set
    ClosePrice NUMERIC NOT NULL,
    -- Security closing price on this day
    DayHigh NUMERIC NOT NULL,
    -- Highest price for the security on this day
    DayLow NUMERIC NOT NULL,
    -- Lowest price for the security on this day
    Volume INT64 NOT NULL,
    -- Trading volume of the security on this day
    BatchID INT64 NOT NULL -- Batch ID when this record was inserted
    );
  ---- Schema of financial table -> Refer Page 44 3.2.14
CREATE TABLE
  master.financial ( SK_CompanyID INT64 NOT NULL,
    -- Company SK
    FI_YEAR INT64 NOT NULL,
    -- Year of the quarter end
    FI_QTR INT64 NOT NULL,
    -- Quarter number that the financial information is for: valid values 1, 2, 3, 4
    FI_QTR_START_DATE DATE NOT NULL,
    -- Start date of quarter
    FI_REVENUE NUMERIC NOT NULL,
    -- Reported revenue for the quarter
    FI_NET_EARN NUMERIC NOT NULL,
    -- Net earnings reported for the quarter
    FI_BASIC_EPS NUMERIC NOT NULL,
    -- Basic earnings per share for the quarter
    FI_DILUT_EPS NUMERIC NOT NULL,
    -- Diluted earnings per share for the quarter
    FI_MARGIN NUMERIC NOT NULL,
    -- Profit divided by revenues for the quarter
    FI_INVENTORY NUMERIC NOT NULL
    -- Value of inventory on hand at the end of quarter
    );
  ---- Schema of prospect table -> Refer Page 45 3.2.15
  DROP TABLE IF EXISTS master.prospect;
CREATE TABLE
  master.prospect ( AgencyID STRING NOT NULL,
    -- Unique identifier from agency
    SK_RecordDateID INT64 NOT NULL,
    -- Last date this prospect appeared in input
    SK_UpdateDateID INT64 NOT NULL,
    -- Latest change date for this prospect
    BatchID INT64 NOT NULL,
    -- Batch ID when this record was last modified
    IsCustomer BOOLEAN NOT NULL,
    -- True if this person is also in DimCustomer, else False
    LastName STRING NOT NULL,
    -- Last name
    FirstName STRING NOT NULL,
    -- First name
    MiddleInitial STRING,
    -- Middle initial
    Gender STRING,
    -- M / F / U
    AddressLine1 STRING,
    -- Postal address
    AddressLine2 STRING,
    -- Postal address
    PostalCode STRING,
    -- Postal code
    City STRING NOT NULL,
    -- City
    State STRING NOT NULL,
    -- State or province
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
    -- S / M / D / W / U
    Age INT64,
    -- Current age
    CreditRating INT64,
    -- Numeric rating
    OwnOrRentFlag STRING,
    -- O / R / U
    Employer STRING,
    -- Name of employer
    NumberCreditCards INT64,
    -- Credit cards
    NetWorth INT64,
    -- Estimated total net worth
    MarketingNameplate STRING
    -- For marketing purposes
    );

---- Schema of Industry table -> Refer Page 44 3.2.16
CREATE TABLE
  master.industry ( IN_ID STRING NOT NULL,
    -- Industry code
    IN_NAME STRING NOT NULL,
    -- Industry description
    IN_SC_ID STRING NOT NULL -- Sector identifier
    );


---- Schema of StatusType table -> Refer Page 45 3.2.19

CREATE TABLE
  master.status_type ( ST_ID STRING NOT NULL,
    -- Status code
    ST_NAME STRING NOT NULL -- Status description
    );

  ---- Schema of TaxRate table -> Refer Page 45 3.2.20
CREATE TABLE
  master.tax_rate ( TX_ID STRING NOT NULL,
    -- Tax rate code
    TX_NAME STRING NOT NULL,
    -- Tax rate description
    TX_RATE NUMERIC NOT NULL -- Tax rate
    );

  ---- Schema of TradeType table -> Refer Page 46 3.2.21
CREATE TABLE
  master.trade_type ( TT_ID STRING NOT NULL,
    -- Trade type code
    TT_NAME STRING NOT NULL,
    -- Trade type description
    TT_IS_SELL INT64 NOT NULL,
    -- Flag indicating a sale
    TT_IS_MRKT INT64 NOT NULL -- Flag indicating a market order
    );

CREATE TABLE
  master.dim_date ( SK_DateID INT64 NOT NULL,
    -- Surrogate key for the date
    DateValue DATE NOT NULL,
    --  The date as text, e.g. “2004-07-07”
    DateDesc STRING NOT NULL,
    --The date Month Day, YYYY, e.g. July 7, 2004
    CalendarYearID INT64 NOT NULL,
    -- Year number as a number
    CalendarYearDesc STRING NOT NULL,
    -- Year number as text
    CalendarQtrID INT64 NOT NULL,
    -- Quarter as a number, e.g. 20042
    CalendarQtrDesc STRING NOT NULL,
    -- Quarter as text, e.g. “2004 Q2”
    CalendarMonthID INT64 NOT NULL,
    -- Month as a number, e.g. 20047
    CalendarMonthDesc STRING NOT NULL,
    -- Month as text, e.g. “2004 July”
    CalendarWeekID INT64 NOT NULL,
    -- Week as a number, e.g. 200428
    CalendarWeekDesc STRING NOT NULL,
    -- Week as text, e.g. “2004-W28”
    DayOfWeekNum INT64 NOT NULL,
    -- Day of week as a number, e.g. 3
    DayOfWeekDesc STRING NOT NULL,
    -- Day of week as text, e.g. “Wednesday”
    FiscalYearID INT64 NOT NULL,
    -- Fiscal year as a number, e.g. 2005
    FiscalYearDesc STRING NOT NULL,
    -- Fiscal year as text, e.g. “2005”
    FiscalQtrID INT64 NOT NULL,
    -- Fiscal quarter as a number, e.g. 20051
    FiscalQtrDesc STRING NOT NULL,
    -- Fiscal quarter as text, e.g. “2005 Q1”
    HolidayFlag BOOLEAN -- Indicates holidays
    );

CREATE TABLE
  master.dim_time ( INT64imeID INT64 NOT NULL,
    -- Surrogate key for the time
    TimeValue STRING NOT NULL,
    -- The time as text, e.g. “01:23:45”
    HourID INT64 NOT NULL,
    -- Hour number as a number, e.g. 01
    HourDesc STRING NOT NULL,
    -- Hour number as text, e.g. “01”
    MinuteID INT64 NOT NULL,
    -- Minute as a number, e.g. 23
    MinuteDesc STRING NOT NULL,
    -- Minute as text, e.g. “01:23”
    SecondID INT64 NOT NULL,
    -- Second as a number, e.g. 45
    SecondDesc STRING NOT NULL,
    -- Second as text, e.g. “01:23:45”
    MarketHoursFlag BOOLEAN,
    -- Indicates a time during market hours
    OfficeHoursFlag BOOLEAN -- Indicates a time during office hours
    );

  -- Schema of DimAccount table -> Refer Page 38 3.2.1
CREATE TABLE
  master.dim_account(SK_AccountID INT64 NOT NULL,
    --Surrogate key for AccountID
    AccountID INT64 NOT NULL,
    --Customer account identifier
    SK_BrokerID INT64 NOT NULL,
    --Surrogate key of managing broker
    SK_CustomerID INT64 NOT NULL,
    --Surrogate key of customer
    Status STRING NOT NULL,
    --Account status, active or closed
    AccountDesc STRING,
    --Name of customer account
    TaxStatus INT64 NOT NULL,
    --0, 1 or 2 Tax status of this account
    IsCurrent BOOLEAN NOT NULL,
    --True if this is the current record
    BatchID INT64 NOT NULL,
    --Batch ID when this record was inserted
    EffectiveDate DATE NOT NULL,
    --Beginning of date range when this record was the current record
    EndDate DATE NOT NULL
    --Ending of date range when this record was the current record. A record that is not expired willuse the date 9999-12-31.
    );
  -- Schema of DimBroker table -> Refer Page 38 3.2.2
CREATE TABLE
  master.dim_broker(SK_BrokerID INT64 NOT NULL,
    --Surrogate key for broker
    BrokerID INT64 NOT NULL,
    --Natural key for broker
    ManagerID INT64,
    --Natural key for manager’s HR record
    FirstName STRING NOT NULL,
    --First name
    LastName STRING NOT NULL,
    --Last Name
    MiddleInitial STRING,
    --Middle initial
    Branch STRING,
    --Facility in which employee has office
    Office STRING,
    --Office number or description
    Phone STRING,
    --Employee phone number
    IsCurrent BOOLEAN NOT NULL,
    --True if this is the current record
    BatchID INT64 NOT NULL,
    --Batch ID when this record was inserted
    EffectiveDate DATE NOT NULL,
    --Beginning of date range when this record was the current record
    EndDate DATE NOT NULL
    --Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31.
    );
  -- Schema of DimCompany table -> Refer Page 39 3.2.3
CREATE TABLE
  master.dim_company(SK_CompanyID INT64 NOT NULL,
    --Surrogate key for CompanyID
    CompanyID INT64 NOT NULL,
    --Company identifier (CIK number)
    Status STRING NOT NULL,
    --Company status
    Name STRING NOT NULL,
    --Company name
    Industry STRING NOT NULL,
    --Company’s industry
    SPrating STRING,
    --Standard & Poor company’s rating
    isLowGrade BOOLEAN,
    --True if this company is low grade
    CEO STRING NOT NULL,
    --CEO name
    AddressLine1 STRING,
    --Address Line 1
    AddressLine2 STRING,
    --Address Line 2
    PostalCode STRING NOT NULL,
    --Zip or postal code
    City STRING NOT NULL,
    --City
    StateProv STRING NOT NULL,
    --State or Province
    Country STRING,
    Description STRING NOT NULL,
    --Company description
    FoundingDate DATE,
    --the company was founded
    IsCurrent BOOLEAN NOT NULL,
    --True if this is the current record
    BatchID INT64 NOT NULL,
    --Batch ID when this record was inserted
    EffectiveDate DATE NOT NULL,
    --Beginning of date range when this record was the current record
    EndDate DATE NOT NULL
    --Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31.
    );
  -- Schema of DimCustomer table -> Refer Page 39 3.2.4
DROP TABLE IF EXISTS master.dim_customer;
CREATE TABLE
  master.dim_customer(SK_CustomerID INT64 NOT NULL,
    --Surrogate key for CustomerID
    CustomerID INT64 NOT NULL,
    --Customer identifier
    TaxID STRING NOT NULL,
    --Customer’s tax identifier
    Status STRING NOT NULL,
    --Customer status type
    LastName STRING NOT NULL,
    --Customer's last name.
    FirstName STRING NOT NULL,
    --Customer's first name.
    MiddleInitial STRING,
    --Customer's middle name initial
    Gender STRING,
    --Gender of the customer
    Tier INT64,
    --Customer tier
    DOB DATE NOT NULL,
    --Customer’s date of birth.
    AddressLine1 STRING NOT NULL,
    --Address Line 1
    AddressLine2 STRING,
    --Address Line 2
    PostalCode STRING NOT NULL,
    --Zip or Postal Code
    City STRING NOT NULL,
    --City
    StateProv STRING NOT NULL,
    --State or Province
    Country STRING,
    --Country
    Phone1 STRING,
    --Phone number 1
    Phone2 STRING,
    --Phone number 2
    Phone3 STRING,
    --Phone number 3
    Email1 STRING,
    --Email address 1
    Email2 STRING,
    --Email address 2
    NationalTaxRateDesc STRING,
    --National Tax rate description
    NationalTaxRate NUMERIC,
    --National Tax rate
    LocalTaxRateDesc STRING,
    --Local Tax rate description
    LocalTaxRate NUMERIC,
    --Local Tax rate
    AgencyID STRING,
    --Agency identifier
    CreditRating INT64,
    --Credit rating
    NetWorth INT64,
    --Net worth
    MarketingNameplate STRING,
    --Marketing nameplate
    IsCurrent BOOLEAN NOT NULL,
    --True if this is the current record
    BatchID INT64 NOT NULL,
    --Batch ID when this record was inserted
    EffectiveDate DATE NOT NULL,
    --Beginning of date range when this record was the current record
    EndDate DATE NOT NULL
    --Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31.
    );
  -- Schema of DimSecurity table -> Refer Page 41 3.2.6
CREATE TABLE
  master.dim_security(SK_SecurityID INT64 NOT NULL,
    --Surrogate key for Symbol
    Symbol STRING NOT NULL,
    --Identifies security on “ticker”
    Issue STRING NOT NULL,
    --Issue type
    Status STRING NOT NULL,
    --Status type
    Name STRING NOT NULL,
    --Security name
    ExchangeID STRING NOT NULL,
    --Exchange the security is traded on
    SK_CompanyID INT64 NOT NULL,
    --Company issuing security
    SharesOutstanding INT64 NOT NULL,
    --Shares outstanding
    FirstTrade DATE NOT NULL,
    --Date of first trade
    FirstTradeOnExchange DATE NOT NULL,
    --Date of first trade on this exchange
    Dividend NUMERIC NOT NULL,
    --Annual dividend per share
    IsCurrent BOOLEAN NOT NULL,
    --True if this is the current record
    BatchID INT64 NOT NULL,
    --Batch ID when this record was inserted
    EffectiveDate DATE NOT NULL,
    --Beginning of date range when this record was the current record
    EndDate DATE NOT NULL
    --Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31.
    );
  -- Schema of DimTime table -> Refer Page 41 3.2.7
CREATE TABLE
  master.dim_time(SK_TimeID INT64 NOT NULL,
    --Surrogate key for the time
    TimeValue TIME NOT NULL,
    --The time stored appropriately for doing comparisons in the Data Warehouse
    HourID INT64 NOT NULL,
    --Hour number as a number, e.g. 01
    HourDesc STRING NOT NULL,
    --Hour number as text, e.g. “01”
    MinuteID INT64 NOT NULL,
    --Minute as a number, e.g. 23
    MinuteDesc STRING NOT NULL,
    --Minute as text, e.g. “01:23”
    SecondID INT64 NOT NULL,
    --Second as a number, e.g. 45
    SecondDesc STRING NOT NULL,
    --Second as text, e.g. “01:23:45”
    MarketHoursFlag BOOLEAN,
    --Indicates a time during market hours
    OfficeHoursFlag BOOLEAN
    --Indicates a time during office hours
    );
  -- Schema of DimTrade table -> Refer Page 42 3.2.7.1
CREATE TABLE
  master.dim_trade(TradeID INT64 NOT NULL,
    --Trade identifier
    SK_BrokerID INT64,
    --Surrogate key for BrokerID
    SK_CreateDateID INT64 NOT NULL,
    --Surrogate key for date created
    SK_CreateTimeID INT64 NOT NULL,
    --Surrogate key for time created
    SK_CloseDateID INT64,
    --Surrogate key for date closed
    SK_CloseTimeID INT64,
    --Surrogate key for time closed
    Status STRING NOT NULL,
    --Trade status
    Type STRING NOT NULL,
    --Trade type
    CashFlag BOOLEAN NOT NULL,
    --Is this trade a cash (1) or margin (0) trade?
    SK_SecurityID INT64 NOT NULL,
    --Surrogate key for SecurityID
    SK_CompanyID INT64 NOT NULL,
    --Surrogate key for CompanyID
    Quantity NUMERIC NOT NULL,
    --Quantity of securities traded.
    BidPrice NUMERIC NOT NULL,
    --The requested unit price.
    SK_CustomerID INT64 NOT NULL,
    --Surrogate key for CustomerID
    SK_AccountID INT64 NOT NULL,
    --Surrogate key for AccountID
    ExecutedBy STRING NOT NULL,
    --Name of person executing the trade.
    TradePrice NUMERIC,
    --Unit price at which the security was traded.
    Fee NUMERIC,
    --Fee charged for placing this trade request
    Commission NUMERIC,
    --Commission earned on this trade
    Tax NUMERIC,
    --Amount of tax due on this trade
    BatchID INT64 NOT NULL
    --Batch ID when this record was inserted
    );
  -- Schema of DImessages table -> Refer Page 42 3.2.8.1
CREATE TABLE
  master.di_messages(MessageDateAndTime DATETIME NOT NULL,
    --Date and time of the message
    BatchID INT64 NOT NULL,
    --DI run number; see the section “Overview of BatchID usage”
    MessageSource STRING,
    --Typically the name of the transform that logs the message
    MessageText STRING NOT NULL,
    --Description of why the message was logged
    MessageType STRING NOT NULL,
    --“Status” or “Alert” or “Reject”
    MessageData STRING
    --Varies with the reason for logging the message
    );
  -- Schema of FactCashBalances table -> Refer Page 43 3.2.9
CREATE TABLE
  master.fact_cash_balances(SK_CustomerID INT64 NOT NULL,
    --Surrogate key for CustomerID
    SK_AccountID INT64 NOT NULL,
    --Surrogate key for AccountID
    SK_DateID INT64 NOT NULL,
    --Surrogate key for the date
    Cash NUMERIC NOT NULL,
    --Cash balance for the account after applying changes for this day
    BatchID INT64 NOT NULL
    --Batch ID when this record was inserted
    );
