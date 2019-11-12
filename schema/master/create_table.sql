---- Schema of FactMarketHistory table -> Refer Page 43 3.2.11
CREATE TABLE
  master.FactMarketHistory ( SK_SecurityID INT64 NOT NULL,
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

