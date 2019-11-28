DROP TABLE IF EXISTS master.fact_market_history;
CREATE TABLE
    master.fact_market_history
(
    SK_SecurityID           INT64   NOT NULL,
    --Surrogate key for SecurityID
    SK_CompanyID            INT64   NOT NULL,
    -- Surrogate key for CompanyID
    SK_DateID               INT64   NOT NULL,
    -- Surrogate key for the date
    PERatio                 NUMERIC,
    -- Price to earnings per share ratio
    Yield                   NUMERIC NOT NULL,
    -- Dividend to price ratio, as a percentage
    FiftyTwoWeekHigh        NUMERIC NOT NULL,
    -- Security highest price in last 52 weeks from this day
    SK_FiftyTwoWeekHighDate INT64   NOT NULL,
    -- Earliest date on which the 52 week high price was set
    FiftyTwoWeekLow         NUMERIC NOT NULL,
    -- Security lowest price in last 52 weeks from this day
    SK_FiftyTwoWeekLowDate  INT64   NOT NULL,
    -- Earliest date on which the 52 week low price was set
    ClosePrice              NUMERIC NOT NULL,
    -- Security closing price on this day
    DayHigh                 NUMERIC NOT NULL,
    -- Highest price for the security on this day
    DayLow                  NUMERIC NOT NULL,
    -- Lowest price for the security on this day
    Volume                  INT64   NOT NULL,
    -- Trading volume of the security on this day
    BatchID                 INT64   NOT NULL -- Batch ID when this record was inserted
);