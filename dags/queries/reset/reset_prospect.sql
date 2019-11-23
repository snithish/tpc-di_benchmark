DROP TABLE IF EXISTS master.prospect;
CREATE TABLE
    master.prospect
(
    AgencyID           STRING  NOT NULL,
    -- Unique identifier from agency
    SK_RecordDateID    INT64   NOT NULL,
    -- Last date this prospect appeared in input
    SK_UpdateDateID    INT64   NOT NULL,
    -- Latest change date for this prospect
    BatchID            INT64   NOT NULL,
    -- Batch ID when this record was last modified
    IsCustomer         BOOLEAN NOT NULL,
    -- True if this person is also in DimCustomer, else False
    LastName           STRING  NOT NULL,
    -- Last name
    FirstName          STRING  NOT NULL,
    -- First name
    MiddleInitial      STRING,
    -- Middle initial
    Gender             STRING,
    -- M / F / U
    AddressLine1       STRING,
    -- Postal address
    AddressLine2       STRING,
    -- Postal address
    PostalCode         STRING,
    -- Postal code
    City               STRING  NOT NULL,
    -- City
    State              STRING  NOT NULL,
    -- State or province
    Country            STRING,
    -- Postal country
    Phone              STRING,
    -- Telephone number
    Income             INT64,
    -- Annual income
    NumberCars         INT64,
    -- Cars owned
    NumberChildren     INT64,
    -- Dependent children
    MaritalStatus      STRING,
    -- S / M / D / W / U
    Age                INT64,
    -- Current age
    CreditRating       INT64,
    -- Numeric rating
    OwnOrRentFlag      STRING,
    -- O / R / U
    Employer           STRING,
    -- Name of employer
    NumberCreditCards  INT64,
    -- Credit cards
    NetWorth           INT64,
    -- Estimated total net worth
    MarketingNameplate STRING
    -- For marketing purposes
);
