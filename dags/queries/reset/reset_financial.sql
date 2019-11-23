DROP TABLE IF EXISTS
    master.financial;
CREATE TABLE
    master.financial
(
    SK_CompanyID      INT64   NOT NULL,
    -- Company SK
    FI_YEAR           INT64   NOT NULL,
    -- Year of the quarter end
    FI_QTR            INT64   NOT NULL,
    -- Quarter number that the financial information is for: valid values 1, 2, 3, 4
    FI_QTR_START_DATE DATE    NOT NULL,
    -- Start date of quarter
    FI_REVENUE        NUMERIC NOT NULL,
    -- Reported revenue for the quarter
    FI_NET_EARN       NUMERIC NOT NULL,
    -- Net earnings reported for the quarter
    FI_BASIC_EPS      NUMERIC NOT NULL,
    -- Basic earnings per share for the quarter
    FI_DILUT_EPS      NUMERIC NOT NULL,
    -- Diluted earnings per share for the quarter
    FI_MARGIN         NUMERIC NOT NULL,
    -- Profit divided by revenues for the quarter
    FI_INVENTORY      NUMERIC NOT NULL,
    -- Value of inventory on hand at the end of quarter
    FI_ASSETS         NUMERIC NOT NULL,
    -- Value of total assets at the end of the quarter.
    FI_LIABILITY      NUMERIC NOT NULL,
    -- Value of total liabilities at the end of thequarter.
    FI_OUT_BASIC      INT64   NOT NULL,
    -- Average number of shares outstanding (basic).
    FI_OUT_DILUT      INT64   NOT NULL -- Average number of shares outstanding(diluted)
);