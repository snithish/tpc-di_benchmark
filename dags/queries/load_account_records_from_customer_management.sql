CREATE TEMP FUNCTION
  latest_value( history_of_values ANY TYPE) AS ( (
    SELECT
      excluding_nulls[ORDINAL(ARRAY_LENGTH(excluding_nulls))]
    FROM (
      SELECT
        ARRAY_AGG(x IGNORE NULLS) AS excluding_nulls
      FROM
        UNNEST(history_of_values) x)) );
WITH
    accounts AS (
        SELECT
            Customer.ActionTYPE AS Action,
            Customer.ActionTS AS effective_time_stamp,
            Customer.attr_C_ID AS CustomerID,
            Customer.Account.attr_CA_ID AS AccountID,
            Customer.Account.CA_B_ID AS BrokerID,
            Customer.Account.CA_NAME AS AccountDesc,
            Customer.Account.attr_CA_TAX_ST AS TaxStatus,
            CASE
                WHEN Customer.ActionTYPE IN ("INACT", "CLOSEACCT") THEN "INACTIVE"
                ELSE
                    "ACTIVE"
                END
                AS Status
        FROM
            staging.customer_management
        WHERE
                Customer.ActionTYPE IN ("NEW",
                                        "ADDACCT",
                                        "UPDACCT",
                                        "CLOSEACCT",
                                        "INACT")),
    inactive_date AS (
        SELECT
            CustomerID,
            DATE(effective_time_stamp) AS effective_date
        FROM
            accounts
        WHERE
                Action = "INACT"),
    effective_account AS (
        SELECT
            CustomerID,
            AccountID,
            latest_value(ARRAY_AGG(BrokerID) OVER w) AS BrokerID,
            latest_value(ARRAY_AGG(AccountDesc) OVER w) AS AccountDesc,
            latest_value(ARRAY_AGG(TaxStatus) OVER w) AS TaxStatus,
            Action,
            Status,
            DATE(effective_time_stamp) AS EffectiveDate,
            LEAD(DATE(effective_time_stamp), 1, DATE('9999-12-31')) OVER w AS EndDate
        FROM
            accounts
        WHERE
                Action NOT IN ("INACT")
    WINDOW
    w AS (
    PARTITION BY
      AccountID
    ORDER BY
      effective_time_stamp ASC))
SELECT
    acc.CustomerID,
    acc.AccountID,
    acc.BrokerID,
    acc.AccountDesc,
    acc.TaxStatus,
    Action,
    IF
        (inact.CustomerID IS NULL,
         Status,
         "INACTIVE") AS Status,
    acc.EffectiveDate,
    CASE
        WHEN inact.CustomerID IS NOT NULL THEN IF(acc.EndDate < inact.effective_date, acc.EndDate, inact.effective_date)
        ELSE
            acc.EndDate
        END
                     AS EndDate
FROM
    effective_account acc
        LEFT OUTER JOIN
    inactive_date inact
    ON
                acc.CustomerID = inact.CustomerID
            AND acc.EffectiveDate <= inact.effective_date;