INSERT INTO
    master.di_messages
SELECT
    CURRENT_DATETIME() AS MessageDateAndTime,
    1 AS BatchID,
    "DimCustomer" AS MessageSource,
    error.MessageData AS MessageData,
    "Alert" AS MessageType,
    error.MessageData AS MessageData
FROM (
         SELECT
             "Invalid customer tier" AS MessageText,
             CONCAT(CONCAT("C_ID=", '', CAST(CustomerID AS STRING)), ',', CONCAT("C_TIER=", '', CAST(Tier AS STRING))) AS MessageData
         FROM
             staging.customer_historical
         WHERE
                 Tier NOT IN (1,
                              2,
                              3)
         UNION ALL
         SELECT
             "DOB out of range" AS MessageText,
             CONCAT(CONCAT("C_ID=", '', CAST(CustomerID AS STRING)), ',', CONCAT("C_DOB=", '', CAST(DOB AS STRING))) AS MessageData
         FROM
             staging.customer_historical
                 CROSS JOIN
             staging.batch_date
         WHERE
                 DOB > BatchDate
            OR DATE_DIFF(BatchDate, DOB, YEAR) > 100) error;