SELECT
    PARSE_DATETIME('%E4Y%m%d-%H%M%S',
                   SUBSTR(ROW, 1, 15)) AS PTS,
    TRIM(SUBSTR(ROW, 19, 15)) AS Symbol,
    TRIM(SUBSTR(ROW, 34, 6)) AS IssueType,
    TRIM(SUBSTR(ROW, 40, 4)) AS Status,
    TRIM(SUBSTR(ROW, 44, 70)) AS Name,
    TRIM(SUBSTR(ROW, 114, 6)) AS ExID,
    CAST(TRIM(SUBSTR(ROW, 120, 13)) AS INT64) AS ShOut,
    PARSE_DATE('%E4Y%m%d',
               TRIM(SUBSTR(ROW, 133, 8))) AS FirstTradeDate,
    PARSE_DATE('%E4Y%m%d',
               TRIM(SUBSTR(ROW, 141, 8))) AS FirstTradeExchg,
    CAST(TRIM(SUBSTR(ROW, 149, 12)) AS NUMERIC) AS Dividend,
    CASE
        WHEN CHAR_LENGTH(TRIM(SUBSTR(ROW, 161, 60))) <= 10 THEN SAFE_CAST(TRIM(SUBSTR(ROW, 161, 60)) AS INT64)
        ELSE
            NULL
        END
        AS CIK,
    CASE
        WHEN (CHAR_LENGTH(TRIM(SUBSTR(ROW, 161, 60))) > 10 OR SAFE_CAST(TRIM(SUBSTR(ROW, 161, 60)) AS INT64) IS NULL) THEN TRIM(SUBSTR(ROW, 161, 60))
        ELSE
            NULL
        END
        AS CompanyName
FROM
    staging.finwire
WHERE
        TRIM(SUBSTR(ROW, 16, 3)) = 'SEC';