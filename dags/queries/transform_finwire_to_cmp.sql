-- Load from staging.finwire to staging.cmp_records
-- Refer to Page 30 -> 2.2.8
-- Multi record is parsed to constituent tables here we have done it for CMP, for format look at table + plus data
-- casting done for easy loading to Dimension table later
INSERT INTO
    staging.cmp_records(PTS,
                        COMPANYNAME,
                        CIK,
                        STATUS,
                        INDUSTRYID,
                        SPRATING,
                        FOUNDINGDATE,
                        ADDRLINE1,
                        ADDRLINE2,
                        POSTALCODE,
                        CITY,
                        STATEPROVINCE,
                        COUNTRY,
                        CEONAME,
                        DESCRIPTION)
SELECT
    PARSE_DATETIME('%E4Y%m%d-%H%M%S',
                   SUBSTR(ROW, 1, 15)) AS PTS,
    TRIM(SUBSTR(ROW, 19, 60)) AS CompanyName,
    CAST(TRIM(SUBSTR(ROW, 79, 10)) AS INT64) AS CIK,
    TRIM(SUBSTR(ROW, 89, 4)) AS Status,
    TRIM(SUBSTR(ROW, 93, 2)) AS IndustryID,
    TRIM(SUBSTR(ROW, 95, 4)) AS SPrating,
    CASE
        WHEN TRIM(SUBSTR(ROW, 99, 8)) = '' THEN NULL
        ELSE
            PARSE_DATE('%E4Y%m%d',
                       TRIM(SUBSTR(ROW, 99, 8)))
        END
        AS FoundingDate,
    TRIM(SUBSTR(ROW, 107, 80)) AS AddrLine1,
    CASE
        WHEN TRIM(SUBSTR(ROW, 187, 80)) = '' THEN NULL
        ELSE
            TRIM(SUBSTR(ROW, 187, 80))
        END
        AS AddrLine2,
    TRIM(SUBSTR(ROW, 267, 12)) AS PostalCode,
    TRIM(SUBSTR(ROW, 279, 25)) AS City,
    TRIM(SUBSTR(ROW, 304, 20)) AS StateProvince,
    CASE
        WHEN TRIM(SUBSTR(ROW, 324, 24)) = '' THEN NULL
        ELSE
            TRIM(SUBSTR(ROW, 324, 24))
        END
        AS Country,
    TRIM(SUBSTR(ROW, 348, 46)) AS CEOname,
    TRIM(SUBSTR(ROW, 394, 150)) AS Description
FROM
    staging.finwire
WHERE
        TRIM(SUBSTR(ROW, 16, 3)) = 'CMP';