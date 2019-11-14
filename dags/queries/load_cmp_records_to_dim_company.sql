WITH
  cmp_record_effective_date AS (
  SELECT
    *,
    LEAD(PTS, 1, DATETIME('9999-12-31')) OVER (PARTITION BY CIK ORDER BY PTS ASC) AS end_date_time
  FROM
    staging.cmp_records)
SELECT
  CAST(CONCAT(FORMAT_DATETIME('%E4Y%m%d',
        cmp.PTS), '', CAST(cmp.CIK AS STRING)) AS INT64) AS SK_CompanyID,
  cmp.CIK AS CompanyID,
  st.ST_NAME AS Status,
  cmp.CompanyName AS Name,
  ind.IN_NAME AS Industry,
  CASE
    WHEN cmp.SPRating NOT IN ("AAA", "AA+", "AA-", "A+", "A-", "BBB+", "BBB-", "BB+", "BB-", "B+", "B-", "CCC+", "CCC-", "CC", "C", "D") THEN NULL
  ELSE
  cmp.SPRating
END
  AS SPRating,
  CASE
    WHEN cmp.SPRating NOT IN ("AAA", "AA+", "AA-", "A+", "A-", "BBB+", "BBB-", "BB+", "BB-", "B+", "B-", "CCC+", "CCC-", "CC", "C", "D") THEN NULL
    WHEN STARTS_WITH(cmp.SPRating, "A")
  OR STARTS_WITH(cmp.SPRating, "BBB") THEN FALSE
  ELSE
  TRUE
END
  AS isLowGrade,
  cmp.CEOName AS CEO,
  cmp.AddrLine1 AS AddressLine1,
  cmp.AddrLine2 AS AddressLine2,
  cmp.PostalCode AS PostalCode,
  cmp.City AS City,
  cmp.StateProvince AS StateProv,
  cmp.Country AS Country,
  cmp.Description AS Description,
  cmp.FoundingDate AS FoundingDate,
  CASE
    WHEN DATE(cmp.end_date_time) = DATE('9999-12-31') THEN TRUE
  ELSE
  FALSE
END
  AS IsCurrent,
  1 AS BatchID,
  DATE(cmp.PTS) AS EffectiveDate,
  DATE(cmp.end_date_time) AS EndDate
FROM (cmp_record_effective_date cmp
  JOIN
    master.status_type st
  ON
    cmp.Status = st.ST_ID)
JOIN
  master.industry ind
ON
  cmp.IndustryID = ind.IN_ID;
