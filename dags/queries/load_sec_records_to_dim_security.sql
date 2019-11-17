WITH
    sec_record_effective_date AS (
        SELECT
            *,
            LEAD(PTS, 1, DATETIME('9999-12-31')) OVER (PARTITION BY Symbol ORDER BY PTS ASC) AS end_date_time
        FROM
            staging.sec_records)
SELECT
    FARM_FINGERPRINT(CONCAT(FORMAT_DATETIME('%E4Y%m%d',
                                            sec.PTS), '', sec.Symbol)) AS SK_SecurityID,
    sec.Symbol AS Symbol,
    sec.IssueType AS Issue,
    st.ST_NAME AS Status,
    sec.Name AS Name,
    sec.ExID AS ExchangeID,
    cmp.SK_CompanyID AS SK_CompanyID,
    sec.ShOut AS SharesOutstanding,
    sec.FirstTradeDate AS FirstTrade,
    sec.FirstTradeExchg AS FirstTradeOnExchange,
    sec.Dividend AS Dividend,
    IF
        (DATE(sec.end_date_time) = DATE('9999-12-31'),
         TRUE,
         FALSE) AS IsCurrent,
    1 AS BatchID,
    DATE(sec.PTS) AS EffectiveDate,
    DATE(sec.end_date_time) AS EndDate
FROM
    sec_record_effective_date sec
        JOIN
    master.dim_company cmp
    ON
            (sec.CIK IS NOT NULL
                AND sec.CIK = cmp.CompanyID)
            OR (sec.CIK IS NULL
            AND sec.CompanyName = cmp.Name)
                AND DATE(sec.PTS) BETWEEN cmp.EffectiveDate
                   AND cmp.EndDate
        JOIN
    master.status_type st
    ON
            sec.Status = st.ST_ID;