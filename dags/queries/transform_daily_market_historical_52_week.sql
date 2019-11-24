WITH daily_market_with_date AS (
    SELECT h.*,
           d.SK_DateID
    FROM staging.daily_market_historical h
             JOIN
         master.dim_date d
         ON
             h.DM_DATE = d.DateValue),
     year_range AS (
         SELECT l.DM_S_SYMB,
                l.DM_DATE,
                l.SK_DateID,
                l.DM_HIGH,
                l.DM_LOW,
                l.DM_VOL,
                r.DM_HIGH   AS o_high,
                r.DM_LOW    AS o_low,
                r.DM_DATE   AS o_date,
                r.SK_DateID AS o_SK_DateID
         FROM daily_market_with_date l
                  JOIN
              daily_market_with_date r
              ON
                      l.DM_S_SYMB = r.DM_S_SYMB
                      AND r.DM_DATE <= l.DM_DATE
                      AND r.DM_DATE > DATE_SUB(l.DM_DATE, INTERVAL 1 YEAR)),
     min_dm AS (
         SELECT DM_S_SYMB,
                DM_DATE,
                SK_DateID,
                DM_HIGH,
                DM_LOW,
                DM_VOL,
                o_low       AS FiftyTwoWeekLow,
                o_SK_DateID AS SK_FiftyTwoWeekLowDate
         FROM (
                  SELECT *,
                         ROW_NUMBER() OVER (PARTITION BY DM_S_SYMB, DM_DATE ORDER BY o_low ASC, o_date ASC) AS row_num
                  FROM year_range) a
         WHERE row_num = 1),
     max_dm AS (
         SELECT DM_S_SYMB,
                DM_DATE,
                o_high AS FiftyTwoWeekHigh,
                o_SK_DateID AS SK_FiftyTwoWeekHighDate
         FROM (
                  SELECT *,
                         ROW_NUMBER() OVER (PARTITION BY DM_S_SYMB, DM_DATE ORDER BY o_high DESC, o_date ASC) AS row_num
                  FROM year_range) a
         WHERE row_num = 1)
SELECT mn.DM_S_SYMB,
       mn.DM_DATE,
       mn.DM_HIGH,
       mn.DM_LOW,
       mn.DM_VOL,
       FiftyTwoWeekLow,
       SK_FiftyTwoWeekLowDate,
       FiftyTwoWeekHigh,
       SK_FiftyTwoWeekHighDate
FROM max_dm mx
         JOIN
     min_dm mn
     ON
             mx.DM_S_SYMB = mn.DM_S_SYMB
             AND mx.DM_DATE = mn.DM_DATE;