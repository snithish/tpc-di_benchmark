WITH market AS (
    SELECT mdm.DayHigh, mdm.DayLow, mdm.SK_DateID, d.DateValue, sdm.DM_S_SYMB AS Symbol
    FROM staging.daily_market sdm
             JOIN master.dim_security ds ON ds.Symbol = sdm.DM_S_SYMB
             JOIN master.fact_market_history mdm ON mdm.SK_SecurityID = ds.SK_SecurityID
             JOIN master.dim_date d ON d.SK_DateID = mdm.SK_DateID
    WHERE d.DateValue > DATE_SUB(sdm.DM_DATE, INTERVAL 1 YEAR)
),
     min_dm AS (
         SELECT Symbol,
                FiftyTwoWeekLow,
                SK_FiftyTwoWeekLowDate
         FROM (SELECT Symbol,
                      DayLow                                                             AS FiftyTwoWeekLow,
                      SK_DateID                                                          AS SK_FiftyTwoWeekLowDate,
                      row_number() OVER (PARTITION BY Symbol ORDER BY DayLow, DateValue) AS row_num
               FROM market) a
         WHERE a.row_num = 1
     ),
     max_dm AS (
         SELECT Symbol,
                FiftyTwoWeekHigh,
                SK_FiftyTwoWeekHighDate
         FROM (SELECT Symbol,
                      DayHigh                                                                  AS FiftyTwoWeekHigh,
                      SK_DateID                                                                AS SK_FiftyTwoWeekHighDate,
                      row_number() OVER (PARTITION BY Symbol ORDER BY DayHigh DESC, DateValue) AS row_num
               FROM market) a
         WHERE a.row_num = 1
     ),
     data AS (
         SELECT MarketDate,
                sk_MarketDate,
                Symbol,
                ClosePrice,
                DayHigh,
                DayLow,
                Volume,
                IF(current_is_high, DayHigh, FiftyTwoWeekHigh)              AS FiftyTwoWeekHigh,
                IF(current_is_high, sk_MarketDate, SK_FiftyTwoWeekHighDate) AS SK_FiftyTwoWeekHighDate,
                IF(current_is_low, DayLow, FiftyTwoWeekLow)                 AS FiftyTwoWeekLow,
                IF(current_is_low, sk_MarketDate, SK_FiftyTwoWeekLowDate)   AS SK_FiftyTwoWeekLowDate
         FROM (
                  SELECT DM_DATE                                                        AS MarketDate,
                         d.SK_DateID                                                    AS sk_MarketDate,
                         DM_S_SYMB                                                      AS Symbol,
                         DM_CLOSE                                                       AS ClosePrice,
                         DM_HIGH                                                        AS DayHigh,
                         DM_LOW                                                         AS DayLow,
                         DM_VOL                                                         AS Volume,
                         (mx.FiftyTwoWeekHigh IS NULL OR DM_HIGH > mx.FiftyTwoWeekHigh) AS current_is_high,
                         (mn.FiftyTwoWeekLow IS NULL OR DM_LOW < mn.FiftyTwoWeekLow)    AS current_is_low,
                         mx.FiftyTwoWeekHigh,
                         mx.SK_FiftyTwoWeekHighDate,
                         mn.FiftyTwoWeekLow,
                         mn.SK_FiftyTwoWeekLowDate
                  FROM staging.daily_market sdm
                           LEFT JOIN min_dm mn ON sdm.DM_S_SYMB = mn.Symbol
                           LEFT JOIN max_dm mx ON sdm.DM_S_SYMB = mx.Symbol
                           JOIN master.dim_date d ON d.DateValue = sdm.DM_DATE)
     ),
     fin_four_qtr AS (
         SELECT f.SK_CompanyID,
                c.CompanyID,
                FI_QTR_START_DATE,
                SUM(FI_BASIC_EPS)
                    OVER (PARTITION BY c.CompanyID ORDER BY f.FI_QTR_START_DATE ROWS BETWEEN 3 PRECEDING AND CURRENT ROW ) AS EPS_QTR,
                LEAD(FI_QTR_START_DATE, 1, DATE('9999-12-31'))
                     OVER (PARTITION BY c.CompanyID ORDER BY f.FI_QTR_START_DATE ASC)                                      AS Next_Quarter_Start
         FROM master.financial f
                  JOIN master.dim_company c
                       ON f.SK_CompanyID = c.SK_CompanyID
     )
SELECT ds.SK_SecurityID,
       ds.SK_CompanyID,
       dm.sk_MarketDate                                                                                    AS SK_DateID,
       IF(ff.EPS_QTR != 0 AND ff.EPS_QTR IS NOT NULL, CAST((dm.ClosePrice / ff.EPS_QTR) AS NUMERIC), NULL) AS PERatio,
       IF(ds.Dividend != 0, CAST((dm.ClosePrice / ds.Dividend) AS NUMERIC), 0)                             AS Yield,
       FiftyTwoWeekHigh,
       SK_FiftyTwoWeekHighDate,
       FiftyTwoWeekLow,
       SK_FiftyTwoWeekLowDate,
       dm.ClosePrice,
       dm.DayHigh,
       dm.DayLow,
       dm.Volume,
       bn.batch_id                                                                                         AS BatchID
FROM data dm
         JOIN master.dim_security ds
              ON ds.Symbol = dm.Symbol AND dm.MarketDate >= ds.EffectiveDate AND dm.MarketDate < ds.EndDate
         LEFT JOIN fin_four_qtr ff
                   ON ff.SK_CompanyID = ds.SK_CompanyID AND dm.MarketDate >= ff.FI_QTR_START_DATE AND
                      dm.MarketDate < ff.Next_Quarter_Start
         CROSS JOIN staging.batch_number bn;