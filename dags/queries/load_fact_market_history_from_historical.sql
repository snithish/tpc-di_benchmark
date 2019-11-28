WITH fin_four_qtr AS (
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
       d.SK_DateID,
       IF(ff.EPS_QTR != 0 AND ff.EPS_QTR IS NOT NULL, CAST((dm.DM_CLOSE / ff.EPS_QTR) AS NUMERIC), NULL) AS PERatio,
       IF(ds.Dividend != 0, CAST((dm.DM_CLOSE / ds.Dividend) AS NUMERIC), 0)                             AS Yield,
       FiftyTwoWeekHigh,
       SK_FiftyTwoWeekHighDate,
       FiftyTwoWeekLow,
       SK_FiftyTwoWeekLowDate,
       dm.DM_CLOSE                                                                                       AS ClosePrice,
       dm.DM_HIGH                                                                                        AS DayHigh,
       dm.DM_LOW                                                                                         AS DayLow,
       dm.DM_VOL                                                                                         AS Volume,
       1                                                                                                 AS BatchID
FROM staging.daily_market_historical_transformed dm
         JOIN master.dim_security ds
              ON ds.Symbol = dm.DM_S_SYMB AND dm.DM_DATE >= ds.EffectiveDate AND dm.DM_DATE < ds.EndDate
         LEFT JOIN fin_four_qtr ff
                   ON ff.SK_CompanyID = ds.SK_CompanyID AND dm.DM_DATE >= ff.FI_QTR_START_DATE AND
                      dm.DM_DATE < ff.Next_Quarter_Start
         JOIN master.dim_date d ON d.DateValue = dm.DM_DATE;