WITH fin_four_qtr AS (
    SELECT f.SK_CompanyID,
           c.CompanyID,
           SUM(FI_BASIC_EPS)
               OVER (PARTITION BY c.CompanyID ORDER BY f.FI_QTR_START_DATE ROWS BETWEEN 3 PRECEDING AND CURRENT ROW ) AS EPS_QTR,
           LEAD(FI_QTR_START_DATE, 1)
                OVER (PARTITION BY c.CompanyID ORDER BY f.FI_QTR_START_DATE ASC)                                      AS Next_Quarter_Start
    FROM master.financial f
             JOIN master.dim_company c
                  ON f.SK_CompanyID = c.SK_CompanyID
)
SELECT *
FROM staging.daily_market_historical_transformed dm
         JOIN master.dim_security ds
              ON ds.Symbol = dm.DM_S_SYMB AND dm.DM_DATE >= ds.EffectiveDate AND dm.DM_DATE < ds.EndDate
