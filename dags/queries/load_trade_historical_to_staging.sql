WITH expanded_trade AS (
    SELECT t.*,
           IF
               ((th.TH_ST_ID = 'SBMT'
               AND t.T_TT_ID IN ('TMB',
                                 'TMS'))
                    OR th.TH_ST_ID = 'PNDG',
                th.TH_DTS,
                NULL) AS creation_time,
           IF
               (th.TH_ST_ID IN ('CMPT',
                                'CNCL'),
                th.TH_DTS,
                NULL) AS closing_time,
           th.TH_DTS  AS action_time
    FROM staging.trade_historical t
             JOIN
         staging.trade_history_historical th
         ON
             t.T_ID = th.TH_T_ID),
     recent_trade AS (
         SELECT T_ID,
                T_ST_ID,
                T_TT_ID,
                T_IS_CASH,
                T_S_SYMB,
                T_QTY,
                T_BID_PRICE,
                T_CA_ID,
                T_EXEC_NAME,
                T_TRADE_PRICE,
                T_CHRG,
                T_COMM,
                T_TAX,
                creation_time,
                closing_time
         FROM (
                  SELECT T_ID,
                         FIRST_VALUE(T_ST_ID) OVER w                                           AS T_ST_ID,
                         FIRST_VALUE(T_TT_ID) OVER w                                           AS T_TT_ID,
                         FIRST_VALUE(T_IS_CASH) OVER w                                         AS T_IS_CASH,
                         FIRST_VALUE(T_S_SYMB) OVER w                                          AS T_S_SYMB,
                         FIRST_VALUE(T_QTY) OVER w                                             AS T_QTY,
                         FIRST_VALUE(T_BID_PRICE) OVER w                                       AS T_BID_PRICE,
                         FIRST_VALUE(T_CA_ID) OVER w                                           AS T_CA_ID,
                         FIRST_VALUE(T_EXEC_NAME) OVER w                                       AS T_EXEC_NAME,
                         FIRST_VALUE(T_TRADE_PRICE) OVER w                                     AS T_TRADE_PRICE,
                         FIRST_VALUE(T_CHRG) OVER w                                            AS T_CHRG,
                         FIRST_VALUE(T_COMM) OVER w                                            AS T_COMM,
                         FIRST_VALUE(T_TAX) OVER w                                             AS T_TAX,
                         LAST_VALUE(creation_time)
                                    OVER (w RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS creation_time,
                         FIRST_VALUE(closing_time) OVER w                                      AS closing_time,
                         ROW_NUMBER() OVER w                                                   AS time_order
                  FROM expanded_trade
                      WINDOW
                          w AS (
                              PARTITION BY
                                  T_ID
                              ORDER BY
                                  action_time DESC )) a
         WHERE a.time_order = 1)
SELECT T_ID              AS TradeID,
       acc.SK_BrokerID   AS SK_BrokerID,
       std.SK_DateID     AS SK_CreateDateID,
       stt.SK_TimeID     AS SK_CreateTimeID,
       en.SK_DateID      AS SK_CloseDateID,
       et.SK_TimeID      AS SK_CloseTimeID,
       sty.ST_NAME       AS Status,
       tt.TT_NAME        AS Type,
       T_IS_CASH         AS CashFlag,
       sec.SK_SecurityID AS SK_SecurityID,
       sec.SK_CompanyID  AS SK_CompanyID,
       T_QTY             AS Quantity,
       T_BID_PRICE       AS BidPrice,
       acc.SK_CustomerID AS SK_CustomerID,
       acc.SK_AccountID  AS SK_AccountID,
       T_EXEC_NAME       AS ExecutedBy,
       T_TRADE_PRICE     AS TradePrice,
       T_CHRG            AS Fee,
       T_COMM            AS Commission,
       T_TAX             AS Tax,
       1                 AS BatchID
FROM recent_trade t
         JOIN
     master.dim_date std
     ON
         std.DateValue = DATE(t.creation_time)
         JOIN
     master.dim_time stt
     ON
             PARSE_TIME("%H:%M:%S",
                        stt.TimeValue) = TIME_TRUNC(TIME(t.creation_time), SECOND)
         LEFT JOIN
     master.dim_date en
     ON
         en.DateValue = DATE(t.closing_time)
         LEFT JOIN
     master.dim_time et
     ON
             PARSE_TIME("%H:%M:%S",
                        et.TimeValue) = TIME_TRUNC(TIME(t.closing_time), SECOND)
         JOIN master.status_type sty ON t.T_ST_ID = sty.ST_ID
         JOIN master.trade_type tt ON t.T_TT_ID = tt.TT_ID
         JOIN master.dim_security sec
              ON t.T_S_SYMB = sec.Symbol AND DATE(t.creation_time) >= sec.EffectiveDate AND
                 t.creation_time < sec.EndDate
         JOIN master.dim_account acc
              ON t.T_CA_ID = acc.AccountID AND DATE(t.creation_time) >= acc.EffectiveDate AND
                 t.creation_time < acc.EndDate;