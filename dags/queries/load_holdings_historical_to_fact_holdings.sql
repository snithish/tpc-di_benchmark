SELECT h.HH_H_T_ID      AS TradeID,
       h.HH_T_ID        AS CurrentTradeID,
       t.SK_CustomerID,
       t.SK_AccountID,
       t.SK_SecurityID,
       t.SK_CompanyID,
       t.SK_CloseDateID AS SK_DateID,
       t.SK_CloseTimeID AS SK_TimeID,
       t.BidPrice       AS CurrentPrice,
       h.HH_AFTER_QTY   AS CurrentHolding,
       1                AS BatchID
FROM staging.holding_history_historical h
         JOIN master.dim_trade t ON h.HH_T_ID = t.TradeID