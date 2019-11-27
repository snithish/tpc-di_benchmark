MERGE INTO master.fact_holdings old USING
    (SELECT h.HH_T_ID        AS TradeID,
            h.HH_H_T_ID      AS CurrentTradeID,
            t.SK_CustomerID,
            t.SK_AccountID,
            t.SK_SecurityID,
            t.SK_CompanyID,
            t.SK_CloseDateID AS SK_DateID,
            t.SK_CloseTimeID AS SK_TimeID,
            t.BidPrice       AS CurrentPrice,
            HH_AFTER_QTY     AS CurrentHolding,
            bn.batch_id      AS BatchID
     FROM staging.holding_history h
              JOIN master.dim_trade t ON t.TradeID = h.HH_T_ID
              CROSS JOIN staging.batch_number bn) new_record ON old.TradeID = new_record.TradeID AND
                                                                old.CurrentTradeID = new_record.CurrentTradeID AND
                                                                old.SK_CustomerID = new_record.SK_CustomerID AND
                                                                old.SK_AccountID = new_record.SK_AccountID AND
                                                                old.SK_SecurityID = new_record.SK_SecurityID AND
                                                                old.SK_CompanyID = new_record.SK_CompanyID AND
                                                                old.SK_DateID = new_record.SK_DateID AND
                                                                old.SK_TimeID = new_record.SK_TimeID
    WHEN NOT MATCHED THEN INSERT ROW