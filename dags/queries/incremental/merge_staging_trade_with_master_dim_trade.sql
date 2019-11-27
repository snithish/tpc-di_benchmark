MERGE INTO
    master.dim_trade old
    USING
        (
            WITH recent_trade AS (
                SELECT T_ID          AS TradeID,
                       T_DTS         AS TradeTime,
                       T_ST_ID       AS StatusID,
                       T_TT_ID       AS TypeID,
                       T_CA_ID       AS AccountID,
                       T_IS_CASH     AS CashFlag,
                       T_S_SYMB      AS Symbol,
                       T_QTY         AS Quantity,
                       T_BID_PRICE   AS BidPrice,
                       T_EXEC_NAME   AS ExecutedBy,
                       T_TRADE_PRICE AS TradePrice,
                       T_CHRG        AS Fee,
                       T_COMM        AS Commission,
                       T_TAX         AS Tax
                FROM (
                         SELECT *,
                                ROW_NUMBER() OVER (PARTITION BY T_ID ORDER BY CDC_DSN DESC ) AS row_num
                         FROM staging.trade) t
                WHERE row_num = 1),
                 enriched AS (
                     SELECT t.TradeID,
                            a.SK_BrokerID,
                            da.SK_DateID AS SK_CreateDateID,
                            dt.SK_TimeID AS SK_CreateTimeID,
                            IF
                                (t.StatusID IN ('CMPT',
                                                'CNCL'),
                                 da.SK_DateID,
                                 NULL)   AS SK_CloseDateID,
                            IF
                                (t.StatusID IN ('CMPT',
                                                'CNCL'),
                                 dt.SK_TimeID,
                                 NULL)   AS SK_CloseTimeID,
                            st.ST_NAME   AS Status,
                            tt.TT_NAME   AS Type,
                            t.CashFlag,
                            ds.SK_SecurityID,
                            ds.SK_CompanyID,
                            t.Quantity,
                            t.BidPrice,
                            a.SK_CustomerID,
                            a.SK_AccountID,
                            t.ExecutedBy,
                            t.TradePrice,
                            t.Fee,
                            t.Commission,
                            t.Tax,
                            bn.batch_id  AS BatchID
                     FROM recent_trade t
                              JOIN
                          master.dim_account a
                          ON
                                      t.AccountID = a.AccountID
                                  AND a.IsCurrent
                              JOIN
                          master.dim_security ds
                          ON
                                      t.Symbol = ds.Symbol
                                  AND ds.IsCurrent
                              JOIN
                          master.dim_date da
                          ON
                                  DATE(t.TradeTime) = da.DateValue
                              JOIN
                          master.dim_time dt
                          ON
                                  TIME_TRUNC(TIME(t.TradeTime),
                                             SECOND) = PARSE_TIME('%H:%M:%S',
                                                                  dt.TimeValue)
                              JOIN
                          master.status_type st
                          ON
                                  t.StatusID = st.ST_ID
                              JOIN
                          master.trade_type tt
                          ON
                                  t.TypeID = tt.TT_ID
                              CROSS JOIN
                          staging.batch_number bn)
            SELECT *
            FROM enriched) new_record
    ON
            old.TradeID = new_record.TradeID
    WHEN MATCHED
        AND new_record.Status = 'Completed'
            OR new_record.Status = 'Canceled' THEN
        UPDATE
            SET
                old.SK_CloseDateID = new_record.SK_CloseDateID,
                old.SK_CloseTimeID = new_record.SK_CloseTimeID,
                old.Status = new_record.Status,
                old.Type = new_record.Type,
                old.CashFlag = new_record.CashFlag,
                old.Quantity = new_record.Quantity,
                old.BidPrice = new_record.BidPrice,
                old.ExecutedBy = new_record.ExecutedBy,
                old.TradePrice = new_record.TradePrice,
                old.Fee = new_record.Fee,
                old.Commission = new_record.Commission,
                old.Tax = new_record.Tax
    WHEN MATCHED THEN
        UPDATE
            SET
                old.Status = new_record.Status,
                old.Type = new_record.Type,
                old.CashFlag = new_record.CashFlag,
                old.Quantity = new_record.Quantity,
                old.BidPrice = new_record.BidPrice,
                old.ExecutedBy = new_record.ExecutedBy,
                old.TradePrice = new_record.TradePrice,
                old.Fee = new_record.Fee,
                old.Commission = new_record.Commission,
                old.Tax = new_record.Tax
    WHEN NOT MATCHED THEN
        INSERT (TradeID,
                SK_BrokerID,
                SK_CreateDateID,
                SK_CreateTimeID,
                SK_CloseDateID,
                SK_CloseTimeID,
                Status,
                Type,
                CashFlag,
                SK_SecurityID,
                SK_CompanyID,
                Quantity,
                BidPrice,
                SK_CustomerID,
                SK_AccountID,
                ExecutedBy,
                TradePrice,
                Fee,
                Commission,
                Tax,
                BatchID) VALUES (new_record.TradeID,
                                 new_record.SK_BrokerID,
                                 new_record.SK_CreateDateID,
                                 new_record.SK_CreateTimeID,
                                 new_record.SK_CloseDateID,
                                 new_record.SK_CloseTimeID,
                                 new_record.Status,
                                 new_record.Type,
                                 new_record.CashFlag,
                                 new_record.SK_SecurityID,
                                 new_record.SK_CompanyID,
                                 new_record.Quantity,
                                 new_record.BidPrice,
                                 new_record.SK_CustomerID,
                                 new_record.SK_AccountID,
                                 new_record.ExecutedBy,
                                 new_record.TradePrice,
                                 new_record.Fee,
                                 new_record.Commission,
                                 new_record.Tax,
                                 new_record.BatchID);
