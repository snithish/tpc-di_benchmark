MERGE INTO master.fact_cash_balances old USING
    (WITH total_cash AS (
        SELECT *
        FROM (SELECT *,
                     SUM(CT_AMT) OVER (PARTITION BY CT_CA_ID)                         AS total_cash,
                     ROW_NUMBER() OVER (PARTITION BY CT_CA_ID ORDER BY CDC_DSN DESC ) AS row_num
              FROM staging.cash_transaction) t
        WHERE row_num = 1
    )
     SELECT SK_CustomerID,
            SK_AccountID,
            SK_DateID,
            CAST(total_cash AS NUMERIC) AS Cash,
            bn.batch_id                 AS BatchID
     FROM total_cash t
              JOIN master.dim_account a ON a.AccountID = t.CT_CA_ID AND a.IsCurrent
              JOIN master.dim_date d ON DATE(t.CT_DTS) = d.DateValue
              CROSS JOIN staging.batch_number bn) new_record ON old.SK_AccountID = new_record.SK_AccountID AND
                                                                old.SK_CustomerID = new_record.SK_CustomerID AND
                                                                old.SK_DateID = new_record.SK_DateID
    WHEN NOT MATCHED THEN INSERT ROW