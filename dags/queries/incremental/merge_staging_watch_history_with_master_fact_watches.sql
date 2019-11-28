MERGE INTO
    master.fact_watches old
    USING
        (
            WITH recent_watch AS (
                SELECT W_C_ID   AS CustomerID,
                       W_S_SYMB AS Symbol,
                       W_DTS    AS ActionTime,
                       W_ACTION AS Action
                FROM (
                         SELECT *,
                                ROW_NUMBER() OVER (PARTITION BY W_C_ID, W_S_SYMB ORDER BY CDC_DSN DESC ) AS row_num
                         FROM staging.watch_history) w
                WHERE row_num = 1)
            SELECT c.SK_CustomerID,
                   s.SK_SecurityID,
                   d.SK_DateID AS SK_DateID_DatePlaced,
                   d.SK_DateID AS SK_DateID_DateRemoved,
                   bn.batch_id AS BatchID,
                   Action
            FROM recent_watch w
                     JOIN
                 master.dim_customer c
                 ON
                     c.CustomerID = w.CustomerID
                     JOIN
                 master.dim_security s
                 ON
                         s.Symbol = w.Symbol
                         AND s.IsCurrent
                     JOIN
                 master.dim_date d
                 ON
                     d.DateValue = DATE(w.ActionTime)
                     CROSS JOIN
                 staging.batch_number bn
            WHERE (w.Action = 'ACTV'
                AND c.IsCurrent
                AND s.IsCurrent)
               OR w.Action = 'CNCL'
            ORDER BY w.CustomerID,
                     w.Symbol) new_record
    ON
            old.SK_SecurityID = new_record.SK_SecurityID
            AND old.SK_CustomerID = new_record.SK_CustomerID
    WHEN MATCHED AND new_record.Action = 'CNCL' THEN UPDATE SET old.SK_DateID_DateRemoved = new_record.SK_DateID_DateRemoved
    WHEN NOT MATCHED
        AND new_record.Action = 'ACTV' THEN
        INSERT
            (SK_CustomerID,
             SK_SecurityID,
             SK_DateID_DatePlaced,
             SK_DateID_DateRemoved,
             BatchID)
            VALUES (new_record.SK_CustomerID, new_record.SK_SecurityID, new_record.SK_DateID_DatePlaced, NULL,
                    new_record.BatchID);
