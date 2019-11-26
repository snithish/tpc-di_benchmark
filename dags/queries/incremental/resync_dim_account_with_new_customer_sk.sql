MERGE INTO
    master.dim_account old
    USING
        (
            WITH data AS (SELECT n.SK_CustomerID    as new_sk,
                                 n.Status           AS CustomerStatus,
                                 a.SK_AccountID,
                                 a.AccountID,
                                 a.SK_BrokerID,
                                 a.Status,
                                 a.AccountDesc,
                                 a.TaxStatus,
                                 true               AS IsCurrent,
                                 bn.batch_id        AS BatchID,
                                 bd.BatchDate       AS EffectiveDate,
                                 DATE('9999-12-31') AS EndDate
                          FROM master.dim_customer o
                                   JOIN master.dim_customer n ON o.CustomerID = n.CustomerID AND n.IsCurrent
                                   JOIN staging.batch_date bd
                                        ON bd.BatchDate = n.EffectiveDate AND o.EndDate = bd.BatchDate
                                   JOIN master.dim_account a ON a.SK_CustomerID = o.SK_CustomerID AND a.IsCurrent
                                   CROSS JOIN staging.batch_number bn)
            SELECT SK_AccountID AS join_key, *
            FROM data
            UNION ALL
            SELECT null AS join_key, *
            FROM data) new_record
    ON
            old.SK_AccountID = new_record.join_key
            AND old.IsCurrent = TRUE
    WHEN MATCHED AND new_record.EffectiveDate <> old.EffectiveDate THEN UPDATE SET old.IsCurrent = FALSE, old.EndDate = new_record.EffectiveDate
    WHEN NOT MATCHED
        AND new_record.join_key IS NULL THEN
        INSERT
            (SK_AccountID,
             AccountID,
             SK_BrokerID,
             SK_CustomerID,
             Status,
             AccountDesc,
             TaxStatus,
             IsCurrent,
             BatchID,
             EffectiveDate,
             EndDate)
            VALUES (CAST(CONCAT(FORMAT_DATE('%E4Y%m%d', new_record.EffectiveDate), '',
                                CAST(new_record.AccountID AS STRING)) AS INT64), new_record.AccountID,
                    new_record.SK_BrokerID,
                    new_record.new_sk,
                    IF(new_record.CustomerStatus = 'Inactive', new_record.CustomerStatus, new_record.Status),
                    new_record.AccountDesc,
                    new_record.TaxStatus,
                    new_record.IsCurrent,
                    new_record.BatchID,
                    new_record.EffectiveDate,
                    new_record.EndDate);
