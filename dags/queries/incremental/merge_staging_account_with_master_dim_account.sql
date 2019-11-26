MERGE INTO
    master.dim_account old
    USING
        (
            WITH recent_account AS (
                SELECT a.CA_ID     AS AccountID,
                       a.CA_B_ID   AS BrokerID,
                       a.CA_C_ID   AS CustomerID,
                       a.CA_ST_ID  AS StatusID,
                       a.CA_NAME   AS AccountDesc,
                       a.CA_TAX_ST AS TaxStatus
                FROM (SELECT *, row_number() over (PARTITION BY CA_ID ORDER BY CDC_DSN DESC ) as row_num
                      FROM staging.account) a
                WHERE a.row_num = 1
            ),
                 enriched AS (
                     SELECT CAST(CONCAT(FORMAT_DATE('%E4Y%m%d', bd.BatchDate), '',
                                        CAST(a.AccountID AS STRING)) AS INT64) AS SK_AccountID,
                            a.AccountID,
                            b.SK_BrokerID,
                            c.SK_CustomerID,
                            IF(c.Status = 'Inactive', c.Status, st.ST_NAME)    AS Status,
                            AccountDesc,
                            TaxStatus,
                            TRUE                                               AS IsCurrent,
                            bn.batch_id                                        AS BatchID,
                            bd.BatchDate                                       AS EffectiveDate,
                            DATE('9999-12-31')                                 AS EndDate
                     FROM recent_account a
                              JOIN master.dim_customer c
                                   ON c.CustomerID = a.CustomerID AND c.IsCurrent
                              JOIN master.dim_broker b ON b.BrokerID = a.BrokerID
                              JOIN master.status_type st ON st.ST_ID = a.StatusID
                              CROSS JOIN staging.batch_number bn
                              CROSS JOIN staging.batch_date bd
                 ),
                 enriched_excluded AS (
                     SELECT e.*
                     FROM enriched e
                              LEFT JOIN master.dim_account a ON e.SK_AccountID = a.SK_AccountID
                     WHERE a.SK_AccountID IS NULL
                 )
            SELECT AccountID AS join_key, *
            FROM enriched_excluded
            UNION ALL
            SELECT null AS join_key, *
            FROM enriched_excluded) new_record
    ON
            old.AccountID = new_record.join_key
            AND old.IsCurrent = TRUE
    WHEN MATCHED AND old.EffectiveDate <> new_record.EffectiveDate THEN UPDATE SET old.IsCurrent = FALSE, old.EndDate = new_record.EffectiveDate
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
            VALUES (new_record.SK_AccountID,
                    new_record.AccountID,
                    new_record.SK_BrokerID,
                    new_record.SK_CustomerID,
                    new_record.Status,
                    new_record.AccountDesc,
                    new_record.TaxStatus,
                    new_record.IsCurrent,
                    new_record.BatchID,
                    new_record.EffectiveDate,
                    new_record.EndDate);