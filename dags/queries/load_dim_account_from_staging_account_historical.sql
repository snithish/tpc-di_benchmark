SELECT
    CAST(CONCAT(FORMAT_DATE('%E4Y%m%d', acc.EffectiveDate), '', CAST(acc.AccountID AS STRING)) AS INT64) AS SK_AccountID,
    acc.AccountID AS AccountID,
    bro.SK_BrokerID AS SK_BrokerID,
    cus.SK_CustomerID AS SK_CustomerID,
    acc.Status AS Status,
    acc.AccountDesc AS AccountDesc,
    acc.TaxStatus AS TaxStatus,
    CASE
        WHEN acc.EndDate = DATE('9999-12-31') THEN TRUE
        ELSE
            FALSE
        END
                                                                                                         AS IsCurrent,
    1 AS BatchID,
    acc.EffectiveDate AS EffectiveDate,
    acc.EndDate AS EndDate
FROM (staging.account_historical acc
    JOIN
    master.dim_customer cus
    ON
                acc.CustomerID = cus.CustomerID
            AND acc.EffectiveDate >= cus.EffectiveDate
            AND acc.EffectiveDate < cus.EndDate)
         JOIN
     master.dim_broker bro
     ON
                 bro.BrokerID = acc.BrokerID
             AND acc.EffectiveDate >= bro.EffectiveDate
             AND acc.EffectiveDate < bro.EndDate;