MERGE INTO
    master.dim_customer old
    USING
        (
            WITH deduplicated AS (
                SELECT s.*
                FROM staging.dim_customer s
                         LEFT JOIN master.dim_customer c ON s.SK_CustomerID = c.SK_CustomerID
                WHERE c.SK_CustomerID IS NULL
            )
            SELECT CustomerID AS join_key,
                   *
            FROM deduplicated
            UNION ALL
            SELECT NULL AS join_key,
                   *
            FROM deduplicated) new_record
    ON
            old.CustomerID = new_record.join_key
            AND old.IsCurrent = TRUE
    WHEN MATCHED AND new_record.EffectiveDate <> old.EffectiveDate THEN UPDATE SET old.IsCurrent = FALSE, old.EndDate = new_record.EffectiveDate
    WHEN NOT MATCHED
        AND new_record.join_key IS NULL THEN
        INSERT
            (SK_CustomerID,
             CustomerID,
             TaxID,
             Status,
             LastName,
             FirstName,
             MiddleInitial,
             Gender,
             Tier,
             DOB,
             AddressLine1,
             AddressLine2,
             PostalCode,
             City,
             StateProv,
             Country,
             Phone1,
             Phone2,
             Phone3,
             Email1,
             Email2,
             NationalTaxRateDesc,
             NationalTaxRate,
             LocalTaxRateDesc,
             LocalTaxRate,
             AgencyID,
             CreditRating,
             NetWorth,
             MarketingNameplate,
             IsCurrent,
             BatchID,
             EffectiveDate,
             EndDate)
            VALUES (new_record.SK_CustomerID, new_record.CustomerID, new_record.TaxID, new_record.Status,
                    new_record.LastName, new_record.FirstName, new_record.MiddleInitial, new_record.Gender,
                    new_record.Tier, new_record.DOB, new_record.AddressLine1, new_record.AddressLine2,
                    new_record.PostalCode, new_record.City, new_record.StateProv, new_record.Country, new_record.Phone1,
                    new_record.Phone2, new_record.Phone3, new_record.Email1, new_record.Email2,
                    new_record.NationalTaxRateDesc, new_record.NationalTaxRate, new_record.LocalTaxRateDesc,
                    new_record.LocalTaxRate, new_record.AgencyID, new_record.CreditRating, new_record.NetWorth,
                    new_record.MarketingNameplate, new_record.IsCurrent, new_record.BatchID, new_record.EffectiveDate,
                    new_record.EndDate);