MERGE INTO
    master.prospect p
    USING
        (
            SELECT FirstName,
                   LastName,
                   AddressLine1,
                   AddressLine2,
                   PostalCode
            FROM master.dim_customer
            WHERE IsCurrent = TRUE
              AND Status = 'ACTIVE') c
    ON
            UPPER(p.FirstName) = UPPER(c.FirstName)
            AND UPPER(p.LastName) = UPPER(c.LastName)
            AND UPPER(p.AddressLine1) = UPPER(c.AddressLine1)
            AND UPPER(p.AddressLine2) = UPPER(c.AddressLine2)
            AND UPPER(p.PostalCode) = UPPER(c.PostalCode)
    WHEN MATCHED
        AND p.IsCustomer = FALSE THEN
        UPDATE
            SET
                p.IsCustomer = TRUE;