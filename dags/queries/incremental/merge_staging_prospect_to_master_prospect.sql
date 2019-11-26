CREATE
TEMPORARY
FUNCTION
  construct_MarketingNameplate(Income FLOAT64,
    NumberCars FLOAT64,
    NumberChildren FLOAT64,
    Age FLOAT64,
    CreditRating FLOAT64,
    NumberCreditCards FLOAT64,
    NetWorth FLOAT64)
  RETURNS STRING
  LANGUAGE js AS """
result = [];
if ((NetWorth !== null && NetWorth > 1000000) || (Income !== null && Income > 200000)) result.push(" HighValue ");
if ((NumberChildren !== null && NumberChildren > 3) || (NumberCreditCards !== null && NumberCreditCards > 5)) result.push(" Expenses ");
if ((Income !== null && Income < 50000) || (CreditRating !== null && CreditRating < 600) || (NetWorth !== null && NetWorth < 100000)) result.push(" MoneyAlert ");
if ((NumberCars !== null && NumberCars > 3) || (NumberCreditCards !== null && NumberCreditCards > 7)) result.push(" Spender ");
if ((Age !== null && Age < 25) && (NetWorth != null && NetWorth > 1000000)) result.push(" Inherited ");
if (result.length == 0)
    return null;
return result.join("+");
    """;
MERGE INTO
    master.prospect old
    USING
        (
            SELECT p.AgencyID,
                   d.SK_DateID                              AS SK_RecordDateID,
                   d.SK_DateID                              AS SK_UpdateDateID,
                   bn.batch_id                              AS BatchID,
                   FALSE                                    AS IsCustomer,
                   p.LastName,
                   p.FirstName,
                   p.MiddleInitial,
                   p.Gender,
                   p.AddressLine1,
                   p.AddressLine2,
                   p.PostalCode,
                   p.City,
                   p.State,
                   p.Country,
                   p.Phone,
                   p.Income,
                   p.NumberCars,
                   p.NumberChildren,
                   p.MaritalStatus,
                   p.Age,
                   p.CreditRating,
                   p.OwnOrRentFlag,
                   p.Employer,
                   p.NumberCreditCards,
                   p.NetWorth,
                   construct_MarketingNameplate(p.Income,
                                                p.NumberCars,
                                                p.NumberChildren,
                                                p.Age,
                                                p.CreditRating,
                                                p.NumberCreditCards,
                                                p.NetWorth) AS MarketingNameplate
            FROM staging.prospect p
                     CROSS JOIN
                 staging.batch_date bd
                     CROSS JOIN
                 staging.batch_number bn
                     JOIN
                 master.dim_date d
                 ON
                     bd.BatchDate = d.DateValue) new_record
    ON
        old.AgencyID = new_record.AgencyID
    WHEN MATCHED AND old.LastName != new_record.LastName OR old.FirstName != new_record.FirstName OR
                     old.MiddleInitial != new_record.MiddleInitial OR old.Gender != new_record.Gender OR
                     old.AddressLine1 != new_record.AddressLine1 OR old.AddressLine2 != new_record.AddressLine2 OR
                     old.PostalCode != new_record.PostalCode OR old.City != new_record.City OR
                     old.State != new_record.State OR
                     old.Country != new_record.Country OR old.phone != new_record.phone OR
                     old.Income != new_record.Income OR old.NumberCars != new_record.NumberCars OR
                     old.NumberChildren != new_record.NumberChildren OR old.MaritalStatus != new_record.MaritalStatus OR
                     old.Age != new_record.Age OR
                     old.CreditRating != new_record.CreditRating OR old.OwnOrRentFlag != new_record.OwnOrRentFlag OR
                     old.Employer != new_record.Employer OR old.NumberCreditCards != new_record.NumberCreditCards OR
                     old.NetWorth != new_record.NetWorth THEN UPDATE SET old.LastName = new_record.LastName, old.FirstName = new_record.FirstName,
        old.MiddleInitial = new_record.MiddleInitial, old.Gender = new_record.Gender,
        old.AddressLine1 = new_record.AddressLine1, old.AddressLine2 = new_record.AddressLine2,
        old.PostalCode = new_record.PostalCode, old.City = new_record.City,
        old.State = new_record.State,
        old.Country = new_record.Country, old.phone = new_record.phone,
        old.Income = new_record.Income, old.NumberCars = new_record.NumberCars,
        old.NumberChildren = new_record.NumberChildren, old.MaritalStatus = new_record.MaritalStatus,
        old.Age = new_record.Age,
        old.CreditRating = new_record.CreditRating, old.OwnOrRentFlag = new_record.OwnOrRentFlag,
        old.Employer = new_record.Employer, old.NumberCreditCards = new_record.NumberCreditCards,
        old.NetWorth = new_record.NetWorth, old.SK_UpdateDateID = new_record.SK_UpdateDateID
    WHEN NOT MATCHED THEN INSERT ROW