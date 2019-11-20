-- noinspection SqlNoDataSourceInspectionForFile
-- Load from staging.hr to master.dim_broker
-- Refer to Page 60 -> 4.5.2
-- For surrogate key strategy to concat OLTP Key -> EmployeeID with effective date
WITH
    min_date AS (
        SELECT
            MIN(DateValue) AS EffectiveDate
        FROM
            master.dim_date)
SELECT
    CAST(CONCAT(FORMAT_DATE('%E4Y%m%d', min_date.EffectiveDate), '', CAST(EmployeeID AS STRING)) AS INT64) AS SK_BrokerID,
    EmployeeID AS BrokerID,
    ManagerID,
    EmployeeFirstName,
    EmployeeLastName,
    EmployeeMI,
    EmployeeBranch,
    EmployeeOffice,
    EmployeePhone,
    TRUE AS IsCurrent,
    1 AS BatchID,
    min_date.EffectiveDate,
    DATE('9999-12-31') AS EndDate
FROM
    staging.hr
        CROSS JOIN
    min_date
WHERE
        EmployeeJobCode = 314;