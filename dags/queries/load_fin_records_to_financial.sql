SELECT
  cmp.SK_CompanyID AS SK_CompanyID,
  fin.Year AS FI_YEAR,
  fin.Quarter AS FI_QTR,
  fin.QtrStartDate AS FI_QTR_START_DATE,
  fin.Revenue AS FI_REVENUE,
  fin.Earnings AS FI_NET_EARN,
  fin.EPS AS FI_BASIC_EPS,
  fin.DilutedEPS AS FI_DILUT_EPS,
  fin.Margin AS FI_MARGIN,
  fin.Inventory AS FI_INVENTORY,
  fin.Assets AS FI_ASSETS,
  fin.Liabilities AS FI_LIABILITY,
  fin.ShOut AS FI_OUT_BASIC,
  fin.DilutedShOut AS FI_OUT_DILUT
FROM
  staging.fin_records fin
JOIN
  master.dim_company cmp
ON
  ((fin.CIK IS NOT NULL
      AND fin.CIK = cmp.CompanyID)
    OR (fin.CIK IS NULL
      AND fin.CompanyName = cmp.Name))
  AND DATE(fin.PTS) >= cmp.EffectiveDate
  AND DATE(fin.PTS) < cmp.EndDate;