WITH subquery AS (
SELECT
  nres.municipality_code,
  hs.year,
  APPROX_QUANTILES(nres.sales_price_m2, 100)[OFFSET(50)] AS median_m2
FROM {{ ref('stg_projet_prello__notary_real_estate_sales') }} AS nres
JOIN {{ ref('stg_projet_prello__housing_stock') }} AS hs
  ON nres.municipality_code = hs.municipality_code
WHERE nres.sales_price_m2 IS NOT NULL
  AND hs.year = 2018
GROUP BY nres.municipality_code, hs.year
ORDER BY nres.municipality_code ASC, hs.year ASC
)
SELECT 
s.municipality_code,
s.year,
l.department_code,
s.median_m2
FROM subquery AS s 
JOIN {{ ref('int_geo_ref_clean') }} AS l 
  ON s.municipality_code = l.municipality_code
WHERE department_code NOT LIKE '%97%'
    AND department_code NOT IN ('FR-67','FR-68')
ORDER BY s.municipality_code ASC, s.year ASC
