-- Prix médian au m² par année / commune
WITH price_by_year AS (
  SELECT
    nres.municipality_code,
    hs.year,
    PERCENTILE_CONT(nres.sales_price_m2, 0.5) 
      OVER (PARTITION BY nres.municipality_code, hs.year) AS median_m2
  FROM {{ ref('stg_projet_prello__notary_real_estate_sales') }} AS nres
  JOIN {{ ref('stg_projet_prello__housing_stock') }} AS hs
    ON nres.municipality_code = hs.municipality_code
  WHERE nres.sales_price_m2 IS NOT NULL
    AND hs.year BETWEEN 1968 AND 2018
),

-- Taux de vacance par commune / année
vacancy_by_city AS (
  SELECT
    hs.municipality_code,
    hs.year,
    ROUND(SAFE_DIVIDE(nb_vacants_housing, nb_tot_housing) * 100, 2) AS vacancy_rate
  FROM {{ ref('stg_projet_prello__housing_stock') }} AS hs
  WHERE hs.nb_tot_housing > 0
    AND hs.year BETWEEN 1968 AND 2018
),

-- jointure des prix, taux de vacance et département par année / commune
combined AS (
  SELECT
    p.year,
    p.municipality_code,
    gr.department_name,
    p.median_m2,
    v.vacancy_rate
  FROM price_by_year p
  JOIN vacancy_by_city v
    ON p.municipality_code = v.municipality_code AND p.year = v.year
  JOIN {{ ref('stg_projet_prello__geographical_referential') }} AS gr
    ON p.municipality_code = gr.municipality_code
),

-- Communes par taux de vacance
categorized AS (
  SELECT *,
    CASE 
      WHEN vacancy_rate < 5 THEN 'faible vacance (<5%)'
      WHEN vacancy_rate BETWEEN 5 AND 10 THEN 'modérée (5-10%)'
      ELSE 'forte vacance (>10%)'
    END AS vacancy_group
  FROM combined
)

-- Évolution du prix moyen par groupe de vacance / année 
SELECT DISTINCT
  department_name,
  year,
  vacancy_group,
  ROUND(AVG(median_m2), 0) AS avg_median_m2
FROM categorized
GROUP BY year, department_name, vacancy_group
ORDER BY department_name, year ASC