SELECT 
  department_code,
  ROUND(AVG(median_m2),2) AS avg_median_m2
FROM {{ ref('prix_m²_par_an_par_commune') }}
WHERE department_code NOT LIKE '%97%'
GROUP BY department_code
ORDER BY avg_median_m2 DESC