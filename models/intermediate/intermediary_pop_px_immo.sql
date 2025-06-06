SELECT
m.municipality_code,
m.year,
m.population,
g.city_name,
g.city_name_normalized,
g.department_code,
g.country_code,
g.department_name,
n.sales_date,
n.sales_amount,
n.nom_commune,
n.sales_price_m2
FROM {{ ref('stg_projet_prello__population_by_municipality') }} AS m
JOIN {{ ref('stg_projet_prello__geographical_referential') }} AS g
ON m.municipality_code = g.municipality_code 
JOIN {{ ref('stg_projet_prello__notary_real_estate_sales') }} AS n
ON m.municipality_code = n.municipality_code