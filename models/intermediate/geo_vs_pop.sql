SELECT
p.municipality_code,
p.year,
p.population,
p.country_code,
g.city_name,
g.city_name_normalized,
g.department_code,
g.department_name
FROM {{ ref('stg_projet_prello__population_by_municipality') }} AS p
JOIN {{ ref('stg_projet_prello__geographical_referential') }} AS g 
ON p.municipality_code = g.municipality_code
WHERE year = 2019