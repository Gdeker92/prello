WITH poi_depart AS(
SELECT
    country_name,
    department_code,
    department_name,
    department_for_sol,
    --- faire le score moyen par département
    ROUND(AVG(poi.score_touristique), 2) AS avg_score_touristique,
    --- ajoute le nombre de site touristique par région
    SUM(CASE WHEN typologie_site_touristique = 'nature/paysages' THEN 1 ELSE 0 END) AS nb_site_nature_paysages,
    SUM(CASE WHEN typologie_site_touristique = 'culture/patrimoine' THEN 1 ELSE 0 END) AS nb_site_culture_patrimoine,
    SUM(CASE WHEN typologie_site_touristique = 'loisir/bien-être' THEN 1 ELSE 0 END) AS nb_site_loisir_bien_etre

FROM {{ ref('int_POI_join') }} AS poi
JOIN {{ ref('int_geo_ref_clean') }} AS geo
    USING (municipality_code)
GROUP BY department_code, department_name, department_for_sol, country_name

)


SELECT 
geo2.country_name,
geo2.department_code,
geo2.department_name,
ROUND((geo2.avg_score_touristique + sol.soleil_score + (6 - inf.infrac_score)) / 3, 2) AS score_attractivite_region,
geo2.avg_score_touristique,
sol.j_ensoleillement,
sol.soleil_score,
inf.avg_infractions,
inf.infrac_score
FROM poi_depart as geo2
LEFT JOIN {{ ref('int_j_ensoleillement') }} AS sol
    ON geo2.department_for_sol = sol.department
LEFT JOIN {{ ref('int_infrac_depart') }} AS inf
    ON geo2.department_code = inf.department