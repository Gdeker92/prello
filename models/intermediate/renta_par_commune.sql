SELECT 
    px.municipality_code,
    px.year,
    px.department_code,
    px.median_m2,
    re.intensite_tension_immo,
    re.rental_med_all,
    re.city_name_normalized,
    re.coordonnees,
    ROUND(SAFE_DIVIDE(re.rental_med_all*12, px.median_m2),2)*100 AS renta_brute
FROM {{ ref('prix_m²_par_an_par_commune') }} AS px
JOIN {{ ref('real_estate_info_by_municipality_dpt') }} AS re
    ON px.municipality_code = re.municipality_code
ORDER BY renta_brute DESC 