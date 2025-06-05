SELECT 

re.municipality_code,
intensite_tension_immo,
rental_max_apartment,
rental_min_apartment,
rental_med_house,
rental_max_house,
rental_min_house,
rental_med_all,
rental_max_all,
rental_min_all,
country_name,
department_name,
department_code,
epci_code,
city_name
,city_name_normalized
,coordonnees
FROM {{ ref('stg_projet_prello__real_estate_info_by_municipality') }} AS re
JOIN {{ ref('int_geo_ref_clean') }} AS geo 
ON re.municipality_code = geo.municipality_code
