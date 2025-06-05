SELECT 

ROUND(AVG (intensite_tension_immo),2) AS intensite_immo_dpt,
ROUND(AVG (rental_med_all),2) AS rental_med_dpt,
country_name,
department_name,
department_code
FROM {{ ref('real_estate_info_by_municipality_dpt') }} 
GROUP BY country_name, department_code, department_name
ORDER BY intensite_immo_dpt DESC, rental_med_dpt DESC