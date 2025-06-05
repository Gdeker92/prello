SELECT 
    pdt.department_code,
    pdt.avg_median_m2,
    re.intensite_immo_dpt,
    re.rental_med_dpt,
    ROUND(SAFE_DIVIDE(re.rental_med_dpt * 100, pdt.avg_median_m2) * 12, 2) AS renta_brute_dpt
FROM {{ ref('prix_m²_par_an_par_department') }} AS pdt
JOIN {{ ref('real_estate_info_by_dpt') }} AS re
    ON pdt.department_code = re.department_code
ORDER BY renta_brute_dpt DESC
