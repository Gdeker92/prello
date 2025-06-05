SELECT *
FROM {{ ref('stg_projet_prello__real_estate_info_by_municipality') }} AS re
JOIN {{ ref('int_geo_ref_clean') }} AS geo 
ON re.municipality_code = geo.municipality_code