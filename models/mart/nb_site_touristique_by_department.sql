SELECT 
geo.department_code,
geo.department_name,
site.municipality_code,
site.typologie_site_touristique,
site.tourism_area,
site.name_site,
site.coordonnees_activites,
site.score_site_pondere AS score_site_touristique


FROM {{ ref('int_POI_join') }} AS site
LEFT JOIN {{ ref('int_geo_ref_clean') }} AS geo
ON geo.municipality_code = site.municipality_code