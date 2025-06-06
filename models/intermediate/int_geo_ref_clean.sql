--je clean le dataset pour que les données puissent sortir sur looker
-- Changer le country name en string "France"
SELECT
  CASE 
    WHEN CAST(country_code AS STRING) = '01' THEN 'France'
    ELSE CAST(country_code AS STRING)
  END AS country_name,
  department_name,
LOWER(REGEXP_REPLACE(TRIM(department_name), r"[^a-zA-Z0-9]", "")) AS department_for_sol,
department_code,
epci_code,
city_name,
city_name_normalized,
municipality_code,

--créer les coordonnées concat sous format looker

CONCAT(CAST(latitude AS STRING), ',', CAST(longitude AS STRING)) AS coordonnees

FROM {{ ref('stg_projet_prello__geographical_referential') }}
