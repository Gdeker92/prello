--j'identifie ma moyenne d'infraction sur 5 ans par région (il est dans le from)
-- concat mon dpartement pour faire une bonne mise en forme (en lien avec le dataset Geoloc)

WITH cal_avg AS (
SELECT 
    CONCAT('FR-', department) AS department,
    ROUND((Annee_2018 + Annee_2019 + Annee_2020 + Annee_2021 + Annee_2022) / 5.0, 0) AS avg_infractions
FROM {{ ref('stg_projet_prello__infraction_depart') }}
  )
  ,
-- calcul des min et max de l'avg
min_max AS (
  SELECT 
    MIN(avg_infractions) AS min_infractions,
    MAX(avg_infractions) AS max_infractions
  FROM cal_avg
    
)

-- Calcul pondéré du score d'infraction par département
SELECT
  cal.department,
  cal.avg_infractions,
  ROUND(
    ((cal.avg_infractions - mm.min_infractions) / 
     (mm.max_infractions - mm.min_infractions)) * 5,
    2
  ) AS infrac_score
FROM cal_avg as cal
CROSS JOIN min_max as mm
ORDER BY infrac_score DESC