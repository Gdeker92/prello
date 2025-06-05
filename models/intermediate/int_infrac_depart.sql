--j'identifie ma moyenne d'infraction sur 5 ans par région (il est dans le from)
--Je peux ensuite identifier mon min et mon max

WITH stats AS (
  SELECT 
    MIN(avg_infractions) AS min_infractions,
    MAX(avg_infractions) AS max_infractions
  FROM (
    SELECT 
      ROUND((Annee_2018 + Annee_2019 + Annee_2020 + Annee_2021 + Annee_2022) / 5.0, 0) AS avg_infractions
    FROM {{ ref('stg_projet_prello__infraction_depart') }}
  )
),
-- concat mon dpartement pour faire une bonne mise en forme (en lien avec le doataset Geoloc)
sum_infrac AS (
  SELECT 
    CONCAT('FR-', department) AS department,
    ROUND((Annee_2018 + Annee_2019 + Annee_2020 + Annee_2021 + Annee_2022) / 5.0, 0) AS avg_infractions
  FROM {{ ref('stg_projet_prello__infraction_depart') }}
)

-- Calcul pondéré du score d'infraction par département
SELECT
  si.department,
  si.avg_infractions,
  ROUND(
    ((stats.max_infractions - si.avg_infractions) / 
     (stats.max_infractions - stats.min_infractions)) * 5,
    2
  ) AS infrac_score
FROM sum_infrac si, stats
ORDER BY infrac_score DESC