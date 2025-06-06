WITH stats AS (
  SELECT 
    MIN(Temps_d_enseillement__jours_an_) AS min_ensoleillement,
    MAX(Temps_d_enseillement__jours_an_) AS max_ensoleillement
  FROM `projet-prello-461709.projet_prello.ensoleillement_depart`
)
,

score_clean AS(
SELECT
  LOWER(REGEXP_REPLACE(TRIM(D__partements), r"[^a-zA-Z0-9]", "")) AS department,
  Temps_d_enseillement__jours_an_ AS j_ensoleillement,
  ROUND(
    ((Temps_d_enseillement__jours_an_ - stats.min_ensoleillement) / 
    (stats.max_ensoleillement - stats.min_ensoleillement)) * 5, 
    2
    
  ) AS soleil_score
FROM {{ ref('stg_projet_prello__ensoleillement_depart') }}, stats
ORDER BY soleil_score DESC
)

SELECT 
    CASE
        WHEN department = "eureetloire" THEN "eureetloir"
    ELSE department
    END AS department,
    j_ensoleillement,
    soleil_score

FROM score_clean
