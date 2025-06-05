WITH stats AS (
  SELECT 
    MIN(Temps_d_enseillement__jours_an_) AS min_ensoleillement,
    MAX(Temps_d_enseillement__jours_an_) AS max_ensoleillement
  FROM `projet-prello-461709.projet_prello.ensoleillement_depart`
)

SELECT
  D__partements AS department,
  Temps_d_enseillement__jours_an_ AS j_ensoleillement,
  ROUND(
    ((Temps_d_enseillement__jours_an_ - stats.min_ensoleillement) / 
    (stats.max_ensoleillement - stats.min_ensoleillement)) * 5, 
    2
    
  ) AS soleil_score
FROM {{ ref('stg_projet_prello__ensoleillement_depart') }}, stats
ORDER BY soleil_score DESC