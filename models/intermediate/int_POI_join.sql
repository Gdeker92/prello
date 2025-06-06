-- Étape 1 : Jointure des hébergements et sites touristiques par commune
WITH joined_table AS (
  SELECT
    e.municipality_code,
    e.poi AS type_logement,
    e.importance AS importance_heber,
    e.name_reprocessed AS name,
    CONCAT(e.latitude, ",", e.longitude) AS coordonnees_heberg, -- coordonnées hébergement

    s.poi AS tourism_area,
    s.importance AS importance_site,
    s.name_reprocessed AS name_site,
    CONCAT(s.latitude, ",", s.longitude) AS coordonnees_activites -- coordonnées activité (corrigé)
  FROM {{ ref('stg_projet_prello__POI_tourist_establishments') }} AS e
  JOIN {{ ref('stg_projet_prello__POI_touristic_sites_by_municipality') }} AS s
    USING (municipality_code)
),

-- Étape 2 : Classification des sites selon leur typologie
type_site AS (
  SELECT
    *,
    CASE
      -- Culture et patrimoine
      WHEN tourism_area IN (
        '1', '2','world_heritage', 'heritage', 'historical_monuments', 'monument',
        'castle', 'museum', 'cinema', 'theatre', 'vineyard'
      ) THEN 'culture/patrimoine'

      -- Nature et paysages
      WHEN tourism_area IN (
        'park', 'national_park', 'protected_area', 'forest', 'valley',
        'wetland', 'meadow', 'viewpoint', 'volcano', 'geyser', 'waterfall',
        'dune', 'rock', 'sand', 'beach', 'cliff', 'islet', 'ridge',
        'cave_entrance', 'water', 'allotments'
      ) THEN 'nature/paysages'

      -- Loisirs et bien-être
      WHEN tourism_area IN (
        'zoo', 'attraction', 'theme_park', 'water_park', 'golf_course',
        'casino', 'marina', 'wreck'
      ) THEN 'loisir/bien-être'

      -- Autre ou non classé
      ELSE 'autre'
    END AS typologie_site_touristique
  FROM joined_table
),

-- Étape 3 : Calcul des scores moyens d’importance par commune (hébergement et site)
moyenne_prepa_score AS (
  SELECT
    municipality_code,
    type_logement,
    AVG(importance_heber) OVER (PARTITION BY municipality_code) AS score_heberg, -- score par commune pour l’hébergement
    name,
    coordonnees_heberg,

    typologie_site_touristique,
    tourism_area,
    AVG(importance_site) OVER (PARTITION BY municipality_code) AS score_site, -- score par commune pour le site
    name_site,
    coordonnees_activites
  FROM type_site
),

-- Étape 4 : Calcul du min et max global pour normalisation
score_final AS (
  SELECT
    *,
    MIN(score_site) OVER () AS min_score_site,
    MIN(score_heberg) OVER () AS min_score_heberg,
    MAX(score_site) OVER () AS max_score_site,
    MAX(score_heberg) OVER () AS max_score_heberg
  FROM moyenne_prepa_score
),

-- Étape 5 : Normalisation min-max pour avoir des scores entre 0 et 1
score_normalise AS (
  SELECT
    *,
    -- Normalisation du score site
    CASE
      WHEN max_score_site = min_score_site THEN 0.5 -- cas limite
      ELSE (score_site - min_score_site) / (max_score_site - min_score_site)
    END AS score_site_normalise,

    -- Normalisation du score hébergement
    CASE
      WHEN max_score_heberg = min_score_heberg THEN 0.5 -- cas limite
      ELSE (score_heberg - min_score_heberg) / (max_score_heberg - min_score_heberg)
    END AS score_heberg_normalise
  FROM score_final
),

-- Étape 6 : Projection des scores normalisés sur une échelle de 1 à 5
score_pondere AS (
  SELECT
    *,
    ROUND((score_site_normalise * 4) + 1, 2) AS score_site_pondere, -- score site sur 5
    ROUND((score_heberg_normalise * 4) + 1, 2) AS score_heberg_pondere -- score hébergement sur 5
  FROM score_normalise
),

-- Étape 7 : Calcul du score touristique final pondéré (ex : 60% site, 40% hébergement)
score_touristique_final AS (
  SELECT
    *,
    ROUND((score_site_pondere * 0.6) + (score_heberg_pondere * 0.4), 2) AS score_touristique
  FROM score_pondere
)

-- Étape finale : Sélection des champs pour analyse ou visualisation
SELECT
  municipality_code, -- ma granularité (commune)

  -- Infos hébergement
  type_logement,
  name,
  coordonnees_heberg,
  score_heberg_pondere, -- score touristique hébergement (sur 5)

  -- Infos site touristique
  typologie_site_touristique,
  tourism_area,
  name_site,
  coordonnees_activites,
  score_site_pondere, -- score touristique site (sur 5)

  -- Score global touristique (pondéré)
  score_touristique
FROM score_touristique_final
