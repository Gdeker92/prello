WITH joined_table AS (
  SELECT
    e.municipality_code,
    e.poi AS type_logement,
    e.importance AS importance_heber,
    -- XXXX as score_heb
    e.name_reprocessed AS name,
    CONCAT(e.latitude, ",", e.longitude) AS coordonnees_heberg,

    s.poi AS tourism_area,
    s.importance AS importance_site,
    -- XXX as score_tourism
    s.name_reprocessed,
    CONCAT(e.latitude, ",", e.longitude) AS coordonnees_activites

  FROM {{ ref('stg_projet_prello__POI_tourist_establishments') }} AS e
  JOIN {{ ref('stg_projet_prello__POI_touristic_sites_by_municipality') }} AS s
    USING (municipality_code)
),

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

moyenne_prepa_score AS (
  SELECT
    municipality_code,
    type_logement,
    importance_heber,
    -- score par municipalité heberg
    AVG(importance_heber) OVER (PARTITION BY municipality_code) AS score_heberg,
    name,
    coordonnees_heberg,

    typologie_site_touristique,
    tourism_area,
    importance_site,
    -- score par municipalité site
    AVG(importance_site) OVER (PARTITION BY municipality_code) AS score_site,
    name_reprocessed AS name_site,
    coordonnees_activites
  FROM type_site
),

score_final AS (
  SELECT
    *,
    -- Étape 1: Calcul min et max sur toute la table (via fenêtre sans partition)
    MIN(score_site) OVER () AS min_score_site,
    MIN(score_heberg) OVER () AS min_score_heberg,
    MAX(score_site) OVER () AS max_score_site,
    MAX(score_heberg) OVER () AS max_score_heberg
  FROM moyenne_prepa_score
),

score_normalise AS (
  SELECT
    *,
    -- Étape 2: Normalisation min-max (valeur comprise entre 0 et 1) score site
    CASE
      WHEN max_score_site = min_score_site THEN 0.5  -- éviter division par 0 si tous scores égaux
      ELSE (score_site - min_score_site) / (max_score_site - min_score_site)
    END AS score_site_normalise,
        --Normalisation min-max (valeur comprise entre 0 et 1) score heberg
    CASE
      WHEN max_score_heberg = min_score_heberg THEN 0.5  -- éviter division par 0 si tous scores égaux
      ELSE (score_heberg - min_score_heberg) / (max_score_heberg - min_score_heberg)
    END AS score_heberg_normalise
  FROM score_final
),

score_pondere AS (
  SELECT
    *,
    -- Étape 3: Projection sur une échelle de 1 à 5
    ROUND((score_site_normalise * 4) + 1, 2) AS score_site_pondere,
    ROUND(score_heberg_normalise * 4 + 1, 2) AS score_heberg_pondere
  FROM score_normalise
),
score_touristique_final AS (
  SELECT
    *,
    -- Calcul du score touristique pondéré combiné (exemple : 60% site, 40% hébergement)
    ROUND((score_site_pondere * 0.6)+ (score_heberg_pondere * 0.4), 2) AS score_touristique
  FROM score_pondere
)

  SELECT
        municipality_code, -- ma granularité

        type_logement, -- mes infos hébergement
        name,
        coordonnees_heberg,
        score_heberg_pondere, -- mon score touristique d'hébergement

        typologie_site_touristique, -- mes infos sites tourist
        tourism_area,
        name_site,
        coordonnees_activites,
        score_site_pondere, -- mon score touristique de sites

        score_touristique -- mon score global touristique

  FROM score_touristique_final