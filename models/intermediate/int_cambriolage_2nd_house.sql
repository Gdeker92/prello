WITH data_source AS (
  SELECT
    departement_code,
    ROUND((_2018 + _2019 + _2020 + _2021 +_2022) / 5.0, 0) AS avg_cambiriolage_maison_secondaire
  FROM {{ ref('stg_projet_prello__cambriolage_2nd_house') }}
),

min_max AS (
  SELECT
    MIN(avg_cambiriolage_maison_secondaire) AS min_camb,
    MAX(avg_cambiriolage_maison_secondaire) AS max_camb
  FROM data_source
)

SELECT 
  ds.departement_code,
  ds.avg_cambiriolage_maison_secondaire,
  ROUND(
    SAFE_DIVIDE(
      (ds.avg_cambiriolage_maison_secondaire - mm.min_camb),
      (mm.max_camb - mm.min_camb)
    ) * 5,
    2
  ) AS score_cambriolage
FROM data_source AS ds
CROSS JOIN min_max mm
WHERE ds.departement_code IS NOT NULL