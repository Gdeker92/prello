with 

source as (

    select * from {{ source('projet_prello', 'infraction_depart') }}

),

renamed as (

    select
        string_field_0 AS department,
        int64_field_1 AS Annee_2018,
        int64_field_2 AS Annee_2019,
        int64_field_3 AS Annee_2020,
        int64_field_4 AS Annee_2021,
        int64_field_5 AS Annee_2022

    from source

)

select * from renamed
