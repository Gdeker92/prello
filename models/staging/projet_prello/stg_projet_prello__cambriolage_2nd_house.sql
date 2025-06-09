with 

source as (

    select * from {{ source('projet_prello', 'cambriolage_2nd_house') }}

),

renamed as (

    select
        departement_code,
        _2018,
        _2019,
        _2020,
        _2021,
        _2022,
        avg_cambiriolage_maison_secondaire

    from source

)

select * from renamed
