with 

source as (

    select * from {{ source('projet_prello', 'infraction_depart') }}

),

renamed as (

    select
        string_field_0,
        2018,
        2019,
        2020,
        2021,
        2022

    from source

)

select * from renamed
