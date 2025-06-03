with 

source as (

    select * from {{ source('projet_prello', 'housing_stock') }}

),

renamed as (

    select
        int64_field_0,
        municipality_code,
        year,
        nb_principal_home,
        nb_second_home,
        nb_vacants_housing,
        nb_tot_housing,
        secondary_home_rate,
        principal_home_rate,
        vacants_housing_rate,
        country_code

    from source

)

select * from renamed
