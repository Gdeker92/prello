with 

source as (

    select * from {{ source('projet_prello', 'population_by_municipality') }}

),

renamed as (

    select
        municipality_code,
        year,
        population,
        country_code

    from source

)

select * from renamed
