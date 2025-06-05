with 

source as (

    select * from {{ source('projet_prello', 'louis_departement') }}

),

renamed as (

    select
        department_code,
        department_name,
        municipality_code,
        nom_commune,
        full_location

    from source

)

select * from renamed
