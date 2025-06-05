with 

source as (

    select * from {{ source('projet_prello', 'louis_annee_vente') }}

),

renamed as (

    select
        annee,
        department_code,
        department_name,
        nb_ventes_maisons,
        nb_ventes_appartements,
        total_ventes

    from source

)

select * from renamed
