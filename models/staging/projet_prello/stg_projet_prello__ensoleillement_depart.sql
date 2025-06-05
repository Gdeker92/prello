with 

source as (

    select * from {{ source('projet_prello', 'ensoleillement_depart') }}

),

renamed as (

    select
        d__partements,
        temps_d_enseillement__jours_an_

    from source

)

select * from renamed
