with 

source as (

    select * from {{ source('projet_prello', 'notary_real_estate_sales') }}

),

renamed as (

    select
        sales_date,
        sales_amount,
        street_number,
        street_code,
        street_name,
        nom_commune,
        municipality_code,
        premise_type,
        surface,
        number_of_principal_rooms,
        sales_price_m2,
        latitude,
        longitude

    from source

)

select * from renamed
