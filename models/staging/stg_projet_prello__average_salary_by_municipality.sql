with 

source as (

    select * from {{ source('projet_prello', 'average_salary_by_municipality') }}

),

renamed as (

    select *

    from source

)

select * from renamed
