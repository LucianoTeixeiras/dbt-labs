
with
    markup as (
        select
            customer_id,
            company_name,
            contact_name,
            contact_title,
            address,
            city,
            region,
            postal_code,
            country,
            phone,
            first_value(customer_id) over (
                partition by company_name, contact_name
                order by company_name
                rows between unbounded preceding and unbounded following
            ) as result
        from {{ source("sources", "customers") }}
    ),
    removed as (select distinct result from markup),
    final as (
        select
            customer_id,
            company_name,
            contact_name,
            contact_title,
            address,
            city,
            region,
            postal_code,
            country,
            phone
        from {{ source("sources", "customers") }}
        join removed on customer_id = result
    )
select
    customer_id,
    company_name,
    contact_name,
    contact_title,
    address,
    city,
    region,
    postal_code,
    country,
    phone
from final
