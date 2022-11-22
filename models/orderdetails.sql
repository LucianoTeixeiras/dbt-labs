select * from {{ source("sources", "order_details") }}
