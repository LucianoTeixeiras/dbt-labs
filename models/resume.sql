with
    products as (
        select
            ct.category_name,
            sp.company_name as suppliers,
            pd.product_name,
            pd.unit_price,
            pd.product_id
        from {{ source("sources", "products") }} pd
        left join
            {{ source("sources", "suppliers") }} sp on pd.supplier_id = sp.supplier_id
        left join
            {{ source("sources", "categories") }} ct on pd.category_id = ct.category_id
    ),
    orderdetails as (
        select
            pd.category_name,
            pd.suppliers,
            pd.product_name,
            pd.unit_price,
            pd.product_id,
            od.order_id,
            od.quantity,
            od.discount
        from {{ ref("orderdetails") }} od
        left join products pd on od.product_id = pd.product_id
    ),
    orders as (
        select
            od.order_date,
            od.order_id,
            cs.company_name as customer,
            emp.name as employee,
            emp.age,
            emp.lengthofservice
        from {{ source("sources", "orders") }} od
        left join {{ ref("customers") }} cs on od.customer_id = cs.customer_id
        left join {{ ref("employees") }} emp on od.employee_id = emp.employee_id
        left join {{ source("sources", "shippers") }} sh on od.ship_via = sh.shipper_id
    )
select * from orders