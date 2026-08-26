{{ config(schema='silver') }}
select
    *
    
from {{ source('jaffle_shop', 'customers') }}