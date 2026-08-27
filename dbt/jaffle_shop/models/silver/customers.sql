{{ config(schema='silver') }}
select
    *
    
from {{ source('staging', 'customers') }}