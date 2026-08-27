select
    *
    
from {{ source('staging', 'items') }}