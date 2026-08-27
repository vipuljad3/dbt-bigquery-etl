select
    *
    
from {{ source('staging', 'orders') }}