select
    *
    
from {{ source('staging', 'stores') }}