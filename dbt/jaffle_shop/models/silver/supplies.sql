select
    *
    
from {{ source('staging', 'supplies') }}