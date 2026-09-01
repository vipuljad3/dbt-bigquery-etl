select
    *
    
from {{ source('staging', 'sensor_locations') }}