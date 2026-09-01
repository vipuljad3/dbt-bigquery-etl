{{
    config(
        materialized='incremental',
        unique_key='ID'
    )
}}

select
    *
from {{ source('staging', 'monthly_counts_per_hour') }}

{% if is_incremental() %}
    -- this filter only applies on incremental runs
    where SENSING_DATE >= (select max(SENSING_DATE) from {{ this }})
{% endif %}