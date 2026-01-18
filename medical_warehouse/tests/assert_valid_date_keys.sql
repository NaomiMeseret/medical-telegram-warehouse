-- Custom test: Ensure all fact table date_keys exist in dim_dates
-- This test passes if it returns 0 rows

select 
    f.date_key,
    f.message_id
from {{ ref('fct_messages') }} f
left join {{ ref('dim_dates') }} d
    on f.date_key = d.date_key
where d.date_key is null
