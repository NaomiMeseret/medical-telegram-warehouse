-- Custom test: Ensure view counts are non-negative
-- This test passes if it returns 0 rows

select 
    message_id,
    channel_name,
    views
from {{ ref('stg_telegram_messages') }}
where views < 0
