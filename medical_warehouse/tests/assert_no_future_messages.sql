-- Custom test: Ensure no messages have future dates
-- This test passes if it returns 0 rows

select 
    message_id,
    channel_name,
    message_date
from {{ ref('stg_telegram_messages') }}
where message_date > current_timestamp
