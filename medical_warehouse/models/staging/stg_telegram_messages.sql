{{
    config(
        materialized='view',
        schema='staging'
    )
}}

with raw_messages as (
    select * from {{ source('raw', 'telegram_messages') }}
),

cleaned_messages as (
    select
        -- Primary identifiers
        message_id,
        channel_name,
        
        -- Date/time fields
        message_date,
        date_trunc('day', message_date) as message_date_day,
        
        -- Text content
        coalesce(message_text, '') as message_text,
        length(coalesce(message_text, '')) as message_length,
        
        -- Media flags
        coalesce(has_media, false) as has_media,
        case 
            when image_path is not null and image_path != '' then true
            else false
        end as has_image,
        image_path,
        
        -- Engagement metrics
        coalesce(views, 0) as views,
        coalesce(forwards, 0) as forwards,
        
        -- Metadata
        loaded_at
        
    from raw_messages
    
    -- Filter out invalid records
    where message_id is not null
        and channel_name is not null
        and message_date is not null
        
        -- Remove future dates (data quality check)
        and message_date <= current_timestamp
)

select * from cleaned_messages
