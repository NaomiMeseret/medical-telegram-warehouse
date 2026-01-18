{{
    config(
        materialized='table',
        schema='marts'
    )
}}

with staging_messages as (
    select * from {{ ref('stg_telegram_messages') }}
),

channel_metrics as (
    select
        channel_name,
        min(message_date) as first_post_date,
        max(message_date) as last_post_date,
        count(*) as total_posts,
        avg(views) as avg_views,
        sum(case when has_image then 1 else 0 end) as total_images,
        count(distinct date_trunc('day', message_date)) as active_days
    from staging_messages
    group by channel_name
),

channel_classification as (
    select
        channel_name,
        case
            when lower(channel_name) like '%pharma%' 
                or lower(channel_name) like '%med%' 
                or lower(channel_name) like '%drug%' then 'Pharmaceutical'
            when lower(channel_name) like '%cosmetic%' 
                or lower(channel_name) like '%beauty%' then 'Cosmetics'
            when lower(channel_name) like '%medical%' 
                or lower(channel_name) like '%health%' then 'Medical'
            else 'Other'
        end as channel_type
    from channel_metrics
),

dim_channels as (
    select
        -- Surrogate key (using row_number for simplicity)
        row_number() over (order by cm.channel_name) as channel_key,
        
        -- Natural key
        cm.channel_name,
        
        -- Channel classification
        cc.channel_type,
        
        -- Date metrics
        cm.first_post_date,
        cm.last_post_date,
        date_part('day', cm.last_post_date - cm.first_post_date) as days_active,
        
        -- Activity metrics
        cm.total_posts,
        cm.active_days,
        round(cm.total_posts::numeric / nullif(cm.active_days, 0), 2) as avg_posts_per_day,
        
        -- Engagement metrics
        round(cm.avg_views, 2) as avg_views,
        cm.total_images,
        round((cm.total_images::numeric / nullif(cm.total_posts, 0)) * 100, 2) as image_percentage
        
    from channel_metrics cm
    inner join channel_classification cc
        on cm.channel_name = cc.channel_name
)

select * from dim_channels
