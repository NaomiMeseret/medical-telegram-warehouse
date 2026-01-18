{{
    config(
        materialized='table',
        schema='marts'
    )
}}

with date_spine as (
    select distinct 
        date_trunc('day', message_date) as date_day
    from {{ ref('stg_telegram_messages') }}
),

dim_dates as (
    select
        -- Surrogate key: date as integer (YYYYMMDD)
        to_char(date_day, 'YYYYMMDD')::integer as date_key,
        
        -- Full date
        date_day as full_date,
        
        -- Day of week (1 = Monday, 7 = Sunday)
        extract(dow from date_day) + 1 as day_of_week,
        to_char(date_day, 'Day') as day_name,
        to_char(date_day, 'Dy') as day_name_short,
        
        -- Week
        extract(week from date_day) as week_of_year,
        date_trunc('week', date_day) as week_start_date,
        
        -- Month
        extract(month from date_day) as month,
        to_char(date_day, 'Month') as month_name,
        to_char(date_day, 'Mon') as month_name_short,
        
        -- Quarter
        extract(quarter from date_day) as quarter,
        case 
            when extract(quarter from date_day) = 1 then 'Q1'
            when extract(quarter from date_day) = 2 then 'Q2'
            when extract(quarter from date_day) = 3 then 'Q3'
            else 'Q4'
        end as quarter_name,
        
        -- Year
        extract(year from date_day) as year,
        
        -- Flags
        case 
            when extract(dow from date_day) in (0, 6) then true
            else false
        end as is_weekend,
        
        case 
            when date_day = current_date then true
            else false
        end as is_today,
        
        case 
            when date_day = current_date - interval '1 day' then true
            else false
        end as is_yesterday,
        
        -- Date parts for filtering
        extract(day from date_day) as day_of_month,
        extract(doy from date_day) as day_of_year
        
    from date_spine
)

select * from dim_dates
