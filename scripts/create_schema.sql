-- Create database schema for medical telegram warehouse
-- Run this script to set up the database structure

-- Create raw schema for raw data
CREATE SCHEMA IF NOT EXISTS raw;

-- Create staging schema for dbt transformations
CREATE SCHEMA IF NOT EXISTS staging;

-- Create marts schema for dimensional model
CREATE SCHEMA IF NOT EXISTS marts;

-- Grant permissions (adjust as needed for your setup)
-- GRANT USAGE ON SCHEMA raw TO dbt_user;
-- GRANT USAGE ON SCHEMA staging TO dbt_user;
-- GRANT USAGE ON SCHEMA marts TO dbt_user;

-- Create raw.telegram_messages table
-- This table will be populated by load_to_postgres.py script
CREATE TABLE IF NOT EXISTS raw.telegram_messages (
    message_id BIGINT NOT NULL,
    channel_name VARCHAR(255) NOT NULL,
    message_date TIMESTAMP,
    message_text TEXT,
    has_media BOOLEAN DEFAULT FALSE,
    image_path VARCHAR(1000),
    views INTEGER,
    forwards INTEGER,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (message_id, channel_name)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_telegram_messages_channel 
ON raw.telegram_messages(channel_name);

CREATE INDEX IF NOT EXISTS idx_telegram_messages_date 
ON raw.telegram_messages(message_date);

CREATE INDEX IF NOT EXISTS idx_telegram_messages_has_media 
ON raw.telegram_messages(has_media);

-- Comments for documentation
COMMENT ON SCHEMA raw IS 'Raw data from Telegram scraping pipeline';
COMMENT ON SCHEMA staging IS 'Cleaned and standardized data (dbt staging models)';
COMMENT ON SCHEMA marts IS 'Dimensional model for analytics (dbt marts)';

COMMENT ON TABLE raw.telegram_messages IS 'Raw messages scraped from Telegram channels';
