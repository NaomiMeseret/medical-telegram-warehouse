#!/usr/bin/env python3
"""
Load raw Telegram JSON data from data lake into PostgreSQL database.

This script:
1. Reads JSON files from data/raw/telegram_messages/
2. Creates raw schema and table if not exists
3. Loads messages into raw.telegram_messages table
4. Handles duplicates and updates
"""

import os
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any
from dotenv import load_dotenv

import psycopg2
from psycopg2.extras import execute_batch
from psycopg2 import sql


# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'database': os.getenv('POSTGRES_DB', 'medical_warehouse'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', 'postgres')
}

# Paths
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / 'data' / 'raw' / 'telegram_messages'


def create_raw_schema(conn):
    """Create raw schema and table if they don't exist."""
    with conn.cursor() as cur:
        # Create raw schema
        cur.execute("""
            CREATE SCHEMA IF NOT EXISTS raw;
        """)
        
        # Create raw.telegram_messages table
        cur.execute("""
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
        """)
        
        # Create indexes for better query performance
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_telegram_messages_channel 
            ON raw.telegram_messages(channel_name);
        """)
        
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_telegram_messages_date 
            ON raw.telegram_messages(message_date);
        """)
        
        conn.commit()
        logger.info("Raw schema and table created successfully")


def load_json_file(file_path: Path) -> List[Dict[str, Any]]:
    """Load messages from a JSON file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            if isinstance(data, list):
                return data
            else:
                logger.warning(f"File {file_path} does not contain a list")
                return []
    except Exception as e:
        logger.error(f"Error loading {file_path}: {e}")
        return []


def insert_messages(conn, messages: List[Dict[str, Any]], channel_name: str):
    """Insert messages into raw.telegram_messages table."""
    if not messages:
        logger.warning(f"No messages to insert for channel {channel_name}")
        return 0
    
    # Prepare data for insertion
    insert_data = []
    for msg in messages:
        # Parse message_date
        message_date = None
        if msg.get('message_date'):
            try:
                message_date = datetime.fromisoformat(msg['message_date'].replace('Z', '+00:00'))
            except Exception as e:
                logger.warning(f"Error parsing date {msg.get('message_date')}: {e}")
        
        insert_data.append((
            msg.get('message_id'),
            msg.get('channel_name') or channel_name,
            message_date,
            msg.get('message_text'),
            msg.get('has_media', False),
            msg.get('image_path'),
            msg.get('views'),
            msg.get('forwards'),
            datetime.now()
        ))
    
    # Insert using ON CONFLICT to handle duplicates
    insert_query = """
        INSERT INTO raw.telegram_messages 
        (message_id, channel_name, message_date, message_text, has_media, image_path, views, forwards, loaded_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (message_id, channel_name) 
        DO UPDATE SET
            message_date = EXCLUDED.message_date,
            message_text = EXCLUDED.message_text,
            has_media = EXCLUDED.has_media,
            image_path = EXCLUDED.image_path,
            views = EXCLUDED.views,
            forwards = EXCLUDED.forwards,
            loaded_at = EXCLUDED.loaded_at;
    """
    
    try:
        with conn.cursor() as cur:
            execute_batch(cur, insert_query, insert_data, page_size=1000)
            conn.commit()
            logger.info(f"Inserted/updated {len(insert_data)} messages from {channel_name}")
            return len(insert_data)
    except Exception as e:
        conn.rollback()
        logger.error(f"Error inserting messages for {channel_name}: {e}")
        raise


def get_json_files(data_dir: Path) -> List[Path]:
    """Get all JSON files from data lake."""
    json_files = []
    if data_dir.exists():
        json_files = list(data_dir.rglob('*.json'))
    else:
        logger.warning(f"Data directory not found: {data_dir}")
    return json_files


def main():
    """Main function to load data into PostgreSQL."""
    logger.info("Starting data loading process...")
    
    # Connect to database
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info(f"Connected to PostgreSQL database: {DB_CONFIG['database']}")
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        logger.error(f"Check your database configuration in .env file")
        return
    
    try:
        # Create raw schema and table
        create_raw_schema(conn)
        
        # Get all JSON files
        json_files = get_json_files(DATA_DIR)
        logger.info(f"Found {len(json_files)} JSON file(s) to process")
        
        if not json_files:
            logger.warning("No JSON files found. Make sure you've run the scraper first.")
            return
        
        # Load each JSON file
        total_messages = 0
        for json_file in json_files:
            logger.info(f"Processing file: {json_file}")
            
            # Extract channel name from filename
            channel_name = json_file.stem
            
            # Load messages from JSON
            messages = load_json_file(json_file)
            
            if messages:
                # Insert into database
                count = insert_messages(conn, messages, channel_name)
                total_messages += count
        
        # Summary
        logger.info("=" * 60)
        logger.info("DATA LOADING SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Files processed: {len(json_files)}")
        logger.info(f"Total messages loaded: {total_messages}")
        
        # Check total messages in database
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM raw.telegram_messages;")
            total_in_db = cur.fetchone()[0]
            logger.info(f"Total messages in database: {total_in_db}")
        
    except Exception as e:
        logger.error(f"Error during data loading: {e}")
        raise
    finally:
        conn.close()
        logger.info("Database connection closed")


if __name__ == '__main__':
    main()
