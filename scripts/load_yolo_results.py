#!/usr/bin/env python3
"""
Load YOLO detection results CSV into PostgreSQL database.
"""

import os
import csv
import logging
from pathlib import Path
from typing import Dict, Any
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

# Debug: Log connection details (without exposing password)
logger.debug(f"Connecting to PostgreSQL: {DB_CONFIG['user']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")

# Paths
BASE_DIR = Path(__file__).parent.parent
YOLO_CSV = BASE_DIR / 'data' / 'processed' / 'yolo_detections.csv'


def create_raw_yolo_table(conn):
    """Create raw.yolo_detections table if it doesn't exist."""
    with conn.cursor() as cur:
        # Create raw schema if it doesn't exist
        cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
        
        # Create raw.yolo_detections table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.yolo_detections (
                message_id BIGINT NOT NULL,
                channel_name VARCHAR(255) NOT NULL,
                image_path VARCHAR(1000),
                detected_objects_count INTEGER,
                detected_classes TEXT,
                top_detected_class VARCHAR(255),
                top_confidence FLOAT,
                image_category VARCHAR(50),
                all_confidences TEXT,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (message_id, channel_name)
            );
        """)
        
        # Create indexes
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_yolo_detections_channel 
            ON raw.yolo_detections(channel_name);
        """)
        
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_yolo_detections_category 
            ON raw.yolo_detections(image_category);
        """)
        
        conn.commit()
        logger.info("Raw YOLO detections table created/verified")


def load_yolo_results(conn, csv_file: Path):
    """Load YOLO results from CSV into PostgreSQL."""
    if not csv_file.exists():
        logger.error(f"YOLO results CSV not found: {csv_file}")
        return 0
    
    with conn.cursor() as cur:
        # Clear existing data (or use upsert if preferred)
        cur.execute("TRUNCATE TABLE raw.yolo_detections;")
        logger.info("Cleared existing YOLO detection data")
        
        # Read CSV and load data
        rows = []
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append((
                    int(row['message_id']),
                    row['channel_name'],
                    row['image_path'],
                    int(row['detected_objects_count']) if row['detected_objects_count'] else 0,
                    row['detected_classes'] or None,
                    row['top_detected_class'] or None,
                    float(row['top_confidence']) if row['top_confidence'] else 0.0,
                    row['image_category'],
                    row['all_confidences'] or None
                ))
        
        # Batch insert
        insert_query = """
            INSERT INTO raw.yolo_detections 
            (message_id, channel_name, image_path, detected_objects_count, 
             detected_classes, top_detected_class, top_confidence, image_category, all_confidences)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (message_id, channel_name) 
            DO UPDATE SET
                image_path = EXCLUDED.image_path,
                detected_objects_count = EXCLUDED.detected_objects_count,
                detected_classes = EXCLUDED.detected_classes,
                top_detected_class = EXCLUDED.top_detected_class,
                top_confidence = EXCLUDED.top_confidence,
                image_category = EXCLUDED.image_category,
                all_confidences = EXCLUDED.all_confidences,
                loaded_at = CURRENT_TIMESTAMP;
        """
        
        execute_batch(cur, insert_query, rows)
        conn.commit()
        
        logger.info(f"Loaded {len(rows)} YOLO detection results into database")
        return len(rows)


def main():
    """Main function to load YOLO results."""
    logger.info("Starting YOLO results loading...")
    
    try:
        # Connect to database
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info(f"Connected to PostgreSQL database: {DB_CONFIG['database']}")
        
        # Create table
        create_raw_yolo_table(conn)
        
        # Load results
        if YOLO_CSV.exists():
            count = load_yolo_results(conn, YOLO_CSV)
            logger.info(f"✅ Successfully loaded {count} YOLO detection results")
        else:
            logger.error(f"YOLO CSV file not found: {YOLO_CSV}")
            logger.info("Please run src/yolo_detect.py first to generate detection results")
        
        conn.close()
        
    except Exception as e:
        logger.error(f"Error loading YOLO results: {e}")
        raise


if __name__ == '__main__':
    main()
