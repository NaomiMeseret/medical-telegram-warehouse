# Medical Telegram Warehouse

An end-to-end data pipeline for Telegram medical channels, leveraging dbt for transformation, Dagster for orchestration, and YOLOv8 for data enrichment.

## 📋 Table of Contents

- [Overview](#overview)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Step-by-Step Setup Guide](#step-by-step-setup-guide)
- [Task 1: Data Scraping and Collection](#task-1-data-scraping-and-collection)
- [Next Steps](#next-steps)

## 🎯 Overview

This project builds a robust data platform that generates actionable insights about Ethiopian medical businesses using data scraped from public Telegram channels.

**Key Features:**
- Telegram data extraction using Telethon
- Data lake storage (JSON files and images)
- PostgreSQL data warehouse
- dbt transformations with star schema modeling
- YOLOv8 image detection for data enrichment
- FastAPI analytical API
- Dagster orchestration

## 📁 Project Structure

```
medical-telegram-warehouse/
├── .vscode/                 # VS Code settings
├── .github/workflows/      # CI/CD workflows
├── data/                   # Data storage
│   └── raw/               # Raw data lake
│       ├── images/        # Downloaded images
│       └── telegram_messages/  # JSON message files
├── logs/                  # Log files
├── medical_warehouse/     # dbt project
│   ├── models/
│   │   ├── staging/      # Staging models
│   │   └── marts/        # Mart models (star schema)
│   └── tests/            # dbt tests
├── src/                   # Source code
│   └── scraper.py        # Telegram scraper
├── api/                   # FastAPI application
├── notebooks/            # Jupyter notebooks
├── tests/                # Unit tests
└── scripts/              # Utility scripts
```

## 📦 Prerequisites

Before you begin, ensure you have:

1. **Python 3.11+** installed
2. **PostgreSQL 15+** (or use Docker)
3. **Telegram Account** (for API access)
4. **Git** (for version control)
5. **Docker & Docker Compose** (optional, for containerized setup)

## 🚀 Step-by-Step Setup Guide

### Step 1: Clone and Navigate to Project

```bash
cd "/Users/naomi/Shipping a Data Product"
```

### Step 2: Create Python Virtual Environment

```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
# On macOS/Linux:
source venv/bin/activate
# On Windows:
# venv\Scripts\activate
```

### Step 3: Install Dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### Step 4: Set Up Telegram API Credentials

1. **Go to** [https://my.telegram.org/apps](https://my.telegram.org/apps)
2. **Log in** with your Telegram account
3. **Create a new application**:
   - App title: "Medical Warehouse Scraper" (or any name)
   - Short name: "medwarehouse" (or any short name)
   - Platform: Desktop
   - Description: Optional
4. **Copy your credentials**:
   - `api_id`: A number (e.g., 12345678)
   - `api_hash`: A string (e.g., "abcdef1234567890abcdef1234567890")

### Step 5: Configure Environment Variables

1. **Copy the example environment file**:
   ```bash
   cp .env.example .env
   ```

2. **Edit `.env` file** and add your credentials:
   ```bash
   # Open .env in your editor
   nano .env  # or use VS Code, vim, etc.
   ```

3. **Fill in your Telegram credentials**:
   ```env
   TELEGRAM_API_ID=your_actual_api_id
   TELEGRAM_API_HASH=your_actual_api_hash
   TELEGRAM_PHONE=+1234567890  # Your phone number with country code
   ```

### Step 6: Start PostgreSQL Database (Using Docker)

```bash
# Start PostgreSQL container
docker-compose up -d postgres

# Verify it's running
docker-compose ps
```

**Alternative:** If you have PostgreSQL installed locally, update the `.env` file with your local database credentials.

### Step 7: Verify Setup

```bash
# Check if Python can import required packages
python -c "import telethon; print('Telethon installed successfully')"
```

## 📊 Task 1: Data Scraping and Collection

### Objective

Build a data scraping pipeline that extracts messages and images from Telegram channels and stores them in a raw data lake.

### Instructions

#### Step 1: Test Telegram Connection

First, let's verify your Telegram credentials work:

```bash
# Run the scraper (it will prompt for verification code on first run)
python src/scraper.py
```

**First-time setup:**
1. The script will send a verification code to your Telegram account
2. Enter the code when prompted
3. If you have 2FA enabled, enter your password when prompted
4. The session will be saved for future runs

#### Step 2: Run the Scraper

Once authenticated, the scraper will:

1. **Connect to Telegram** using your credentials
2. **Scrape messages** from configured channels:
   - `chemed`
   - `lobelia4cosmetics`
   - `tikvahpharma`
   - (and more from your `.env` file)
3. **Download images** (if present in messages)
4. **Save data** to the data lake structure

```bash
# Run the scraper
python src/scraper.py
```

#### Step 3: Verify Data Collection

Check that data has been collected:

```bash
# Check JSON files (messages)
ls -la data/raw/telegram_messages/

# Check images
ls -la data/raw/images/

# Check logs
ls -la logs/
cat logs/scraper_*.log
```

#### Step 4: Inspect Collected Data

```bash
# View a sample JSON file
cat data/raw/telegram_messages/$(date +%Y-%m-%d)/chemed.json | head -50

# Check image structure
find data/raw/images -type f | head -10
```

### Data Lake Structure

The scraper creates the following structure:

```
data/raw/
├── telegram_messages/
│   └── YYYY-MM-DD/          # Partitioned by date
│       ├── chemed.json      # Messages from each channel
│       ├── lobelia4cosmetics.json
│       └── tikvahpharma.json
└── images/
    ├── chemed/              # Images organized by channel
    │   ├── 12345.jpg        # Image files named by message_id
    │   └── 12346.jpg
    ├── lobelia4cosmetics/
    └── tikvahpharma/
```

### Data Fields Collected

Each message JSON contains:

- `message_id`: Unique identifier
- `channel_name`: Telegram channel name
- `message_date`: ISO timestamp
- `message_text`: Full message content
- `has_media`: Boolean indicating media presence
- `image_path`: Relative path to downloaded image
- `views`: Number of views
- `forwards`: Number of forwards







---

**Happy Data Engineering! 🚀**
