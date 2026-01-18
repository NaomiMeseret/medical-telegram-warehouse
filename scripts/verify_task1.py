#!/usr/bin/env python3
"""
Verification script for Task 1 deliverables.

This script verifies that all Task 1 deliverables are present:
1. Working scraper script (src/scraper.py)
2. Raw JSON files in the data lake structure
3. Downloaded images organized by channel
4. Log files showing scraping activity
"""

import os
import json
from pathlib import Path
from datetime import datetime

# Colors for terminal output
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
BLUE = '\033[94m'
RESET = '\033[0m'

BASE_DIR = Path(__file__).parent.parent

def check_scraper_script():
    """Check if scraper script exists and is valid."""
    print(f"\n{BLUE}✓ Checking scraper script...{RESET}")
    
    scraper_path = BASE_DIR / 'src' / 'scraper.py'
    
    if not scraper_path.exists():
        print(f"{RED}✗ Scraper script not found: {scraper_path}{RESET}")
        return False
    
    # Check if file has content
    if scraper_path.stat().st_size == 0:
        print(f"{RED}✗ Scraper script is empty{RESET}")
        return False
    
    # Check for key imports
    content = scraper_path.read_text()
    required_components = [
        'TelegramClient',
        'TelegramScraper',
        'async def scrape_channel',
        'download_image',
        'save_messages_to_datalake'
    ]
    
    missing = [comp for comp in required_components if comp not in content]
    
    if missing:
        print(f"{YELLOW}⚠ Scraper script exists but may be incomplete. Missing: {missing}{RESET}")
        return False
    
    print(f"{GREEN}✓ Scraper script found and appears valid{RESET}")
    print(f"  Path: {scraper_path}")
    print(f"  Size: {scraper_path.stat().st_size:,} bytes")
    return True


def check_json_files():
    """Check if JSON files exist in data lake structure."""
    print(f"\n{BLUE}✓ Checking JSON files in data lake...{RESET}")
    
    messages_dir = BASE_DIR / 'data' / 'raw' / 'telegram_messages'
    
    if not messages_dir.exists():
        print(f"{RED}✗ Data lake directory not found: {messages_dir}{RESET}")
        return False
    
    # Find all JSON files
    json_files = list(messages_dir.rglob('*.json'))
    
    if not json_files:
        print(f"{RED}✗ No JSON files found in data lake{RESET}")
        print(f"  Expected location: {messages_dir}/YYYY-MM-DD/channel_name.json")
        return False
    
    print(f"{GREEN}✓ Found {len(json_files)} JSON file(s){RESET}")
    
    # Check structure
    total_messages = 0
    for json_file in json_files:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, list):
                    total_messages += len(data)
                    print(f"  • {json_file.relative_to(BASE_DIR)}: {len(data)} messages")
        except Exception as e:
            print(f"{YELLOW}⚠ Error reading {json_file.name}: {e}{RESET}")
    
    print(f"{GREEN}✓ Total messages in data lake: {total_messages:,}{RESET}")
    return True


def check_images():
    """Check if images are downloaded and organized by channel."""
    print(f"\n{BLUE}✓ Checking downloaded images...{RESET}")
    
    images_dir = BASE_DIR / 'data' / 'raw' / 'images'
    
    if not images_dir.exists():
        print(f"{RED}✗ Images directory not found: {images_dir}{RESET}")
        return False
    
    # Find all image directories (channels)
    channel_dirs = [d for d in images_dir.iterdir() if d.is_dir()]
    
    if not channel_dirs:
        print(f"{RED}✗ No channel directories found in images folder{RESET}")
        return False
    
    print(f"{GREEN}✓ Found {len(channel_dirs)} channel directory(ies){RESET}")
    
    total_images = 0
    for channel_dir in channel_dirs:
        # Count image files
        image_files = list(channel_dir.glob('*.jpg')) + list(channel_dir.glob('*.jpeg'))
        total_images += len(image_files)
        
        if len(image_files) > 0:
            print(f"  • {channel_dir.name}: {len(image_files)} image(s)")
        else:
            print(f"{YELLOW}  ⚠ {channel_dir.name}: No images found{RESET}")
    
    if total_images == 0:
        print(f"{RED}✗ No images found in any channel directory{RESET}")
        return False
    
    print(f"{GREEN}✓ Total images downloaded: {total_images:,}{RESET}")
    return True


def check_log_files():
    """Check if log files exist and show scraping activity."""
    print(f"\n{BLUE}✓ Checking log files...{RESET}")
    
    logs_dir = BASE_DIR / 'logs'
    
    if not logs_dir.exists():
        print(f"{RED}✗ Logs directory not found: {logs_dir}{RESET}")
        return False
    
    # Find log files
    log_files = list(logs_dir.glob('scraper_*.log'))
    
    if not log_files:
        print(f"{RED}✗ No log files found{RESET}")
        return False
    
    print(f"{GREEN}✓ Found {len(log_files)} log file(s){RESET}")
    
    # Check log content
    has_activity = False
    for log_file in sorted(log_files, reverse=True)[:3]:  # Check last 3 logs
        try:
            content = log_file.read_text()
            
            # Check for key log indicators
            indicators = [
                'Starting to scrape channel',
                'Downloaded image',
                'Successfully processed',
                'SCRAPING SUMMARY',
                'Scraping completed'
            ]
            
            found_indicators = [ind for ind in indicators if ind in content]
            
            if found_indicators:
                has_activity = True
                print(f"  • {log_file.name}: Contains scraping activity")
                
                # Show summary if available
                if 'SCRAPING SUMMARY' in content:
                    lines = content.split('\n')
                    for i, line in enumerate(lines):
                        if 'SCRAPING SUMMARY' in line:
                            # Print next 5 lines
                            for j in range(1, 6):
                                if i + j < len(lines):
                                    summary_line = lines[i + j].strip()
                                    if summary_line and '=' not in summary_line:
                                        print(f"    {summary_line}")
                            break
            else:
                print(f"{YELLOW}  ⚠ {log_file.name}: No clear scraping activity found{RESET}")
                
        except Exception as e:
            print(f"{YELLOW}  ⚠ Error reading {log_file.name}: {e}{RESET}")
    
    if not has_activity:
        print(f"{RED}✗ No scraping activity found in log files{RESET}")
        return False
    
    return True


def main():
    """Run all verification checks."""
    print("=" * 70)
    print(f"{BLUE}Task 1 Deliverables Verification{RESET}")
    print("=" * 70)
    
    results = []
    
    # Run all checks
    results.append(("Scraper Script", check_scraper_script()))
    results.append(("JSON Files", check_json_files()))
    results.append(("Downloaded Images", check_images()))
    results.append(("Log Files", check_log_files()))
    
    # Summary
    print("\n" + "=" * 70)
    print(f"{BLUE}Verification Summary{RESET}")
    print("=" * 70)
    
    all_passed = True
    for name, passed in results:
        status = f"{GREEN}✓ PASS{RESET}" if passed else f"{RED}✗ FAIL{RESET}"
        print(f"{name:25} {status}")
        if not passed:
            all_passed = False
    
    print("=" * 70)
    
    if all_passed:
        print(f"\n{GREEN}🎉 All Task 1 deliverables are satisfied!{RESET}")
        print(f"{GREEN}✓ Task 1 is complete and ready for Task 2{RESET}")
        return 0
    else:
        print(f"\n{RED}⚠ Some deliverables are missing or incomplete{RESET}")
        print(f"{YELLOW}Please review the issues above and ensure all requirements are met{RESET}")
        return 1


if __name__ == '__main__':
    exit(main())
