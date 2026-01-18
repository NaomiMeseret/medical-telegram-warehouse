#!/bin/bash
# Fix "database is locked" error by removing session lock files

cd "/Users/naomi/Shipping a Data Product"

echo "Checking for running scraper processes..."

# Check if scraper is running
if ps aux | grep -v grep | grep -q "scraper.py"; then
    echo "⚠️  WARNING: Scraper is still running!"
    echo "Please stop it first with Ctrl+C or: kill <PID>"
    echo ""
    ps aux | grep -v grep | grep "scraper.py"
    exit 1
fi

echo "✅ No scraper processes found. Safe to remove lock files."
echo ""

# Remove session journal file (lock file)
if [ -f "telegram_scraper.session-journal" ]; then
    echo "Removing telegram_scraper.session-journal..."
    rm -f telegram_scraper.session-journal
    echo "✅ Lock file removed!"
else
    echo "ℹ️  No lock file found."
fi

echo ""
echo "✅ Session unlocked! You can now run the scraper:"
echo "   python src/scraper.py"
