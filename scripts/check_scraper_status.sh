#!/bin/bash
# Check scraper status and monitor progress

cd "/Users/naomi/Shipping a Data Product"

echo "🔍 Checking scraper status..."
echo ""

# Check if scraper is running
if ps aux | grep -v grep | grep -q "scraper.py"; then
    echo "✅ Scraper IS running:"
    ps aux | grep -v grep | grep "scraper.py" | awk '{print "   PID: " $2 " | Started: " $9 " " $10 " | CPU: " $3 "% | Memory: " $4 "%"}'
    echo ""
    
    # Check recent log activity
    LATEST_LOG=$(ls -t logs/scraper_*.log 2>/dev/null | head -1)
    if [ -f "$LATEST_LOG" ]; then
        echo "📋 Recent activity (last 5 lines):"
        tail -5 "$LATEST_LOG" | sed 's/^/   /'
        echo ""
        
        # Check for download activity
        if tail -100 "$LATEST_LOG" | grep -q "Downloaded image"; then
            echo "✅ Images are being downloaded!"
            echo ""
            echo "💡 To monitor progress in real-time:"
            echo "   tail -f $LATEST_LOG"
        fi
        
        # Check if completed
        if tail -20 "$LATEST_LOG" | grep -q "Scraping completed"; then
            echo "✅ Scraper has completed!"
        fi
    fi
    
    echo ""
    echo "⏳ Wait for scraper to finish, or stop it with: kill 3153"
else
    echo "❌ No scraper is running"
    echo ""
    echo "✅ You can start the scraper:"
    echo "   python src/scraper.py"
fi
