"""
FastAPI application for Medical Telegram Warehouse Analytics API.

Provides REST endpoints to query the data warehouse and answer business questions.
"""

from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Optional
from datetime import datetime

from api.database import get_db
from api.schemas import (
    TopProductsResponse, TopProduct,
    ChannelActivityResponse, ChannelActivityDay,
    MessageSearchResponse, SearchMessage,
    VisualContentResponse, ChannelVisualStats,
    ErrorResponse
)

# Initialize FastAPI app
app = FastAPI(
    title="Medical Telegram Warehouse API",
    description="Analytical API for Ethiopian medical marketplace insights",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)


@app.get("/", tags=["General"])
async def root():
    """Root endpoint with API information."""
    return {
        "name": "Medical Telegram Warehouse API",
        "version": "1.0.0",
        "description": "Analytical API for Ethiopian medical marketplace insights",
        "endpoints": {
            "top_products": "/api/reports/top-products",
            "channel_activity": "/api/channels/{channel_name}/activity",
            "message_search": "/api/search/messages",
            "visual_content": "/api/reports/visual-content"
        },
        "documentation": "/docs"
    }


@app.get(
    "/api/reports/top-products",
    response_model=TopProductsResponse,
    tags=["Analytics"],
    summary="Get Top Mentioned Products",
    description="Returns the most frequently mentioned terms/products across all channels"
)
async def get_top_products(
    limit: int = Query(10, ge=1, le=100, description="Number of top products to return"),
    db: Session = Depends(get_db)
):
    """
    Endpoint 1: Top Products
    
    Analyzes message_text in fct_messages to find most frequently mentioned products.
    Uses simple word frequency analysis (can be enhanced with NLP in the future).
    """
    try:
        # Simple approach: count common product-related terms
        # In production, this could use NLP, entity extraction, or a product dictionary
        query = text("""
            WITH word_counts AS (
                SELECT 
                    unnest(string_to_array(lower(message_text), ' ')) as word,
                    channel_name
                FROM marts.fct_messages
                WHERE message_text IS NOT NULL AND message_text != ''
            ),
            filtered_words AS (
                SELECT word, channel_name
                FROM word_counts
                WHERE length(word) > 3
                AND word NOT IN ('this', 'that', 'with', 'from', 'have', 'been', 'will', 'were', 'there', 'their', 'them', 'these', 'those')
            ),
            product_mentions AS (
                SELECT 
                    word as product_term,
                    COUNT(*) as mention_count,
                    array_agg(DISTINCT channel_name) as channels
                FROM filtered_words
                GROUP BY word
                HAVING COUNT(*) >= 2
                ORDER BY mention_count DESC
                LIMIT :limit
            )
            SELECT product_term, mention_count, channels
            FROM product_mentions
        """)
        
        result = db.execute(query, {"limit": limit})
        rows = result.fetchall()
        
        products = [
            TopProduct(
                product_term=row[0],
                mention_count=row[1],
                channels=row[2]
            )
            for row in rows
        ]
        
        return TopProductsResponse(
            products=products,
            total_products=len(products),
            limit=limit
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching top products: {str(e)}")


@app.get(
    "/api/channels/{channel_name}/activity",
    response_model=ChannelActivityResponse,
    tags=["Channels"],
    summary="Get Channel Activity",
    description="Returns posting activity and trends for a specific channel"
)
async def get_channel_activity(
    channel_name: str,
    db: Session = Depends(get_db)
):
    """
    Endpoint 2: Channel Activity
    
    Returns daily posting activity, view counts, and engagement metrics for a channel.
    """
    try:
        # Get channel metadata
        channel_query = text("""
            SELECT channel_name, channel_type, total_posts, first_post_date, last_post_date
            FROM marts.dim_channels
            WHERE channel_name = :channel_name
        """)
        
        channel_result = db.execute(channel_query, {"channel_name": channel_name})
        channel_row = channel_result.fetchone()
        
        if not channel_row:
            raise HTTPException(status_code=404, detail=f"Channel '{channel_name}' not found")
        
        # Get daily activity
        activity_query = text("""
            SELECT 
                dd.full_date::date as date,
                COUNT(*) as post_count,
                SUM(fm.view_count) as total_views,
                AVG(fm.view_count) as avg_views,
                SUM(fm.forward_count) as total_forwards
            FROM marts.fct_messages fm
            INNER JOIN marts.dim_channels dc ON fm.channel_key = dc.channel_key
            INNER JOIN marts.dim_dates dd ON fm.date_key = dd.date_key
            WHERE dc.channel_name = :channel_name
            GROUP BY dd.full_date::date
            ORDER BY dd.full_date::date DESC
        """)
        
        activity_result = db.execute(activity_query, {"channel_name": channel_name})
        activity_rows = activity_result.fetchall()
        
        daily_activity = [
            ChannelActivityDay(
                date=row[0].strftime("%Y-%m-%d"),
                post_count=row[1],
                total_views=row[2] or 0,
                avg_views=float(row[3] or 0),
                total_forwards=row[4] or 0
            )
            for row in activity_rows
        ]
        
        # Calculate summary
        total_views_all = sum(day.total_views for day in daily_activity)
        total_forwards_all = sum(day.total_forwards for day in daily_activity)
        avg_views_overall = total_views_all / len(daily_activity) if daily_activity else 0
        
        return ChannelActivityResponse(
            channel_name=channel_row[0],
            channel_type=channel_row[1],
            total_posts=channel_row[2],
            date_range={
                "first_post": channel_row[3].isoformat() if channel_row[3] else None,
                "last_post": channel_row[4].isoformat() if channel_row[4] else None
            },
            daily_activity=daily_activity,
            summary={
                "total_days_active": len(daily_activity),
                "total_views": total_views_all,
                "total_forwards": total_forwards_all,
                "avg_views_per_day": round(avg_views_overall, 2)
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching channel activity: {str(e)}")


@app.get(
    "/api/search/messages",
    response_model=MessageSearchResponse,
    tags=["Search"],
    summary="Search Messages",
    description="Searches for messages containing a specific keyword"
)
async def search_messages(
    query: str = Query(..., min_length=2, description="Search keyword"),
    limit: int = Query(20, ge=1, le=100, description="Maximum number of results"),
    db: Session = Depends(get_db)
):
    """
    Endpoint 3: Message Search
    
    Searches message_text in fct_messages for messages containing the query term.
    """
    try:
        search_query = text("""
            SELECT 
                fm.message_id,
                dc.channel_name,
                fm.message_text,
                fm.message_date,
                fm.view_count,
                fm.forward_count
            FROM marts.fct_messages fm
            INNER JOIN marts.dim_channels dc ON fm.channel_key = dc.channel_key
            WHERE LOWER(fm.message_text) LIKE LOWER(:search_term)
            ORDER BY fm.message_date DESC
            LIMIT :limit
        """)
        
        search_term = f"%{query}%"
        result = db.execute(search_query, {"search_term": search_term, "limit": limit})
        rows = result.fetchall()
        
        messages = [
            SearchMessage(
                message_id=row[0],
                channel_name=row[1],
                message_text=row[2],
                message_date=row[3],
                views=row[4],
                forwards=row[5]
            )
            for row in rows
        ]
        
        # Get total count (could be expensive for large datasets, so we'll approximate)
        count_query = text("""
            SELECT COUNT(*)
            FROM marts.fct_messages
            WHERE LOWER(message_text) LIKE LOWER(:search_term)
        """)
        count_result = db.execute(count_query, {"search_term": search_term})
        total_found = count_result.scalar() or 0
        
        return MessageSearchResponse(
            messages=messages,
            total_found=total_found,
            query=query,
            limit=limit
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error searching messages: {str(e)}")


@app.get(
    "/api/reports/visual-content",
    response_model=VisualContentResponse,
    tags=["Analytics"],
    summary="Visual Content Statistics",
    description="Returns statistics about image usage across channels"
)
async def get_visual_content_stats(
    db: Session = Depends(get_db)
):
    """
    Endpoint 4: Visual Content Stats
    
    Returns statistics about image usage, YOLO classifications, and visual content patterns.
    """
    try:
        stats_query = text("""
            SELECT 
                dc.channel_name,
                COUNT(*) as total_images,
                SUM(CASE WHEN fid.image_category = 'promotional' THEN 1 ELSE 0 END) as promotional_count,
                SUM(CASE WHEN fid.image_category = 'product_display' THEN 1 ELSE 0 END) as product_display_count,
                SUM(CASE WHEN fid.image_category = 'lifestyle' THEN 1 ELSE 0 END) as lifestyle_count,
                SUM(CASE WHEN fid.image_category = 'other' THEN 1 ELSE 0 END) as other_count,
                AVG(fid.top_confidence) as avg_confidence
            FROM marts.fct_image_detections fid
            INNER JOIN marts.dim_channels dc ON fid.channel_key = dc.channel_key
            GROUP BY dc.channel_name
            ORDER BY total_images DESC
        """)
        
        result = db.execute(stats_query)
        rows = result.fetchall()
        
        channels = [
            ChannelVisualStats(
                channel_name=row[0],
                total_images=row[1],
                promotional_count=row[2] or 0,
                product_display_count=row[3] or 0,
                lifestyle_count=row[4] or 0,
                other_count=row[5] or 0,
                avg_confidence=float(row[6] or 0) if row[6] else None
            )
            for row in rows
        ]
        
        # Calculate overall category distribution
        category_query = text("""
            SELECT 
                image_category,
                COUNT(*) as count
            FROM marts.fct_image_detections
            GROUP BY image_category
        """)
        
        category_result = db.execute(category_query)
        category_rows = category_result.fetchall()
        
        category_distribution = {row[0]: row[1] for row in category_rows}
        
        total_images = sum(ch.total_images for ch in channels)
        
        return VisualContentResponse(
            channels=channels,
            total_images=total_images,
            category_distribution=category_distribution
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching visual content stats: {str(e)}")


@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler."""
    return JSONResponse(
        status_code=500,
        content={"error": "Internal server error", "detail": str(exc)}
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
