"""
Pydantic schemas for request/response validation.
"""

from typing import List, Optional
from pydantic import BaseModel, Field
from datetime import datetime


# Common models
class ChannelInfo(BaseModel):
    """Channel information model."""
    channel_name: str
    channel_type: Optional[str] = None
    total_posts: Optional[int] = None


# Endpoint 1: Top Products
class TopProduct(BaseModel):
    """Top product model."""
    product_term: str = Field(..., description="Product or term mentioned")
    mention_count: int = Field(..., description="Number of times mentioned")
    channels: List[str] = Field(..., description="Channels where mentioned")


class TopProductsResponse(BaseModel):
    """Response for top products endpoint."""
    products: List[TopProduct]
    total_products: int
    limit: int


# Endpoint 2: Channel Activity
class ChannelActivityDay(BaseModel):
    """Daily activity statistics."""
    date: str
    post_count: int
    total_views: int
    avg_views: float
    total_forwards: int


class ChannelActivityResponse(BaseModel):
    """Response for channel activity endpoint."""
    channel_name: str
    channel_type: Optional[str]
    total_posts: int
    date_range: dict = Field(..., description="First and last post dates")
    daily_activity: List[ChannelActivityDay]
    summary: dict = Field(..., description="Summary statistics")


# Endpoint 3: Message Search
class SearchMessage(BaseModel):
    """Message search result."""
    message_id: int
    channel_name: str
    message_text: str
    message_date: datetime
    views: int
    forwards: int


class MessageSearchResponse(BaseModel):
    """Response for message search endpoint."""
    messages: List[SearchMessage]
    total_found: int
    query: str
    limit: int


# Endpoint 4: Visual Content Stats
class ChannelVisualStats(BaseModel):
    """Visual content statistics per channel."""
    channel_name: str
    total_images: int
    promotional_count: int
    product_display_count: int
    lifestyle_count: int
    other_count: int
    avg_confidence: Optional[float] = None


class VisualContentResponse(BaseModel):
    """Response for visual content statistics endpoint."""
    channels: List[ChannelVisualStats]
    total_images: int
    category_distribution: dict = Field(..., description="Overall category counts")


# Error responses
class ErrorResponse(BaseModel):
    """Error response model."""
    error: str
    detail: Optional[str] = None
