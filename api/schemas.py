"""
schemas.py — response models. FastAPI turns these into the OpenAPI/Swagger
spec automatically (served at /docs) — this *is* the API contract
documentation tiket.com's JD asks for, generated from code instead of
hand-maintained in a separate doc that goes stale.
"""
from datetime import date, datetime
from typing import Optional
from pydantic import BaseModel


class Article(BaseModel):
    article_id: int
    title: str
    category: Optional[str] = None
    author_id: int
    date_day: date
    content_length: Optional[int] = None


class ArticlePage(BaseModel):
    items: list[Article]
    limit: int
    offset: int
    total: int


class Author(BaseModel):
    author_id: int
    author_name: Optional[str] = None
    total_articles: int
    first_published_date: Optional[date] = None
    last_published_date: Optional[date] = None


class DQScorecard(BaseModel):
    run_at: datetime
    data_source: str
    table_name: str
    completeness: Optional[float] = None
    accuracy: Optional[float] = None
    consistency: Optional[float] = None
    timeliness: Optional[float] = None
    validity: Optional[float] = None
    uniqueness: Optional[float] = None
    overall_score: Optional[float] = None


class HealthCheck(BaseModel):
    status: str
    dwh_reachable: bool
