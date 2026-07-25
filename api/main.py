"""
main.py — Data-as-a-Service API over the DWH marts.

This is the "bridge between big data systems and end-users/other
product teams" piece from the tiket.com JD: instead of every consumer
running their own SQL against the warehouse, they hit a documented,
versioned HTTP contract. Swagger/OpenAPI docs are auto-generated at
/docs from schemas.py — that's the "API contract documentation"
requirement, sourced from code instead of a hand-written doc that
drifts from reality.

Run locally (outside Docker):
    uvicorn main:app --reload --port 8000
Or via the bundled service:
    docker compose up -d api
    open http://localhost:8000/docs
"""
from typing import Optional

from auth import require_api_key
from db import get_cursor
from dependencies import get_article_repository, get_author_repository, get_dq_repository
from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from repositories import ArticleRepository, AuthorRepository, DQRepository
from schemas import Article, ArticlePage, Author, DQScorecard, HealthCheck

app = FastAPI(
    title="news-lakehouse-pipeline API",
    description=(
        "Data-as-a-Service layer over the news lakehouse DWH marts. "
        "Read-only; write access happens exclusively through the Airflow "
        "pipeline, never through this API."
    ),
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten to real frontend origins in production
    allow_methods=["GET"],
)


@app.get("/health", response_model=HealthCheck, tags=["ops"])
def health():
    try:
        with get_cursor() as cur:
            cur.execute("SELECT 1")
        return HealthCheck(status="ok", dwh_reachable=True)
    except Exception:
        return HealthCheck(status="degraded", dwh_reachable=False)


@app.get("/articles", response_model=ArticlePage, tags=["articles"], dependencies=[Depends(require_api_key)])
def list_articles(
    limit: int = Query(20, ge=1, le=200),
    offset: int = Query(0, ge=0),
    category: Optional[str] = None,
    repo: ArticleRepository = Depends(get_article_repository),
):
    items, total = repo.list_articles(limit=limit, offset=offset, category=category)
    return ArticlePage(items=items, limit=limit, offset=offset, total=total)


@app.get("/articles/{article_id}", response_model=Article, tags=["articles"], dependencies=[Depends(require_api_key)])
def get_article(article_id: int, repo: ArticleRepository = Depends(get_article_repository)):
    row = repo.get_article(article_id)
    if not row:
        raise HTTPException(status_code=404, detail="Article not found")
    return row


@app.get("/authors", response_model=list[Author], tags=["authors"], dependencies=[Depends(require_api_key)])
def list_authors(
    limit: int = Query(20, ge=1, le=200),
    offset: int = Query(0, ge=0),
    repo: AuthorRepository = Depends(get_author_repository),
):
    return repo.list_authors(limit=limit, offset=offset)


@app.get("/authors/{author_id}", response_model=Author, tags=["authors"], dependencies=[Depends(require_api_key)])
def get_author(author_id: int, repo: AuthorRepository = Depends(get_author_repository)):
    row = repo.get_author(author_id)
    if not row:
        raise HTTPException(status_code=404, detail="Author not found")
    return row


@app.get(
    "/dq-scorecard/latest",
    response_model=DQScorecard,
    tags=["data-quality"],
    dependencies=[Depends(require_api_key)],
)
def latest_dq_scorecard(repo: DQRepository = Depends(get_dq_repository)):
    row = repo.latest()
    if not row:
        raise HTTPException(status_code=404, detail="No DQ scorecard runs yet")
    return row


@app.get(
    "/dq-scorecard/history",
    response_model=list[DQScorecard],
    tags=["data-quality"],
    dependencies=[Depends(require_api_key)],
)
def dq_scorecard_history(
    limit: int = Query(10, ge=1, le=100),
    repo: DQRepository = Depends(get_dq_repository),
):
    return repo.history(limit=limit)
