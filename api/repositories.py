"""
repositories.py — data access layer, deliberately separated from
routes.py (which only knows about the ArticleRepository interface, never
about SQL or psycopg2 directly).

This is the SOLID angle tiket.com's JD calls out explicitly:
  - Single Responsibility: each repository only knows how to fetch its
    own entity; routes only know how to turn a request into a call.
  - Open/Closed: swapping Postgres for BigQuery later (see docs/
    architecture.md) means adding a BigQueryArticleRepository and
    changing one line in dependencies.py — routes.py doesn't change.
  - Dependency Inversion: routes.py depends on the ArticleRepository
    *abstraction*, not on PostgresArticleRepository directly.
"""
from abc import ABC, abstractmethod
from typing import Optional

from db import get_cursor


class ArticleRepository(ABC):
    @abstractmethod
    def list_articles(self, limit: int, offset: int, category: Optional[str]) -> tuple[list[dict], int]:
        ...

    @abstractmethod
    def get_article(self, article_id: int) -> Optional[dict]:
        ...


class AuthorRepository(ABC):
    @abstractmethod
    def list_authors(self, limit: int, offset: int) -> list[dict]:
        ...

    @abstractmethod
    def get_author(self, author_id: int) -> Optional[dict]:
        ...


class DQRepository(ABC):
    @abstractmethod
    def latest(self) -> Optional[dict]:
        ...

    @abstractmethod
    def history(self, limit: int) -> list[dict]:
        ...


class PostgresArticleRepository(ArticleRepository):
    def list_articles(self, limit: int, offset: int, category: Optional[str]) -> tuple[list[dict], int]:
        with get_cursor() as cur:
            if category:
                cur.execute(
                    "SELECT count(*) AS c FROM news_mart.fct_articles WHERE category = %s",
                    (category,),
                )
                total = cur.fetchone()["c"]
                cur.execute(
                    """
                    SELECT article_id, title, category, author_id, date_day, content_length
                    FROM news_mart.fct_articles
                    WHERE category = %s
                    ORDER BY date_day DESC, article_id
                    LIMIT %s OFFSET %s
                    """,
                    (category, limit, offset),
                )
            else:
                cur.execute("SELECT count(*) AS c FROM news_mart.fct_articles")
                total = cur.fetchone()["c"]
                cur.execute(
                    """
                    SELECT article_id, title, category, author_id, date_day, content_length
                    FROM news_mart.fct_articles
                    ORDER BY date_day DESC, article_id
                    LIMIT %s OFFSET %s
                    """,
                    (limit, offset),
                )
            return cur.fetchall(), total

    def get_article(self, article_id: int) -> Optional[dict]:
        with get_cursor() as cur:
            cur.execute(
                """
                SELECT article_id, title, category, author_id, date_day, content_length
                FROM news_mart.fct_articles
                WHERE article_id = %s
                """,
                (article_id,),
            )
            return cur.fetchone()


class PostgresAuthorRepository(AuthorRepository):
    def list_authors(self, limit: int, offset: int) -> list[dict]:
        with get_cursor() as cur:
            cur.execute(
                """
                SELECT author_id, author_name, total_articles,
                       first_published_date, last_published_date
                FROM news_mart.dim_author
                ORDER BY total_articles DESC
                LIMIT %s OFFSET %s
                """,
                (limit, offset),
            )
            return cur.fetchall()

    def get_author(self, author_id: int) -> Optional[dict]:
        with get_cursor() as cur:
            cur.execute(
                """
                SELECT author_id, author_name, total_articles,
                       first_published_date, last_published_date
                FROM news_mart.dim_author
                WHERE author_id = %s
                """,
                (author_id,),
            )
            return cur.fetchone()


class PostgresDQRepository(DQRepository):
    def latest(self) -> Optional[dict]:
        with get_cursor() as cur:
            cur.execute(
                "SELECT * FROM news_mart.mart_dq_scorecard ORDER BY run_at DESC LIMIT 1"
            )
            return cur.fetchone()

    def history(self, limit: int) -> list[dict]:
        with get_cursor() as cur:
            cur.execute(
                "SELECT * FROM news_mart.mart_dq_scorecard ORDER BY run_at DESC LIMIT %s",
                (limit,),
            )
            return cur.fetchall()
