"""
dependencies.py — the one place that decides which concrete repository
implementation routes.py gets. Swapping the DWH from Postgres to
BigQuery later means adding BigQueryArticleRepository in repositories.py
and changing the three lines below — nothing in routes.py changes.
"""
from repositories import (
    ArticleRepository, AuthorRepository, DQRepository,
    PostgresArticleRepository, PostgresAuthorRepository, PostgresDQRepository,
)

_article_repo = PostgresArticleRepository()
_author_repo = PostgresAuthorRepository()
_dq_repo = PostgresDQRepository()


def get_article_repository() -> ArticleRepository:
    return _article_repo


def get_author_repository() -> AuthorRepository:
    return _author_repo


def get_dq_repository() -> DQRepository:
    return _dq_repo
