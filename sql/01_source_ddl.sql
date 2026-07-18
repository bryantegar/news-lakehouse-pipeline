-- ============================================================
-- SOURCE DB (OLTP) — simulates the upstream news portal database
-- ============================================================

CREATE TABLE IF NOT EXISTS articles (
    id              BIGINT PRIMARY KEY,   -- external ID from source (e.g. kumparan story id), not auto-generated
    title           TEXT NOT NULL,
    content         TEXT,
    author_id       BIGINT,
    author_name     TEXT,
    category        TEXT,
    published_at    TIMESTAMP,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    deleted_at      TIMESTAMP
);

-- Table that logs hard-deleted article IDs (fired by trigger below).
-- Hourly ETL is watermark-based on updated_at, which a hard DELETE never
-- touches — this table is how the daily reconciliation DAG finds them.
CREATE TABLE IF NOT EXISTS article_deleted (
    article_id      BIGINT PRIMARY KEY,
    deleted_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE OR REPLACE FUNCTION trg_article_hard_delete_fn()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO article_deleted (article_id, deleted_at)
    VALUES (OLD.id, NOW())
    ON CONFLICT (article_id) DO NOTHING;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_article_hard_delete ON articles;
CREATE TRIGGER trg_article_hard_delete
    AFTER DELETE ON articles
    FOR EACH ROW EXECUTE FUNCTION trg_article_hard_delete_fn();

CREATE INDEX IF NOT EXISTS idx_articles_updated_at ON articles (updated_at);
