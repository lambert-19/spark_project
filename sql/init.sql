-- Schéma PostgreSQL du projet (cf. CAHIER_DES_CHARGES.md §6).
-- Exécuté automatiquement par l'image postgres au premier démarrage.

-- ============================================================ Données brutes (streaming)

-- Un toot par ligne, tel que reçu de Kafka après nettoyage.
CREATE TABLE IF NOT EXISTS toots (
    toot_id           TEXT PRIMARY KEY,
    created_at        TIMESTAMPTZ NOT NULL,
    user_id           TEXT NOT NULL,
    username          TEXT,
    language          TEXT,
    content           TEXT NOT NULL,
    content_length    INTEGER NOT NULL,
    favourites_count  INTEGER DEFAULT 0,
    reblogs_count     INTEGER DEFAULT 0,
    replies_count     INTEGER DEFAULT 0,
    source_tag        TEXT,                 -- hashtag suivi qui a remonté le toot
    ingested_at       TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_toots_created_at ON toots (created_at);
CREATE INDEX IF NOT EXISTS idx_toots_user_id    ON toots (user_id);

-- Relation toot <-> hashtag (un toot peut avoir plusieurs hashtags).
CREATE TABLE IF NOT EXISTS toot_hashtags (
    toot_id  TEXT NOT NULL REFERENCES toots (toot_id) ON DELETE CASCADE,
    hashtag  TEXT NOT NULL,
    PRIMARY KEY (toot_id, hashtag)
);
CREATE INDEX IF NOT EXISTS idx_toot_hashtags_hashtag ON toot_hashtags (hashtag);

-- ============================================================ Agrégats du streaming

-- Action 1 : nombre de toots par fenêtre temporelle.
CREATE TABLE IF NOT EXISTS stream_window_counts (
    window_start  TIMESTAMPTZ NOT NULL,
    window_end    TIMESTAMPTZ NOT NULL,
    granularity   TEXT NOT NULL,          -- '1 minute', '1 hour', ...
    toot_count    BIGINT NOT NULL,
    updated_at    TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (window_start, granularity)
);

-- Action 2 : statistiques par utilisateur et par fenêtre.
CREATE TABLE IF NOT EXISTS stream_user_stats (
    window_start  TIMESTAMPTZ NOT NULL,
    window_end    TIMESTAMPTZ NOT NULL,
    user_id       TEXT NOT NULL,
    username      TEXT,
    toot_count    BIGINT NOT NULL,
    avg_length    DOUBLE PRECISION,
    updated_at    TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (window_start, user_id)
);

-- Action 2 : statistiques par hashtag et par fenêtre.
CREATE TABLE IF NOT EXISTS stream_hashtag_stats (
    window_start  TIMESTAMPTZ NOT NULL,
    window_end    TIMESTAMPTZ NOT NULL,
    hashtag       TEXT NOT NULL,
    toot_count    BIGINT NOT NULL,
    avg_length    DOUBLE PRECISION,
    updated_at    TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (window_start, hashtag)
);

-- ============================================================ Résultats batch

CREATE TABLE IF NOT EXISTS batch_daily_stats (
    day             DATE PRIMARY KEY,
    toot_count      BIGINT NOT NULL,
    avg_length      DOUBLE PRECISION,
    avg_engagement  DOUBLE PRECISION,     -- favourites + reblogs
    top_hashtag     TEXT,
    computed_at     TIMESTAMPTZ DEFAULT now()
);

-- ============================================================ Sentiment (MLlib)

CREATE TABLE IF NOT EXISTS toot_sentiment (
    toot_id      TEXT PRIMARY KEY REFERENCES toots (toot_id) ON DELETE CASCADE,
    content      TEXT,
    sentiment    TEXT NOT NULL CHECK (sentiment IN ('positive', 'negative', 'neutral')),
    probability  DOUBLE PRECISION,        -- probabilité de la classe positive
    model_name   TEXT,
    scored_at    TIMESTAMPTZ DEFAULT now()
);
