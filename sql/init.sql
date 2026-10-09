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
-- Recalculés entièrement à chaque exécution de batch/batch_job.py (tables vidées puis remplies).

CREATE TABLE IF NOT EXISTS batch_daily_stats (
    day             DATE PRIMARY KEY,
    toot_count      BIGINT NOT NULL,
    avg_length      DOUBLE PRECISION,
    avg_engagement  DOUBLE PRECISION,     -- favourites + reblogs
    top_hashtag     TEXT,
    computed_at     TIMESTAMPTZ DEFAULT now()
);

-- Nombre de toots par jour et par hashtag ; rank = 1 pour le hashtag le plus fréquent du jour.
CREATE TABLE IF NOT EXISTS batch_hashtag_daily (
    day          DATE NOT NULL,
    hashtag      TEXT NOT NULL,
    toot_count   BIGINT NOT NULL,
    rank         INTEGER NOT NULL,
    computed_at  TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (day, hashtag)
);

-- Totaux par hashtag sur tout l'historique (top hashtags global).
CREATE TABLE IF NOT EXISTS batch_hashtag_totals (
    hashtag         TEXT PRIMARY KEY,
    toot_count      BIGINT NOT NULL,
    user_count      BIGINT NOT NULL,
    avg_length      DOUBLE PRECISION,
    avg_engagement  DOUBLE PRECISION,
    computed_at     TIMESTAMPTZ DEFAULT now()
);

-- Utilisateurs actifs : plus de BATCH_ACTIVE_THRESHOLD toots dans l'historique.
CREATE TABLE IF NOT EXISTS batch_active_users (
    user_id         TEXT PRIMARY KEY,
    username        TEXT,
    toot_count      BIGINT NOT NULL,
    avg_length      DOUBLE PRECISION,
    avg_engagement  DOUBLE PRECISION,
    first_toot      TIMESTAMPTZ,
    last_toot       TIMESTAMPTZ,
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

-- Une ligne par classifieur et par entraînement (ml/train_sentiment.py), sur le jeu de test
-- Sentiment140 (20 %). selected = modèle sauvegardé ; les colonnes manual_* et neutral_margin
-- (jeu annoté à la main, avec des tweets neutres) ne concernent que celui-ci.
CREATE TABLE IF NOT EXISTS sentiment_model_metrics (
    run_id                  BIGSERIAL PRIMARY KEY,
    trained_at              TIMESTAMPTZ DEFAULT now(),
    model_name              TEXT NOT NULL,
    selected                BOOLEAN NOT NULL,
    model_path              TEXT,
    train_count             BIGINT,
    test_count              BIGINT,
    accuracy                DOUBLE PRECISION,
    f1                      DOUBLE PRECISION,
    auc                     DOUBLE PRECISION,
    tn                      BIGINT,                 -- matrice de confusion (positif = 1)
    fp                      BIGINT,
    fn                      BIGINT,
    tp                      BIGINT,
    train_s                 DOUBLE PRECISION,
    manual_accuracy         DOUBLE PRECISION,       -- jeu manuel, positif/négatif seulement
    neutral_margin          DOUBLE PRECISION,       -- neutre si |p(positif) - 0.5| <= marge
    manual_3class_accuracy  DOUBLE PRECISION
);

-- ============================================================ Mesures d'optimisation (batch)

-- Une ligne par exécution de batch/batch_job.py : durée de chaque étape selon le scénario
-- (baseline, cache, repartition, optimized). app_id permet de retrouver l'exécution dans le
-- Spark History Server (http://localhost:18080).
CREATE TABLE IF NOT EXISTS batch_benchmarks (
    run_id            BIGSERIAL PRIMARY KEY,
    run_at            TIMESTAMPTZ DEFAULT now(),
    scenario          TEXT NOT NULL,
    app_id            TEXT,
    data_until        TIMESTAMPTZ,            -- BATCH_UNTIL : volume figé pendant les mesures
    toot_count        BIGINT,
    load_s            DOUBLE PRECISION,       -- lecture JDBC (+ matérialisation du cache)
    active_users_s    DOUBLE PRECISION,
    hashtag_daily_s   DOUBLE PRECISION,
    hashtag_totals_s  DOUBLE PRECISION,
    daily_stats_s     DOUBLE PRECISION,
    total_s           DOUBLE PRECISION
);
