-- Requêtes de démonstration : voir les données du streaming arriver en temps réel.
--
--   docker compose exec postgres psql -U mastodon -d mastodon -f /dev/stdin < sql/demo.sql
-- ou, en boucle toutes les 5 s, dans psql :
--   \i sql/demo.sql   puis   \watch 5

-- 1. Derniers toots arrivés (couche brute)
SELECT ingested_at::time(0) AS recu_a, created_at::time(0) AS publie_a, language,
       left(username, 25) AS auteur, left(content, 60) AS debut
FROM toots ORDER BY ingested_at DESC LIMIT 5;

-- 2. Action 1 : toots par minute sur les 15 dernières minutes
SELECT window_start::time(0) AS minute, toot_count, updated_at::time(0) AS maj
FROM stream_window_counts
WHERE granularity = '1 minute' AND window_start > now() - interval '15 minutes'
ORDER BY window_start DESC;

-- 3. Action 2 : hashtags les plus actifs de l'heure en cours
SELECT hashtag, toot_count, round(avg_length) AS longueur_moy
FROM stream_hashtag_stats
WHERE window_start = date_trunc('hour', now())
ORDER BY toot_count DESC LIMIT 10;

-- 4. Volumes globaux
SELECT (SELECT count(*) FROM toots) AS toots,
       (SELECT count(DISTINCT user_id) FROM toots) AS utilisateurs,
       (SELECT count(DISTINCT hashtag) FROM toot_hashtags) AS hashtags,
       (SELECT max(ingested_at)::time(0) FROM toots) AS dernier_toot;
