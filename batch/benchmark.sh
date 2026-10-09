#!/usr/bin/env bash
# Compare les scénarios d'optimisation du job batch (partie 3).
#
#   batch/benchmark.sh [répétitions]      (défaut : 3)
#
# Chaque scénario est lancé RUNS fois, en alternance (baseline, cache, repartition, optimized,
# baseline, ...) pour que les variations de charge de la machine touchent tous les scénarios.
# Toutes les exécutions analysent les mêmes toots : ceux ingérés avant le lancement du script.
# Résultats : table batch_benchmarks (médiane affichée à la fin) et http://localhost:18080.
set -euo pipefail
cd "$(dirname "$0")/.."

RUNS=${1:-3}
UNTIL=$(date -u +%Y-%m-%dT%H:%M:%S)

for i in $(seq 1 "$RUNS"); do
  for scenario in baseline cache repartition optimized; do
    echo "=== Exécution $i/$RUNS : $scenario"
    docker compose exec -T -e BATCH_UNTIL="$UNTIL" jupyter \
      spark-submit /home/jovyan/work/batch/batch_job.py --scenario "$scenario" 2>&1 \
      | grep "batch_job -" || true
  done
done

docker compose exec -T postgres psql -U mastodon -d mastodon -c "
SELECT scenario, count(*) AS runs, max(toot_count) AS toots,
       round(percentile_cont(0.5) WITHIN GROUP (ORDER BY load_s)::numeric, 1)    AS load_s,
       round(percentile_cont(0.5) WITHIN GROUP (ORDER BY total_s - load_s)::numeric, 1) AS calculs_s,
       round(percentile_cont(0.5) WITHIN GROUP (ORDER BY total_s)::numeric, 1)   AS total_s
FROM batch_benchmarks
WHERE data_until = '$UNTIL'
GROUP BY scenario
ORDER BY array_position(ARRAY['baseline', 'cache', 'repartition', 'optimized'], scenario);"
