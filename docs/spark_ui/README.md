# Captures Spark UI — optimisation du job batch

Captures du Spark History Server (http://localhost:18080) pour deux exécutions de
[batch/batch_job.py](../../batch/batch_job.py) du 10/10/2026 sur les mêmes toots (~15 500) :

| | `baseline` | `optimized` (`cache` + `repartition`/`coalesce`) |
|---|---|---|
| Application | `app-20261010154136-0008` | `app-20261010154653-0011` |
| Jobs / stages | 33 / 33 | 36 / 36 (+ 2 jobs `count` qui remplissent le cache) |
| Données de shuffle écrites | 14,0 Mo | **7,6 Mo** |
| Lecture des toots | relue dans PostgreSQL (JDBC) par chacune des 4 sorties | une seule lecture JDBC, puis 8,5 Mo lus **depuis le cache** |
| Jointure finale de la sortie `active_users` (370 utilisateurs) | `SortMergeJoin` (tri + shuffle des deux côtés) | `BroadcastHashJoin` (pas de shuffle) |
| Écriture des résultats | partitions telles quelles | `Coalesce` (1 partition → 1 connexion JDBC) |

| Fichier | Contenu | À montrer |
|---|---|---|
| [1_baseline_jobs.png](1_baseline_jobs.png) | Liste des jobs, `baseline` | Chaque sortie relance une lecture JDBC (jobs `jdbc at …`) |
| [2_optimized_jobs.png](2_optimized_jobs.png) | Liste des jobs, `optimized` | Les 2 premiers jobs `count` matérialisent le cache |
| [3_baseline_stages.png](3_baseline_stages.png) | Stages, `baseline` | Volumes de shuffle |
| [4_optimized_stages.png](4_optimized_stages.png) | Stages, `optimized` | Shuffle divisé par ~2, colonne *Input* = lecture du cache |
| [5_baseline_sql_dag.png](5_baseline_sql_dag.png) | Plan SQL de la sortie `active_users`, `baseline` | `Scan JDBCRelation` + `SortMergeJoin` |
| [6_optimized_sql_dag.png](6_optimized_sql_dag.png) | Plan SQL de la même sortie, `optimized` | `InMemoryTableScan` + `BroadcastHashJoin` + `Coalesce` |

## Temps d'exécution

Mesures de référence (09/10/2026, 12 079 toots, médiane de 3 exécutions, machine au repos), cf.
[README](../../README.md#optimisation) : **29,0 s → 20,8 s** au total, temps de calcul hors lecture
**27,0 s → 13,4 s**.

Les exécutions capturées ici (une par scénario) ont tourné pendant que le job de streaming
rattrapait une pause de 17 minutes et occupait les 2 autres cœurs du worker : leurs durées
(59 à 74 s, sans ordre net entre scénarios) ne sont **pas** représentatives. Pour comparer des
temps, relancer `batch/benchmark.sh 3` avec le streaming à l'arrêt
(`docker compose stop stream-job`) ; les captures, elles, montrent la structure des plans, qui ne
dépend pas de la charge.

## Remarques

- **Pas de stage à 200 tâches**, même en `baseline` : l'Adaptive Query Execution de Spark
  (activée par défaut) regroupe les petites partitions de shuffle après coup (`AQEShuffleRead`
  dans les plans). Fixer `spark.sql.shuffle.partitions = 4` évite surtout de planifier puis
  fusionner 200 partitions.
- **Onglet Storage vide** dans le History Server : le job libère son cache (`unpersist`) avant de
  se terminer. Le cache est visible dans les plans (`InMemoryTableScan`) et, pendant l'exécution,
  dans l'onglet Storage du Spark UI en direct (http://localhost:4040).
- Captures prises avec un navigateur headless (1600 px de large) ; les plans SQL sont en pleine
  hauteur.
