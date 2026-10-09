"""Producer Mastodon -> Kafka.

Récupère les toots contenant les hashtags suivis et les publie en JSON dans le topic Kafka
(clé = toot_id). Deux modes de collecte :

- stream : streaming Mastodon par WebSocket (temps réel, nécessite MASTODON_ACCESS_TOKEN).
           Une seule connexion pour tous les hashtags ; après chaque (re)connexion, un passage
           de rattrapage par l'API REST récupère les toots publiés pendant la coupure ;
- poll   : interrogation périodique de /timelines/tag (fonctionne sans token).

Pourquoi pas `Mastodon.stream_hashtag()` ? Il utilise le streaming HTTP (Server-Sent Events),
que mastodon.social coupe au bout de ~16 s : on perdait des toots à chaque reconnexion.

`--backfill-pages N` récupère en plus, au démarrage, les N dernières pages (40 toots chacune)
de chaque hashtag : utile pour constituer rapidement des données historiques (partie 3).

Les limites de l'API (300 requêtes / 5 min) sont respectées automatiquement par Mastodon.py
(ratelimit_method="pace").

Exemples :
    uv run producer/mastodon_producer.py                          # mode auto
    uv run producer/mastodon_producer.py --mode poll --backfill-pages 5
    uv run producer/mastodon_producer.py --duration 60            # s'arrête après 60 s
"""

import argparse
import asyncio
import json
import logging
import os
import signal
import threading
import time
from collections import OrderedDict

import requests
import websockets
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError
from mastodon import Mastodon, MastodonError
from toot_parser import parse_status

log = logging.getLogger("producer")

PAGE_SIZE = 40  # maximum autorisé par l'API pour /timelines/tag
USER_AGENT = "mastodon-spark-student-project/0.1"


class RecentIds:
    """Ensemble borné des derniers toot_id envoyés.

    Un même toot peut remonter par plusieurs hashtags suivis ou par le backfill puis le
    polling : on évite de l'envoyer deux fois.
    """

    def __init__(self, max_size: int = 50_000):
        self._ids: OrderedDict[str, None] = OrderedDict()
        self._max_size = max_size
        self._lock = threading.Lock()

    def add(self, toot_id: str) -> bool:
        """Ajoute l'id ; renvoie False s'il était déjà présent."""
        with self._lock:
            if toot_id in self._ids:
                return False
            self._ids[toot_id] = None
            if len(self._ids) > self._max_size:
                self._ids.popitem(last=False)
            return True


class TootSink:
    """Parse les toots et les publie dans Kafka, sans doublon."""

    def __init__(self, producer: KafkaProducer, topic: str):
        self.producer = producer
        self.topic = topic
        self.seen = RecentIds()
        self.sent = 0

    def send(self, status, source_tag: str) -> bool:
        toot = parse_status(status, source_tag)
        if toot is None or not self.seen.add(toot["toot_id"]):
            return False
        self.producer.send(
            self.topic,
            key=toot["toot_id"].encode("utf-8"),
            value=json.dumps(toot, ensure_ascii=False).encode("utf-8"),
        )
        self.sent += 1
        log.debug("@%s [%s] %s", toot["username"], source_tag, toot["content"][:80])
        return True


def create_kafka_producer(bootstrap_servers: str, retries: int = 5) -> KafkaProducer:
    """Connexion à Kafka, avec quelques tentatives (le broker peut démarrer après le producer)."""
    for attempt in range(1, retries + 1):
        try:
            return KafkaProducer(
                bootstrap_servers=bootstrap_servers.split(","),
                acks="all",      # message confirmé par le broker avant d'être considéré envoyé
                linger_ms=100,    # regroupe les envois pour limiter les allers-retours
                retries=5,
            )
        except KafkaTimeoutError:  # levée après ~30 s si aucun broker ne répond
            log.warning("Kafka indisponible sur %s (tentative %d/%d)", bootstrap_servers, attempt, retries)
            time.sleep(5)
    raise SystemExit(f"Impossible de joindre Kafka sur {bootstrap_servers}")


def _max_id(statuses) -> int:
    return max(int(s["id"]) for s in statuses)


def backfill(api: Mastodon, sink: TootSink, tags: list[str], pages: int, stop: threading.Event) -> dict[str, int | None]:
    """Remonte l'historique de chaque hashtag (pagination vers le passé avec max_id).

    Renvoie, pour chaque hashtag, l'id du toot le plus récent vu : point de départ du polling.
    """
    newest: dict[str, int | None] = {tag: None for tag in tags}
    for tag in tags:
        max_id, count = None, 0
        for _ in range(pages):
            if stop.is_set():
                break
            try:
                page = api.timeline_hashtag(tag, max_id=max_id, limit=PAGE_SIZE)
            except MastodonError as exc:
                log.error("Backfill #%s interrompu : %s", tag, exc)
                break
            if not page:
                break
            if newest[tag] is None:
                newest[tag] = _max_id(page)
            count += sum(sink.send(s, tag) for s in page)
            max_id = min(int(s["id"]) for s in page) - 1
        log.info("Backfill #%s : %d toots", tag, count)
    sink.producer.flush()
    return newest


def fetch_new(api: Mastodon, tag: str, last_id: int | None, max_pages: int = 10) -> tuple[list, int | None]:
    """Toots publiés après last_id, du plus ancien au plus récent.

    `min_id` pagine vers l'avant : contrairement à `since_id`, on ne perd pas de toots
    quand plus de 40 sont publiés entre deux passages.
    """
    new = []
    for _ in range(max_pages):
        page = api.timeline_hashtag(tag, min_id=last_id, limit=PAGE_SIZE)
        if not page:
            break
        new.extend(page)
        last_id = _max_id(page)
        if len(page) < PAGE_SIZE:
            break
    return sorted(new, key=lambda s: int(s["id"])), last_id


def poll_once(api, sink, tags, last_ids) -> int:
    """Un passage sur chaque hashtag : envoie les toots postérieurs à last_ids[tag]."""
    total = 0
    for tag in tags:
        try:
            statuses, last_ids[tag] = fetch_new(api, tag, last_ids.get(tag))
        except MastodonError as exc:
            log.error("Polling #%s : %s", tag, exc)
            continue
        sent = sum(sink.send(s, tag) for s in statuses)
        if sent:
            log.info("#%s : +%d toots", tag, sent)
        total += sent
    sink.producer.flush()
    return total


def run_polling(api, sink, tags, interval, last_ids, stop: threading.Event):
    log.info("Mode polling : %s, toutes les %d s", ", ".join(f"#{t}" for t in tags), interval)
    while not stop.is_set():
        poll_once(api, sink, tags, last_ids)
        log.info("Total envoyé : %d", sink.sent)
        stop.wait(interval)


def streaming_url(api_base_url: str) -> str:
    """URL WebSocket du streaming, annoncée par l'instance (souvent un sous-domaine dédié)."""
    info = requests.get(f"{api_base_url}/api/v1/instance", timeout=10, headers={"User-Agent": USER_AGENT})
    info.raise_for_status()
    return info.json()["urls"]["streaming_api"].rstrip("/") + "/api/v1/streaming"


async def stream_websocket(api, sink, tags, last_ids, url, token, stop: threading.Event):
    """Boucle de streaming : connexion, abonnements, rattrapage, réception ; reconnexion si coupure."""
    backoff = 5
    last_report = time.monotonic()
    while not stop.is_set():
        try:
            async with websockets.connect(
                url, additional_headers={"Authorization": f"Bearer {token}"}, user_agent_header=USER_AGENT
            ) as ws:
                # Le protocole WebSocket de Mastodon permet plusieurs abonnements sur une connexion.
                for tag in tags:
                    await ws.send(json.dumps({"type": "subscribe", "stream": "hashtag", "tag": tag}))
                log.info("WebSocket connecté (%s), abonné à %s", url, ", ".join(f"#{t}" for t in tags))

                # Rattrapage des toots publiés avant l'abonnement (démarrage ou coupure).
                caught_up = await asyncio.to_thread(poll_once, api, sink, tags, last_ids)
                log.info("Rattrapage REST : %d toots", caught_up)
                backoff = 5

                while not stop.is_set():
                    if time.monotonic() - last_report > 60:
                        sink.producer.flush()
                        log.info("Total envoyé : %d", sink.sent)
                        last_report = time.monotonic()
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=1)
                    except TimeoutError:
                        continue  # permet de vérifier régulièrement la demande d'arrêt
                    handle_stream_event(json.loads(raw), sink, last_ids)
        except (OSError, websockets.exceptions.WebSocketException) as exc:
            if stop.is_set():
                break
            log.warning("WebSocket interrompu (%s), reconnexion dans %d s", exc, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 300)


def handle_stream_event(message: dict, sink: TootSink, last_ids: dict) -> None:
    """Message reçu : {"stream": ["hashtag", "ai"], "event": "update", "payload": "<toot JSON>"}."""
    if message.get("event") != "update":
        return  # delete, status.update (édition)... non traités
    stream = message.get("stream") or []
    tag = stream[1] if len(stream) > 1 else None
    status = json.loads(message["payload"])
    sink.send(status, tag)
    if tag:
        last_ids[tag] = max(last_ids.get(tag) or 0, int(status["id"]))


def run_streaming(api, sink, tags, last_ids, token, stop: threading.Event):
    url = streaming_url(api.api_base_url)
    log.info("Mode streaming : %s", ", ".join(f"#{t}" for t in tags))
    asyncio.run(stream_websocket(api, sink, tags, last_ids, url, token, stop))


def parse_args() -> argparse.Namespace:
    env = os.environ.get
    parser = argparse.ArgumentParser(description="Collecte des toots Mastodon vers Kafka")
    parser.add_argument("--mode", choices=["auto", "stream", "poll"], default=env("PRODUCER_MODE", "auto"),
                        help="auto = stream si un access token est configuré, sinon poll")
    parser.add_argument("--hashtags", default=env("MASTODON_HASHTAGS", "datascience,ai"),
                        help="hashtags suivis, séparés par des virgules (sans #)")
    parser.add_argument("--interval", type=int, default=int(env("PRODUCER_POLL_INTERVAL", "30")),
                        help="secondes entre deux passages en mode poll")
    parser.add_argument("--backfill-pages", type=int, default=int(env("PRODUCER_BACKFILL_PAGES", "0")),
                        help="pages d'historique (40 toots) à récupérer par hashtag au démarrage")
    parser.add_argument("--duration", type=int, default=0, help="arrêt automatique après N secondes (0 = jamais)")
    parser.add_argument("--log-level", default=env("LOG_LEVEL", "INFO"))
    return parser.parse_args()


def main():
    load_dotenv()  # .env à la racine du projet ; les variables déjà définies (Docker) sont prioritaires
    args = parse_args()
    logging.basicConfig(level=args.log_level.upper(), format="%(asctime)s %(levelname)s %(name)s - %(message)s")

    tags = [t.strip().lstrip("#").lower() for t in args.hashtags.split(",") if t.strip()]
    token = os.environ.get("MASTODON_ACCESS_TOKEN") or None
    mode = args.mode if args.mode != "auto" else ("stream" if token else "poll")
    if mode == "stream" and not token:
        raise SystemExit("Le mode stream nécessite MASTODON_ACCESS_TOKEN (voir .env.example)")

    api = Mastodon(
        api_base_url=os.environ.get("MASTODON_API_BASE_URL", "https://mastodon.social"),
        access_token=token,
        ratelimit_method="pace",  # espace les requêtes pour ne jamais dépasser la limite
        request_timeout=30,
        user_agent=USER_AGENT,
    )
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9094")
    topic = os.environ.get("KAFKA_TOPIC", "mastodon_stream")
    sink = TootSink(create_kafka_producer(bootstrap), topic)
    log.info("Connecté à Kafka %s, topic %s", bootstrap, topic)

    # Arrêt propre (Ctrl+C, docker stop, --duration) : on vide le buffer Kafka avant de quitter.
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    if args.duration:
        timer = threading.Timer(args.duration, stop.set)
        timer.daemon = True
        timer.start()

    last_ids = backfill(api, sink, tags, args.backfill_pages, stop) if args.backfill_pages else {}
    try:
        if mode == "stream":
            run_streaming(api, sink, tags, last_ids, token, stop)
        else:
            run_polling(api, sink, tags, args.interval, last_ids, stop)
    finally:
        sink.producer.flush()
        sink.producer.close()
        log.info("Arrêt. %d toots envoyés dans %s", sink.sent, topic)


if __name__ == "__main__":
    main()
