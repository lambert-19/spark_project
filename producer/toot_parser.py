"""Conversion d'un statut Mastodon (objet renvoyé par Mastodon.py) en message plat pour Kafka.

Le format produit est celui décrit dans CAHIER_DES_CHARGES.md (EF1.4) ; le job Spark
Structured Streaming le relit avec un schéma explicite.
"""

import re
from datetime import UTC, datetime

from bs4 import BeautifulSoup

_WHITESPACE = re.compile(r"\s+")


def html_to_text(html: str) -> str:
    """Le contenu d'un toot est du HTML (<p>, <br>, liens, mentions) : on ne garde que le texte."""
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    # Séparateurs uniquement entre paragraphes et lignes : un séparateur global couperait
    # les hashtags (<a>#<span>AI</span></a> -> "# AI") et les URL découpées en <span>.
    for br in soup.find_all("br"):
        br.replace_with("\n")
    for paragraph in soup.find_all("p"):
        paragraph.append("\n")
    return _WHITESPACE.sub(" ", soup.get_text()).strip()


def to_iso_utc(value) -> str | None:
    """Datetime -> chaîne ISO 8601 en UTC (format lu par Spark avec to_timestamp)."""
    if value is None:
        return None
    if isinstance(value, str):  # payload brut du WebSocket : "2026-10-09T18:38:19.000Z"
        value = datetime.fromisoformat(value)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    return str(value)


def parse_status(status, source_tag: str | None = None) -> dict | None:
    """Extrait les champs utiles d'un toot. Renvoie None si le toot n'a pas de texte.

    `source_tag` est le hashtag suivi qui a permis de récupérer le toot.
    """
    # Un boost (reblog) n'est pas un nouveau contenu : on garde le toot d'origine.
    if status.get("reblog"):
        status = status["reblog"]

    content = html_to_text(status.get("content") or "")
    if not content:
        return None

    account = status.get("account") or {}
    return {
        "toot_id": str(status["id"]),
        "created_at": to_iso_utc(status.get("created_at")),
        "user_id": str(account.get("id")),
        "username": account.get("acct"),
        "language": status.get("language"),
        "content": content,
        "hashtags": sorted({tag["name"].lower() for tag in status.get("tags") or []}),
        "favourites_count": status.get("favourites_count") or 0,
        "reblogs_count": status.get("reblogs_count") or 0,
        "replies_count": status.get("replies_count") or 0,
        "url": status.get("url"),
        # Mastodon ne fournit pas de géolocalisation : champ conservé pour coller à l'énoncé.
        "location": None,
        "source_tag": source_tag,
        "ingested_at": to_iso_utc(datetime.now(UTC)),
    }
