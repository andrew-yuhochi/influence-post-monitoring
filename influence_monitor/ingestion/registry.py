"""Source registry — maps config keys to SocialMediaSource implementations.

The pipeline orchestrator selects the active source via
``settings.twitter_source`` (default: ``"twitter_twikit"``).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from influence_monitor.ingestion.base import SocialMediaSource
from influence_monitor.ingestion.twitter_twikit import TwitterTwikitSource, TwitterIngestor  # noqa: F401

if TYPE_CHECKING:
    pass

# Stubs are imported lazily to avoid import errors from uninstalled
# optional dependencies (tweepy, etc.).  They raise NotImplementedError
# on instantiation.
from influence_monitor.ingestion.twitter_official import TwitterOfficialIngestor  # noqa: E402
from influence_monitor.ingestion.substack import SubstackIngestor  # noqa: E402
from influence_monitor.ingestion.congressional import CongressionalIngestor  # noqa: E402

SOURCE_REGISTRY: dict[str, type[SocialMediaSource]] = {
    "twitter_twikit": TwitterTwikitSource,
    "twitter_official": TwitterOfficialIngestor,
    "substack": SubstackIngestor,
    "congressional": CongressionalIngestor,
}
