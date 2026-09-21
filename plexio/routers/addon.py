import asyncio
import logging
from itertools import chain
from time import perf_counter
from typing import Annotated

from aiohttp import ClientSession
from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from redis.asyncio.client import Redis
from yarl import URL

from plexio import __version__
from plexio.dependencies import (
    get_addon_configuration,
    get_cache,
    get_http_client,
    set_sentry_user,
)
from plexio.models import PLEX_TO_STREMIO_MEDIA_TYPE, STREMIO_TO_PLEX_MEDIA_TYPE
from plexio.models.addon import AddonConfiguration
from plexio.models.plex import PlexMediaMeta
from plexio.models.stremio import (
    StremioCatalog,
    StremioCatalogManifest,
    StremioManifest,
    StremioMediaType,
    StremioMetaResponse,
    StremioStreamsResponse,
)
from plexio.models.utils import (
    is_rating_key_plexio_id,
    plexio_id_to_guid,
    plexio_id_to_rating_key,
)
from plexio.plex.media_server_api import (
    SORT_OPTIONS,
    get_all_episodes,
    get_collection_media,
    get_media,
    get_media_by_rating_key,
    get_next_up_episode,
    get_on_deck,
    get_section_media,
    stremio_to_plex_id,
)
from plexio.plex.playback import b64decode_path, proxy_playback
from plexio.settings import server_is_optimized, settings
from plexio.stream_cache import (
    cache_get,
    cache_set,
    configuration_cache_namespace,
    deserialize_stream_response,
    resource_cache_key,
    serialize_stream_response,
)

router = APIRouter()
router.dependencies.append(Depends(set_sentry_user))
logger = logging.getLogger(__name__)

# Stream pre-warm: resolve and cache /stream responses when a detail page is
# requested, so the first Play press returns from cache. In-flight keys prevent
# duplicate concurrent resolution for the same media.
#
# Series detail pages warm the show's exact continue-watching episode first: the
# sentinel below is pre-resolved by _warm_stream_cache to the real episode id
# (which is also the cache key the device will hit) before any Plex lookup for
# the stream happens.
_NEXT_UP_PREFIX = 'plexio:nextup-'
_stream_prewarm_in_flight: set[str] = set()
_STREAM_PREWARM_CONCURRENCY = 4
_META_BATCH_CONCURRENCY = 8
# At most this many Plex transcode sessions are started by a single pre-warm
# cycle (catalog or detail load). Every 7.1-EAC3 row would otherwise kick its
# own real Plex transcode; the cap keeps the shelf warm from spawning a batch of
# abandoned transcodes on the Plex server.
_TRANSCODE_PREWARM_CAP = 1
# Search results pre-warm the first rows of the movie catalog response, so a
# Play press from search results is also served from cache.
_SEARCH_PREWARM_ITEMS = 8


def _prewarm_warm_ids(
    stremio_type: StremioMediaType, warm_ids: list[str]
) -> list[str]:
    """Bound the number of media pre-warmed per request (user-configurable).

    Series warm the most recent episodes: the common Play press targets a
    continuation episode at the newest end of a long-running show.
    """
    if stremio_type == StremioMediaType.series:
        limit = settings.stream_prewarm_max_episodes
        if limit <= 0:
            return []
        return warm_ids[-limit:]
    limit = settings.stream_prewarm_catalog_items
    if limit <= 0:
        return []
    return warm_ids[:limit]


def _prewarm_catalog_allowed(catalog_id: str) -> bool:
    """Pre-warm all catalogs (sections, recent, collections), bounded by the
    stream_prewarm_catalog_items / stream_prewarm_max_episodes caps and the
    pre-warm concurrency limit + in-flight dedup."""
    return True


def _catalog_prewarm_targets(
    stremio_type: StremioMediaType,
    on_deck_items: list[dict] | None,
    configured_sections: set[str],
    metas: list,
) -> list[tuple[StremioMediaType, str]]:
    """Pick bounded pre-warm targets for a catalog response.

    On-deck catalogs warm each row's exact next-up episode (the raw on-deck
    items ARE the in-progress episodes) plus movie rows. Non-on-deck catalogs
    warm movie rows only, bounded by the configured caps.

    For series catalogs (sections/collections), warm the next-up episode of
    each show in the grid (first N shows), so grid-browsed shows are instant.
    """
    if stremio_type == StremioMediaType.movie:
        return [
            (stremio_type, item.id)
            for item in metas[: settings.stream_prewarm_catalog_items]
            if item.id
        ]
    if stremio_type == StremioMediaType.series and on_deck_items is not None:
        targets: list[tuple[StremioMediaType, str]] = []
        for item in on_deck_items:
            if item.get('type') != 'episode':
                continue
            section = str(item.get('librarySectionID', ''))
            if configured_sections and section not in configured_sections:
                continue
            key = item.get('ratingKey')
            if key:
                targets.append((stremio_type, f'plexio:rk-{key}'))
        return targets[: settings.stream_prewarm_max_episodes]
    if stremio_type == StremioMediaType.series:
        # Series grid (sections/collections): warm next-up for first N shows
        return [
            (stremio_type, f'{_NEXT_UP_PREFIX}{item.id}')
            for item in metas[: settings.stream_prewarm_catalog_items]
            if item.id
        ]
    return []


def _search_prewarm_targets(
    stremio_type: StremioMediaType, search: str, metas: list
) -> list[tuple[StremioMediaType, str]]:
    """Pick pre-warm targets from a search response (movie rows only)."""
    if not search or stremio_type != StremioMediaType.movie:
        return []
    return [
        (stremio_type, item.id)
        for item in metas[: _SEARCH_PREWARM_ITEMS]
        if item.id
    ]


def _schedule_stream_prewarm(
    *,
    http,
    cache,
    configuration: AddonConfiguration,
    namespace: str,
    stremio_type: StremioMediaType,
    warm_ids: list[str],
    config_path: str,
    play_prefix: str | None,
) -> None:
    """Schedule background /stream pre-warm tasks when enabled."""
    if not settings.stream_prewarm:
        return []
    if settings.stream_cache_ttl <= 0:
        return []
    warm_ids = [
        media_id
        for media_id in _prewarm_warm_ids(stremio_type, warm_ids)
        if media_id
    ]
    if not warm_ids:
        return []
    semaphore = asyncio.Semaphore(_STREAM_PREWARM_CONCURRENCY)
    transcode_budget = [_TRANSCODE_PREWARM_CAP]
    return [
        asyncio.create_task(
            _warm_stream_cache(
                http=http,
                cache=cache,
                configuration=configuration,
                namespace=namespace,
                stremio_type=stremio_type,
                media_id=media_id,
                config_path=config_path,
                play_prefix=play_prefix,
                semaphore=semaphore,
                transcode_budget=transcode_budget,
            ),
        )
        for media_id in warm_ids
    ]

RECENT_SORT = 'Date Added (desc)'


def _public_base_url(request: Request) -> str:
    """Resolve the externally reachable base URL for playback proxy links."""
    if settings.base_url:
        candidate = settings.base_url
    else:
        forwarded_proto = (
            request.headers.get('x-forwarded-proto')
            if settings.trust_proxy_headers
            else None
        )
        forwarded_host = (
            request.headers.get('x-forwarded-host')
            if settings.trust_proxy_headers
            else None
        )
        scheme = (
            forwarded_proto.split(',', 1)[0].strip()
            if forwarded_proto
            else request.url.scheme
        )
        host = (
            forwarded_host.split(',', 1)[0].strip()
            if forwarded_host
            else (
                getattr(request.url, 'netloc', None)
                or getattr(request.url, 'authority', None)
            )
        )
        candidate = f'{scheme}://{host}'

    try:
        public_url = URL(candidate)
    except (TypeError, ValueError):
        public_url = URL()
    if (
        public_url.scheme not in {'http', 'https'}
        or not public_url.host
        or public_url.user is not None
        or public_url.query_string
        or public_url.fragment
    ):
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail='Unable to determine the public Plexio URL',
        )
    return str(public_url).rstrip('/')


def _uses_playback_proxy(configuration: AddonConfiguration) -> bool:
    return (
        configuration.report_playback
        and configuration.proxy_streams
        and not settings.disable_stream_proxy
    )


def _sections_of_type(configuration, stremio_type):
    return [
        s
        for s in configuration.sections
        if PLEX_TO_STREMIO_MEDIA_TYPE.get(s.type) == stremio_type
    ]


async def _map_on_deck(http, items, configuration, stremio_type):
    """Map raw Plex On Deck items to Stremio catalog metas of one type.

    In-progress movies map directly; on-deck episodes are mapped up to their
    parent series (deduped) so the row shows shows, not loose episodes."""
    configured = {s.key for s in _sections_of_type(configuration, stremio_type)}
    metas = []
    seen = set()
    for item in items:
        section = str(item.get('librarySectionID', ''))
        if configured and section not in configured:
            continue
        if stremio_type == StremioMediaType.movie and item.get('type') == 'movie':
            if not item.get('guid'):
                continue
            metas.append(PlexMediaMeta(**item).to_stremio_meta_review(configuration))
        elif stremio_type == StremioMediaType.series and item.get('type') == 'episode':
            show_guid = item.get('grandparentGuid')
            if not show_guid or show_guid in seen:
                continue
            seen.add(show_guid)
            resolved = await get_media(
                client=http,
                url=configuration.discovery_url,
                token=configuration.access_token,
                guid=show_guid,
                get_only_first=True,
            )
            if resolved:
                metas.append(resolved[0].to_stremio_meta_review(configuration))
            else:
                show = PlexMediaMeta(
                    guid=show_guid,
                    type='show',
                    title=item.get('grandparentTitle', ''),
                    thumb=item.get('grandparentThumb'),
                    librarySectionID=section,
                    addedAt=item.get('addedAt', 0),
                )
                metas.append(show.to_stremio_meta_review(configuration))
    return metas


async def _recently_added(http, configuration, stremio_type, skip):
    """Recently added top-level items across configured sections of a type,
    merged and sorted by date added. Reuses the section-media call with an
    addedAt:desc sort, so it returns movies/shows, never loose episodes."""
    results = []
    for section in _sections_of_type(configuration, stremio_type):
        results.extend(
            await get_section_media(
                client=http,
                url=configuration.discovery_url,
                token=configuration.access_token,
                section_id=section.key,
                search='',
                skip=skip,
                sort=RECENT_SORT,
            )
        )
    results.sort(key=lambda m: m.added_at, reverse=True)
    return [m.to_stremio_meta_review(configuration) for m in results[:100]]


@router.get('/manifest.json', response_model_exclude_none=True)
@router.get('/{session_id}/manifest.json', response_model_exclude_none=True)
@router.get(
    '/{installation_id}/{base64_cfg}/manifest.json', response_model_exclude_none=True
)
async def get_manifest(
    configuration: Annotated[
        AddonConfiguration | None,
        Depends(get_addon_configuration),
    ],
    installation_id: str | None = None,
    session_id: str | None = None,
) -> StremioManifest:
    catalogs = []
    description = 'Play movies and series from plex.tv.'
    name = 'Plexio'

    if configuration is not None:
        has_movie = any(
            PLEX_TO_STREMIO_MEDIA_TYPE.get(s.type) == StremioMediaType.movie
            for s in configuration.sections
        )
        has_series = any(
            PLEX_TO_STREMIO_MEDIA_TYPE.get(s.type) == StremioMediaType.series
            for s in configuration.sections
        )
        hub_extra = [{'name': 'skip', 'isRequired': False}]
        sv = configuration.server_name
        if has_movie:
            catalogs.append(
                StremioCatalogManifest(
                    id='plexio-ondeck',
                    type=StremioMediaType.movie,
                    name=f'Continue Watching - Movies | {sv}',
                    extra=hub_extra,
                ),
            )
        if has_series:
            catalogs.append(
                StremioCatalogManifest(
                    id='plexio-ondeck',
                    type=StremioMediaType.series,
                    name=f'Continue Watching - Shows | {sv}',
                    extra=hub_extra,
                ),
            )
        if has_movie:
            catalogs.append(
                StremioCatalogManifest(
                    id='plexio-recent',
                    type=StremioMediaType.movie,
                    name=f'Recently Added - Movies | {sv}',
                    extra=hub_extra,
                ),
            )
        if has_series:
            catalogs.append(
                StremioCatalogManifest(
                    id='plexio-recent',
                    type=StremioMediaType.series,
                    name=f'Recently Added - Shows | {sv}',
                    extra=hub_extra,
                ),
            )
        for section in configuration.sections:
            catalogs.append(
                StremioCatalogManifest(
                    id=section.key,
                    type=PLEX_TO_STREMIO_MEDIA_TYPE[section.type],
                    name=f'{section.title} | {configuration.server_name}',
                    extra=[
                        {'name': 'skip', 'isRequired': False},
                        {'name': 'search', 'isRequired': False},
                        {'name': 'sort', 'options': list(SORT_OPTIONS.keys())},
                    ],
                ),
            )
        for collection in configuration.configured_collections:
            section_title = next(
                section.title
                for section in configuration.sections
                if section.key == collection.section_key
            )
            catalogs.append(
                StremioCatalogManifest(
                    id=collection.catalog_id,
                    type=PLEX_TO_STREMIO_MEDIA_TYPE[collection.type],
                    name=f'Collection: {collection.title} ({section_title}) | {sv}',
                    extra=[{'name': 'skip', 'isRequired': False}],
                ),
            )

        name += f' ({configuration.server_name})'
        description += f' Your installation ID: {installation_id or session_id}'

    return StremioManifest(
        id='com.stremio.plexio',
        version=__version__,
        description=description,
        name=name,
        resources=[
            'stream',
            'catalog',
            {
                'name': 'meta',
                'types': ['movie', 'series'],
                'idPrefixes': ['plexio'],
            },
        ],
        types=[StremioMediaType.movie, StremioMediaType.series],
        catalogs=catalogs,
        idPrefixes=['tt', 'plexio'],
        behaviorHints={
            'configurable': True,
            'configurationRequired': configuration is None,
        },
    )


@router.get(
    '/{session_id}/catalog/{stremio_type}/{catalog_id}.json',
    response_model_exclude_none=True,
)
@router.get(
    '/{session_id}/catalog/{stremio_type}/{catalog_id}/{extra}.json',
    response_model_exclude_none=True,
)
@router.get(
    '/{installation_id}/{base64_cfg}/catalog/{stremio_type}/{catalog_id}.json',
    response_model_exclude_none=True,
)
@router.get(
    '/{installation_id}/{base64_cfg}/catalog/{stremio_type}/{catalog_id}/{extra}.json',
    response_model_exclude_none=True,
)
async def get_catalog(
    request: Request,
    http: Annotated[ClientSession, Depends(get_http_client)],
    cache: Annotated[Redis, Depends(get_cache)],
    configuration: Annotated[AddonConfiguration, Depends(get_addon_configuration)],
    stremio_type: StremioMediaType,
    catalog_id: str,
    extra: str = '',
) -> StremioCatalog:
    extras = {}
    for value in extra.split('&'):
        key, separator, item = value.partition('=')
        if separator:
            extras[key] = item
    try:
        skip = max(int(extras.get('skip', 0)), 0)
    except (TypeError, ValueError):
        skip = 0
    on_deck_items = None
    if catalog_id == 'plexio-ondeck':
        items = await get_on_deck(
            client=http,
            url=configuration.discovery_url,
            token=configuration.access_token,
        )
        on_deck_items = items
        metas = await _map_on_deck(http, items, configuration, stremio_type)
    elif catalog_id == 'plexio-recent':
        metas = await _recently_added(http, configuration, stremio_type, skip)
    elif catalog_id.startswith('plexio-collection-'):
        collection = next(
            (
                item
                for item in configuration.configured_collections
                if item.catalog_id == catalog_id
                and PLEX_TO_STREMIO_MEDIA_TYPE[item.type] == stremio_type
            ),
            None,
        )
        if collection is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
        media = await get_collection_media(
            client=http,
            url=configuration.discovery_url,
            token=configuration.access_token,
            rating_key=collection.rating_key,
            skip=skip,
        )
        metas = [item.to_stremio_meta_review(configuration) for item in media]
    else:
        media = await get_section_media(
            client=http,
            url=configuration.discovery_url,
            token=configuration.access_token,
            section_id=catalog_id,
            search=extras.get('search', ''),
            skip=skip,
            sort=extras.get('sort', 'Title'),
        )
        metas = [m.to_stremio_meta_review(configuration) for m in media]
    result = StremioCatalog(metas=metas)

    if (
        settings.stream_prewarm
        and _prewarm_catalog_allowed(catalog_id)
        and server_is_optimized(configuration.server_name)
    ):
        config_path = ''
        play_prefix = None
        if _uses_playback_proxy(configuration):
            config_path = request.url.path.split('/catalog/')[0]
            play_prefix = f'{_public_base_url(request)}{config_path}/play'
        targets = _catalog_prewarm_targets(
            stremio_type=stremio_type,
            on_deck_items=on_deck_items,
            configured_sections={
                s.key for s in _sections_of_type(configuration, stremio_type)
            },
            metas=result.metas,
        )
        _schedule_stream_prewarm(
            http=http,
            cache=cache,
            configuration=configuration,
            namespace=configuration_cache_namespace(configuration),
            stremio_type=stremio_type,
            warm_ids=[media_id for _, media_id in targets],
            config_path=config_path,
            play_prefix=play_prefix,
        )

    search = extras.get('search', '')
    if (
        settings.stream_prewarm
        and search
        and server_is_optimized(configuration.server_name)
    ):
        config_path = ''
        play_prefix = None
        if _uses_playback_proxy(configuration):
            config_path = request.url.path.split('/catalog/')[0]
            play_prefix = f'{_public_base_url(request)}{config_path}/play'
        targets = _search_prewarm_targets(stremio_type, search, result.metas)
        _schedule_stream_prewarm(
            http=http,
            cache=cache,
            configuration=configuration,
            namespace=configuration_cache_namespace(configuration),
            stremio_type=stremio_type,
            warm_ids=[media_id for _, media_id in targets],
            config_path=config_path,
            play_prefix=play_prefix,
        )
    return result


@router.get(
    '/{session_id}/meta/{stremio_type}/{plex_id:path}.json',
    response_model_exclude_none=True,
)
@router.get(
    '/{installation_id}/{base64_cfg}/meta/{stremio_type}/{plex_id:path}.json',
    response_model_exclude_none=True,
)
async def get_meta(
    request: Request,
    http: Annotated[ClientSession, Depends(get_http_client)],
    cache: Annotated[Redis, Depends(get_cache)],
    configuration: Annotated[AddonConfiguration, Depends(get_addon_configuration)],
    stremio_type: StremioMediaType,
    plex_id: str,
) -> StremioMetaResponse:
    if not plex_id.startswith('plexio:'):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)

    namespace = configuration_cache_namespace(configuration)

    if is_rating_key_plexio_id(plex_id):
        media = await get_media_by_rating_key(
            client=http,
            url=configuration.discovery_url,
            token=configuration.access_token,
            rating_key=plexio_id_to_rating_key(plex_id),
            cache=cache,
            cache_namespace=namespace,
        )
    else:
        # Backward compatibility for previously generated plexio:<base64-guid>
        # links.
        try:
            guid = plexio_id_to_guid(plex_id)
        except ValueError:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND) from None
        media = await get_media(
            client=http,
            url=configuration.discovery_url,
            token=configuration.access_token,
            guid=guid,
            get_only_first=True,
            cache=cache,
            cache_namespace=namespace,
        )
    if not media:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
    media = media[0]

    meta = media.to_stremio_meta(configuration)

    if stremio_type == StremioMediaType.series:
        episodes = await get_all_episodes(
            client=http,
            url=configuration.discovery_url,
            token=configuration.access_token,
            key=media.key,
            cache=cache,
            cache_namespace=namespace,
        )
        meta.videos = [e.to_stremio_video_meta(configuration) for e in episodes]

    result = StremioMetaResponse(meta=meta)

    if settings.stream_prewarm and server_is_optimized(configuration.server_name):
        config_path = ''
        play_prefix = None
        if _uses_playback_proxy(configuration):
            config_path = request.url.path.split('/meta/')[0]
            play_prefix = f'{_public_base_url(request)}{config_path}/play'
        if stremio_type == StremioMediaType.series:
            videos = meta.videos or []
            show_rk = media.rating_key if media and media.rating_key else ''
            if show_rk and videos:
                # Exactly one episode per show is warmed: the continue-watching
                # next-up, falling back to the newest episode when the show has
                # no in-progress entry.
                warm_ids = [f'{_NEXT_UP_PREFIX}{show_rk}|{videos[-1].id}']
            elif videos:
                warm_ids = [videos[-1].id]
            else:
                warm_ids = []
        else:
            warm_ids = [meta.id]
        _schedule_stream_prewarm(
            http=http,
            cache=cache,
            configuration=configuration,
            namespace=configuration_cache_namespace(configuration),
            stremio_type=stremio_type,
            warm_ids=warm_ids,
            config_path=config_path,
            play_prefix=play_prefix,
        )
    return result


async def _resolve_meta(
    *,
    http,
    cache,
    configuration,
    namespace: str,
    stremio_type: StremioMediaType,
    plex_id: str,
):
    """Resolve full Stremio meta (with episodes for series) for one plexio id."""
    if is_rating_key_plexio_id(plex_id):
        media = await get_media_by_rating_key(
            client=http,
            url=configuration.discovery_url,
            token=configuration.access_token,
            rating_key=plexio_id_to_rating_key(plex_id),
            cache=cache,
            cache_namespace=namespace,
        )
    else:
        try:
            guid = plexio_id_to_guid(plex_id)
        except ValueError:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND) from None
        media = await get_media(
            client=http,
            url=configuration.discovery_url,
            token=configuration.access_token,
            guid=guid,
            get_only_first=True,
            cache=cache,
            cache_namespace=namespace,
        )
    if not media:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
    meta = media[0].to_stremio_meta(configuration)
    if stremio_type == StremioMediaType.series:
        episodes = await get_all_episodes(
            client=http,
            url=configuration.discovery_url,
            token=configuration.access_token,
            key=media[0].key,
            cache=cache,
            cache_namespace=namespace,
        )
        meta.videos = [e.to_stremio_video_meta(configuration) for e in episodes]
    return meta


@router.api_route(
    '/{session_id}/meta-batch',
    methods=['POST'],
)
@router.api_route(
    '/{installation_id}/{base64_cfg}/meta-batch',
    methods=['POST'],
)
async def get_meta_batch(
    request: Request,
    http: Annotated[ClientSession, Depends(get_http_client)],
    cache: Annotated[Redis, Depends(get_cache)],
    configuration: Annotated[AddonConfiguration, Depends(get_addon_configuration)],
) -> dict:
    body = await request.json()
    items = body.get('items') or []
    namespace = configuration_cache_namespace(configuration)
    semaphore = asyncio.Semaphore(_META_BATCH_CONCURRENCY)

    async def resolve_one(item: dict) -> dict:
        stremio_type = StremioMediaType(item['type'])
        plex_id = item['id']
        async with semaphore:
            try:
                meta = await _resolve_meta(
                    http=http,
                    cache=cache,
                    configuration=configuration,
                    namespace=namespace,
                    stremio_type=stremio_type,
                    plex_id=plex_id,
                )
                return {'type': item['type'], 'id': plex_id, 'meta': meta}
            except Exception:
                logger.exception('meta-batch resolve failed for id=%s', plex_id)
                return {'type': item['type'], 'id': plex_id, 'meta': None}

    metas = await asyncio.gather(*(resolve_one(item) for item in items))
    return {'metas': metas}


async def _prewarm_transcode_sessions(
    *, http, configuration, streams, transcode_budget
) -> None:
    """Start Plex universal transcode sessions for emitted transcode streams.

    Hitting a transcoded stream's `start.m3u8` URL makes Plex begin transcoding
    immediately, so the first Play press after the detail page is served by an
    already-running transcode session. Only 7.1 EAC3 (Dolby Digital Plus) audio
    is routed to transcode by this server; the kick is limited to the transcode
    URLs plexio itself emits.

    `transcode_budget` is a shared single-element counter for the whole pre-warm
    cycle: the first stream that still has budget starts its session and spends
    the cap, so a catalog load never spawns more than the configured number of
    real Plex transcodes (default 1) across all its rows.
    """
    if not transcode_budget or transcode_budget[0] <= 0:
        return
    token = configuration.access_token
    for stream in streams:
        stream_url = getattr(stream, 'url', None)
        if not stream_url or '/video/:/transcode/universal/start.m3u8' not in stream_url:
            continue
        if token and token not in stream_url and 'X-Plex-Token=' not in stream_url:
            continue
        transcode_budget[0] -= 1
        try:
            async with http.get(stream_url, timeout=settings.plex_requests_timeout) as response:
                await response.read()
            logger.info('Prewarmed Plex transcode session for %s', stream_url)
        except Exception:
            logger.warning(
                'Plex transcode prewarm failed for %s', stream_url, exc_info=True
            )
        break


def _split_next_up(media_id: str) -> tuple[str, str | None]:
    """Split `plexio:nextup-<show rk>` into `(show rk, fallback id | None)`."""
    payload = media_id[len(_NEXT_UP_PREFIX):]
    if '|' not in payload:
        return payload, None
    show_rk, fallback = payload.rsplit('|', 1)
    return show_rk, fallback or None


async def get_next_up_media_id(
    *,
    http,
    cache,
    configuration,
    namespace: str,
    stremio_type: StremioMediaType,
    media_id: str,
) -> str | None:
    """Resolve a `plexio:nextup-<show rk>` sentinel to the real episode id.

    Returns the continue-watching episode id when Plex has one; otherwise the
    embedded fallback id (the show's newest episode); None only when neither
    exists (warm aborts).
    """
    show_rating_key, fallback = _split_next_up(media_id)
    episode = await get_next_up_episode(
        client=http,
        url=configuration.discovery_url,
        token=configuration.access_token,
        show_rating_key=show_rating_key,
    )
    if not episode:
        return fallback
    return f'plexio:rk-{episode}'


async def _warm_stream_cache(
    *,
    http,
    cache,
    configuration,
    namespace: str,
    stremio_type: StremioMediaType,
    media_id: str,
    config_path: str,
    play_prefix: str | None,
    semaphore: asyncio.Semaphore,
    transcode_budget: list[int],
) -> None:
    if settings.stream_cache_ttl <= 0:
        return
    if media_id.startswith(_NEXT_UP_PREFIX):
        # Resolve the continue-watching sentinel to the real episode id BEFORE
        # computing the cache key, so the device's actual /stream URL is the one
        # warmed. The sentinel carries the newest-episode fallback for shows
        # with no in-progress entry.
        media_id = await get_next_up_media_id(
            http=http,
            cache=cache,
            configuration=configuration,
            namespace=namespace,
            stremio_type=stremio_type,
            media_id=media_id,
        )
        if not media_id:
            return
    stream_key = resource_cache_key(
        namespace,
        'stream',
        f'{stremio_type.value}:{media_id}',
    )
    if stream_key in _stream_prewarm_in_flight:
        return
    async with semaphore:
        if stream_key in _stream_prewarm_in_flight:
            return
        if await cache_get(cache, stream_key) is not None:
            return
        _stream_prewarm_in_flight.add(stream_key)
        try:
            media = await _resolve_stream_media(
                http=http,
                cache=cache,
                configuration=configuration,
                namespace=namespace,
                stremio_type=stremio_type,
                media_id=media_id,
            )
            result = StremioStreamsResponse(
                streams=chain.from_iterable(
                    meta.get_stremio_streams(configuration, play_prefix)
                    for meta in media
                ),
            )
            if (
                settings.eac3_71_transcode
                and server_is_optimized(configuration.server_name)
            ):
                await _prewarm_transcode_sessions(
                    http=http,
                    configuration=configuration,
                    streams=result.streams,
                    transcode_budget=transcode_budget,
                )
            await cache_set(
                cache,
                stream_key,
                serialize_stream_response(result, configuration.access_token),
                ttl=settings.stream_cache_ttl,
            )
            logger.info(
                'Stream prewarm type=%s media_id=%s streams=%d',
                stremio_type.value,
                media_id,
                len(result.streams),
            )
        except Exception:
            logger.exception('Stream prewarm failed for media_id=%s', media_id)
        finally:
            _stream_prewarm_in_flight.discard(stream_key)


async def _resolve_rating_key_stream_media(
    *,
    http,
    cache,
    configuration,
    namespace: str,
    stremio_type: StremioMediaType,
    rating_key_value: str,
) -> list[PlexMediaMeta]:
    parts = rating_key_value.split(':')
    if stremio_type != StremioMediaType.series or len(parts) != 3:
        return await get_media_by_rating_key(
            client=http,
            url=configuration.discovery_url,
            token=configuration.access_token,
            rating_key=rating_key_value,
            cache=cache,
            cache_namespace=namespace,
        )

    show_rating_key, season, episode = parts
    shows = await get_media_by_rating_key(
        client=http,
        url=configuration.discovery_url,
        token=configuration.access_token,
        rating_key=show_rating_key,
        cache=cache,
        cache_namespace=namespace,
    )
    if not shows:
        return []
    episodes = await get_all_episodes(
        client=http,
        url=configuration.discovery_url,
        token=configuration.access_token,
        key=shows[0].key,
        cache=cache,
        cache_namespace=namespace,
    )
    episode_rating_key = next(
        (
            item.rating_key
            for item in episodes
            if str(item.parent_index) == season and str(item.index) == episode
        ),
        None,
    )
    if not episode_rating_key:
        return []
    return await get_media_by_rating_key(
        client=http,
        url=configuration.discovery_url,
        token=configuration.access_token,
        rating_key=episode_rating_key,
        cache=cache,
        cache_namespace=namespace,
    )


async def _resolve_stream_media(
    *,
    http,
    cache,
    configuration,
    namespace: str,
    stremio_type: StremioMediaType,
    media_id: str,
) -> list[PlexMediaMeta]:
    if media_id.startswith('tt'):
        plex_id = await stremio_to_plex_id(
            client=http,
            url=configuration.discovery_url,
            token=configuration.access_token,
            cache=cache,
            stremio_id=media_id,
            media_type=STREMIO_TO_PLEX_MEDIA_TYPE[stremio_type],
            cache_namespace=namespace,
        )
        if not plex_id:
            return []
        media_id = plex_id
    elif is_rating_key_plexio_id(media_id):
        return await _resolve_rating_key_stream_media(
            http=http,
            cache=cache,
            configuration=configuration,
            namespace=namespace,
            stremio_type=stremio_type,
            rating_key_value=plexio_id_to_rating_key(media_id),
        )
    elif media_id.startswith('plexio:'):
        try:
            media_id = plexio_id_to_guid(media_id)
        except ValueError:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND) from None

    return await get_media(
        client=http,
        url=configuration.discovery_url,
        token=configuration.access_token,
        guid=media_id,
        cache=cache,
        cache_namespace=namespace,
    )


@router.get(
    '/{session_id}/stream/{stremio_type}/{media_id:path}.json',
    response_model_exclude_none=True,
)
@router.get(
    '/{installation_id}/{base64_cfg}/stream/{stremio_type}/{media_id:path}.json',
    response_model_exclude_none=True,
)
async def get_stream(
    request: Request,
    response: Response,
    http: Annotated[ClientSession, Depends(get_http_client)],
    cache: Annotated[Redis, Depends(get_cache)],
    configuration: Annotated[AddonConfiguration, Depends(get_addon_configuration)],
    stremio_type: StremioMediaType,
    media_id: str,
) -> StremioStreamsResponse:
    started = perf_counter()
    namespace = configuration_cache_namespace(configuration)
    config_path = ''
    if _uses_playback_proxy(configuration):
        config_path = request.url.path.split('/stream/')[0]
    stream_key = resource_cache_key(
        namespace,
        'stream',
        f'{stremio_type.value}:{media_id}',
    )

    cache_started = perf_counter()
    cached = await cache_get(cache, stream_key) if settings.stream_cache_ttl else None
    cache_ms = (perf_counter() - cache_started) * 1000
    if cached is not None:
        try:
            result = deserialize_stream_response(
                cached,
                configuration.access_token,
            )
        except ValueError:
            logger.warning('Ignoring invalid cached stream response')
        else:
            total_ms = (perf_counter() - started) * 1000
            response.headers['X-Plexio-Cache'] = 'HIT'
            response.headers['Server-Timing'] = (
                f'cache;dur={cache_ms:.1f}, total;dur={total_ms:.1f}'
            )
            logger.info(
                'Stream response type=%s cache=hit streams=%d total_ms=%.1f',
                stremio_type.value,
                len(result.streams),
                total_ms,
            )
            return result

    lookup_started = perf_counter()
    media = await _resolve_stream_media(
        http=http,
        cache=cache,
        configuration=configuration,
        namespace=namespace,
        stremio_type=stremio_type,
        media_id=media_id,
    )
    lookup_ms = (perf_counter() - lookup_started) * 1000

    build_started = perf_counter()
    play_prefix = None
    if _uses_playback_proxy(configuration):
        base = _public_base_url(request)
        play_prefix = f'{base}{config_path}/play'
    result = StremioStreamsResponse(
        streams=chain.from_iterable(
            meta.get_stremio_streams(configuration, play_prefix) for meta in media
        ),
    )
    build_ms = (perf_counter() - build_started) * 1000

    cache_write_started = perf_counter()
    await cache_set(
        cache,
        stream_key,
        serialize_stream_response(result, configuration.access_token),
        ttl=settings.stream_cache_ttl,
    )
    cache_ms += (perf_counter() - cache_write_started) * 1000
    total_ms = (perf_counter() - started) * 1000
    response.headers['X-Plexio-Cache'] = 'MISS'
    response.headers['Server-Timing'] = (
        f'cache;dur={cache_ms:.1f}, plex;dur={lookup_ms:.1f}, '
        f'build;dur={build_ms:.1f}, total;dur={total_ms:.1f}'
    )
    logger.info(
        'Stream response type=%s cache=miss streams=%d total_ms=%.1f',
        stremio_type.value,
        len(result.streams),
        total_ms,
    )
    return result


@router.api_route(
    '/{session_id}/play/{rating_key}/{duration}/{part_b64}',
    methods=['GET', 'HEAD'],
)
@router.api_route(
    '/{installation_id}/{base64_cfg}/play/{rating_key}/{duration}/{part_b64}',
    methods=['GET', 'HEAD'],
)
async def get_play(
    request: Request,
    http: Annotated[ClientSession, Depends(get_http_client)],
    configuration: Annotated[AddonConfiguration, Depends(get_addon_configuration)],
    rating_key: str,
    duration: int,
    part_b64: str,
    session_id: str | None = None,
    installation_id: str | None = None,
):
    if configuration is None or not _uses_playback_proxy(configuration):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
    return await proxy_playback(
        request,
        client=http,
        configuration=configuration,
        rating_key=rating_key,
        duration_ms=duration,
        part_key=b64decode_path(part_b64),
        identifier=session_id or installation_id or 'plexio',
    )
