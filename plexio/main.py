import os
from contextlib import asynccontextmanager
from pathlib import Path

import aiohttp
import sentry_sdk
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware

from plexio.cache import init_cache
from plexio.routers.addon import PREWARM_ACTIVITY_KEY
from plexio.routers.addon import router as addon_router
from plexio.routers.configuration import router as configuration_router
from plexio.routers.plex_proxy import router as plex_proxy_router
from plexio.security import RequestBodyLimitMiddleware, SecurityHeadersMiddleware
from plexio.sessions import init_sessions
from plexio.settings import settings
from plexio.static import SPAStaticFiles
from plexio.stream_cache import cache_get


def before_send(event, hint):
    if 'exc_info' in hint:
        exc_type, exc_value, tb = hint['exc_info']
        if isinstance(exc_value, HTTPException) and exc_value.status_code in (502, 504):
            return None
    return event


sentry_sdk.init(before_send=before_send)


import asyncio
import logging


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    # uvicorn's default logging leaves the root logger handler-less, so
    # records from this logger would never reach the journal. Attach our
    # own stderr handler instead of touching the global logging config.
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter('%(levelname)s: %(name)s: %(message)s'))
    logger.addHandler(_handler)


async def _periodic_prewarm(plex_client, cache, sessions):
    """Periodically pre-warm main sections/collections for all sessions."""
    # Run once immediately at startup (then every 25 min), so a service
    # restart doesn't leave a 25-minute gap with cold catalogs.
    first_run = True
    while True:
        try:
            if not first_run:
                await asyncio.sleep(1500)  # 25 minutes (just before 30-min cache TTL)
            first_run = False
            if sessions is None:
                continue
            if await cache_get(cache, PREWARM_ACTIVITY_KEY) is None:
                logger.info('Periodic prewarm: no recent activity, skipping')
                continue
            # Get all active sessions
            session_list = await sessions.list()
            if not session_list:
                logger.info('Periodic prewarm: no active sessions, skipping')
                continue
            warmed = 0
            failed = 0
            for session_info in session_list:
                session_id = session_info['session_id']
                try:
                    config = await sessions.get_config(session_id)
                    if config is None:
                        continue
                    base_url = "http://127.0.0.1:7777"
                    # Critical catalogs only: on-deck, recent, 3 main sections
                    critical_catalogs = [
                        ("movie", "plexio-ondeck"),
                        ("movie", "plexio-recent"),
                        ("movie", "3"),
                        ("series", "plexio-ondeck"),
                        ("series", "plexio-recent"),
                        ("series", "4"),
                        ("series", "5"),
                    ]
                    for ctype, cid in critical_catalogs:
                        try:
                            async with plex_client.get(
                                f"{base_url}/{session_id}/catalog/{ctype}/{cid}.json",
                                timeout=aiohttp.ClientTimeout(total=15),
                            ) as r:
                                if r.status == 200:
                                    warmed += 1
                                else:
                                    failed += 1
                        except Exception:
                            failed += 1
                except Exception:
                    logger.exception('Periodic prewarm failed for session_id=%s', session_id)
            logger.info(
                'Periodic prewarm done sessions=%d warmed=%d failed=%d',
                len(session_list),
                warmed,
                failed,
            )
        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception('Periodic prewarm pass failed')


@asynccontextmanager
async def lifespan(app: FastAPI):
    connector = aiohttp.TCPConnector(
        limit=8,
        keepalive_timeout=30,
    )
    plex_client = aiohttp.ClientSession(
        headers={'accept': 'application/json'},
        connector=connector,
    )
    cache = init_cache(settings)
    sessions = await init_sessions(settings)

    # Start periodic pre-warm task
    prewarm_task = asyncio.create_task(_periodic_prewarm(plex_client, cache, sessions))

    yield {
        'plex_client': plex_client,
        'cache': cache,
        'sessions': sessions,
    }

    prewarm_task.cancel()
    try:
        await prewarm_task
    except asyncio.CancelledError:
        pass

    await plex_client.close()
    await cache.close()
    if sessions is not None:
        await sessions.close()


app = FastAPI(
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=settings.cors_origin_regex,
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)
app.add_middleware(
    RequestBodyLimitMiddleware,
    max_body_size=settings.max_request_body_size,
)
if settings.allowed_host_list:
    app.add_middleware(
        TrustedHostMiddleware,
        allowed_hosts=settings.allowed_host_list,
    )
app.add_middleware(SecurityHeadersMiddleware)

app.include_router(addon_router)
app.include_router(configuration_router)
app.include_router(plex_proxy_router)

frontend_dir = Path(os.getenv('PLEXIO_FRONTEND_DIR', 'frontend/dist'))
if frontend_dir.is_dir():
    app.mount('/', SPAStaticFiles(directory=frontend_dir, html=True), name='frontend')
