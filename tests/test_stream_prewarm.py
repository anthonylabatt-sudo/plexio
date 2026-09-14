import asyncio
import unittest
from types import SimpleNamespace
from unittest import mock

from plexio.routers import addon
from plexio.settings import settings


class FakeCache:
    def __init__(self):
        self._store = {}

    async def get(self, key):
        return self._store.get(key)

    async def set(self, key, value, *, ttl):
        if ttl > 0:
            self._store[key] = value


class FakeConfiguration:
    access_token = 'test-token'


STREAM = {'url': 'http://x', 'name': 'Test', 'description': 'test'}
TRANSCODE_STREAM = {
    'url': 'http://plex.example:32400/video/:/transcode/universal/start.m3u8?path=/lib/1&X-Plex-Token=tok',
    'name': 'Test',
    'description': 'transcode',
}


class FakeMedia:
    def __init__(self, streams):
        self._streams = streams

    def get_stremio_streams(self, configuration, play_prefix):
        return list(self._streams)


def schedule_kwargs(cache):
    return {
        'http': object(),
        'cache': cache,
        'configuration': FakeConfiguration(),
        'namespace': 'ns',
        'stremio_type': addon.StremioMediaType.movie,
        'warm_ids': ['plexio:rk-1'],
        'config_path': '',
        'play_prefix': None,
    }


class StreamPrewarmTests(unittest.TestCase):
    _FIELDS = (
        'stream_prewarm',
        'stream_prewarm_max_episodes',
        'stream_prewarm_catalog_items',
        'stream_prewarm_catalogs',
        'stream_cache_ttl',
        'eac3_71_transcode',
    )

    def setUp(self) -> None:
        self._saved = {name: getattr(settings, name) for name in self._FIELDS}
        addon._stream_prewarm_in_flight.clear()

    def tearDown(self) -> None:
        for name, value in self._saved.items():
            setattr(settings, name, value)
        addon._stream_prewarm_in_flight.clear()

    def _run(self, coro):
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()

    def _warm_key(self, media_id, stremio_type='movie'):
        return addon.resource_cache_key(
            'ns', 'stream', f'{stremio_type}:{media_id}:'
        )

    def test_settings_defaults(self):
        self.assertFalse(settings.stream_prewarm)
        self.assertEqual(settings.stream_prewarm_max_episodes, 12)
        self.assertEqual(settings.stream_prewarm_catalog_items, 5)
        self.assertEqual(
            settings.stream_prewarm_catalogs, 'plexio-ondeck,plexio-recent'
        )

    def test_warm_ids_are_capped(self):
        ids = [f'plexio:rk-{i}' for i in range(25)]
        settings.stream_prewarm_max_episodes = 3
        self.assertEqual(
            addon._prewarm_warm_ids(addon.StremioMediaType.series, ids), ids[-3:]
        )
        settings.stream_prewarm_catalog_items = 2
        self.assertEqual(
            addon._prewarm_warm_ids(addon.StremioMediaType.movie, ids), ids[:2]
        )

    def test_catalog_allowed_gate(self):
        self.assertTrue(addon._prewarm_catalog_allowed('plexio-ondeck'))
        self.assertTrue(addon._prewarm_catalog_allowed('plexio-recent'))
        self.assertFalse(addon._prewarm_catalog_allowed('3'))
        settings.stream_prewarm_catalogs = 'plexio-ondeck'
        self.assertTrue(addon._prewarm_catalog_allowed('plexio-ondeck'))
        self.assertFalse(addon._prewarm_catalog_allowed('plexio-recent'))

    def test_catalog_prewarm_targets_movies_capped(self):
        settings.stream_prewarm_catalog_items = 2
        metas = [SimpleNamespace(id=f'plexio:rk-{i}') for i in range(4)]
        targets = addon._catalog_prewarm_targets(
            stremio_type=addon.StremioMediaType.movie,
            on_deck_items=None,
            configured_sections=set(),
            metas=metas,
        )
        self.assertEqual(
            targets,
            [
                (addon.StremioMediaType.movie, 'plexio:rk-0'),
                (addon.StremioMediaType.movie, 'plexio:rk-1'),
            ],
        )

    def test_catalog_prewarm_targets_movies_skip_blank_ids(self):
        metas = [SimpleNamespace(id=''), SimpleNamespace(id='plexio:rk-1')]
        targets = addon._catalog_prewarm_targets(
            stremio_type=addon.StremioMediaType.movie,
            on_deck_items=None,
            configured_sections=set(),
            metas=metas,
        )
        self.assertEqual(
            targets, [(addon.StremioMediaType.movie, 'plexio:rk-1')]
        )

    def test_catalog_prewarm_targets_on_deck_episodes(self):
        settings.stream_prewarm_max_episodes = 2
        items = [
            {
                'type': 'episode',
                'ratingKey': '327316',
                'grandparentRatingKey': 327120,
                'librarySectionID': 4,
            },
            {'type': 'episode', 'ratingKey': '327122', 'librarySectionID': 4},
            {'type': 'movie', 'ratingKey': '58595'},
        ]
        targets = addon._catalog_prewarm_targets(
            stremio_type=addon.StremioMediaType.series,
            on_deck_items=items,
            configured_sections={'4'},
            metas=[],
        )
        self.assertEqual(
            targets,
            [
                (addon.StremioMediaType.series, 'plexio:rk-327316'),
                (addon.StremioMediaType.series, 'plexio:rk-327122'),
            ],
        )

    def test_catalog_prewarm_targets_on_deck_filters_sections(self):
        items = [
            {'type': 'episode', 'ratingKey': '1', 'librarySectionID': 4},
            {'type': 'episode', 'ratingKey': '2', 'librarySectionID': 99},
        ]
        targets = addon._catalog_prewarm_targets(
            stremio_type=addon.StremioMediaType.series,
            on_deck_items=items,
            configured_sections={'4'},
            metas=[],
        )
        self.assertEqual(
            targets, [(addon.StremioMediaType.series, 'plexio:rk-1')]
        )

    def test_catalog_prewarm_targets_series_non_on_deck_empty(self):
        targets = addon._catalog_prewarm_targets(
            stremio_type=addon.StremioMediaType.series,
            on_deck_items=None,
            configured_sections=set(),
            metas=[SimpleNamespace(id='plexio:rk-327120')],
        )
        self.assertEqual(targets, [])

    def test_schedule_disabled_returns_no_tasks(self):
        settings.stream_prewarm = False
        cache = FakeCache()
        resolve = mock.AsyncMock(return_value=[])
        with mock.patch.object(addon, '_resolve_stream_media', new=resolve):
            tasks = addon._schedule_stream_prewarm(
                **schedule_kwargs(cache)
            )
        self.assertEqual(tasks, [])
        self.assertEqual(cache._store, {})
        resolve.assert_not_awaited()

    def test_ttl_zero_returns_no_tasks(self):
        settings.stream_prewarm = True
        settings.stream_cache_ttl = 0
        cache = FakeCache()
        resolve = mock.AsyncMock(return_value=[FakeMedia([STREAM])])
        patcher = mock.patch.object(addon, '_resolve_stream_media', new=resolve)
        with patcher:
            tasks = addon._schedule_stream_prewarm(**schedule_kwargs(cache))
        self.assertEqual(tasks, [])
        self.assertEqual(cache._store, {})

    def test_schedule_warms_capped_movie_ids(self):
        settings.stream_prewarm = True
        settings.stream_prewarm_catalog_items = 2
        cache = FakeCache()
        media = FakeMedia([STREAM])

        async def scenario():
            resolve = mock.AsyncMock(return_value=[media])
            patcher = mock.patch.object(addon, '_resolve_stream_media', new=resolve)
            with patcher:
                tasks = addon._schedule_stream_prewarm(
                    **{
                        **schedule_kwargs(cache),
                        'warm_ids': ['plexio:rk-1', 'plexio:rk-2', 'plexio:rk-3'],
                    }
                )
                await asyncio.gather(*tasks)

        self._run(scenario())
        self.assertEqual(
            set(cache._store),
            {self._warm_key('plexio:rk-1'), self._warm_key('plexio:rk-2')},
        )

    def test_schedule_warms_bounded_series_episodes(self):
        settings.stream_prewarm = True
        settings.stream_prewarm_max_episodes = 3
        cache = FakeCache()
        media = FakeMedia([STREAM])
        ids = [f'plexio:rk-{i}' for i in range(25)]

        async def scenario():
            resolve = mock.AsyncMock(return_value=[media])
            patcher = mock.patch.object(addon, '_resolve_stream_media', new=resolve)
            with patcher:
                tasks = addon._schedule_stream_prewarm(
                    **{
                        **schedule_kwargs(cache),
                        'stremio_type': addon.StremioMediaType.series,
                        'warm_ids': ids,
                    }
                )
                await asyncio.gather(*tasks)

        self._run(scenario())
        self.assertEqual(
            set(cache._store),
            {
                self._warm_key(ids[-3], 'series'),
                self._warm_key(ids[-2], 'series'),
                self._warm_key(ids[-1], 'series'),
            },
        )

    def test_warm_skips_existing_cache(self):
        settings.stream_prewarm = True
        cache = FakeCache()
        key = self._warm_key('plexio:rk-9')
        cache._store[key] = 'already-cached'
        resolve = mock.AsyncMock(return_value=[FakeMedia([STREAM])])

        async def scenario():
            patcher = mock.patch.object(addon, '_resolve_stream_media', new=resolve)
            with patcher:
                tasks = addon._schedule_stream_prewarm(
                    **{**schedule_kwargs(cache), 'warm_ids': ['plexio:rk-9']}
                )
                await asyncio.gather(*tasks)

        self._run(scenario())
        resolve.assert_not_awaited()
        self.assertEqual(cache._store[key], 'already-cached')

    def test_concurrent_same_key_resolves_once(self):
        settings.stream_prewarm = True
        cache = FakeCache()
        calls = []

        async def slow_resolve(**kwargs):
            calls.append(kwargs['media_id'])
            await asyncio.sleep(0.05)
            return [FakeMedia([STREAM])]

        async def scenario():
            resolve = mock.AsyncMock(side_effect=slow_resolve)
            patcher = mock.patch.object(addon, '_resolve_stream_media', new=resolve)
            with patcher:
                kwargs = {
                    'http': object(),
                    'cache': cache,
                    'configuration': FakeConfiguration(),
                    'namespace': 'ns',
                    'stremio_type': addon.StremioMediaType.movie,
                    'media_id': 'plexio:rk-7',
                    'config_path': '',
                    'play_prefix': None,
                    'semaphore': asyncio.Semaphore(4),
                    'transcode_budget': [addon._TRANSCODE_PREWARM_CAP],
                }
                first = asyncio.create_task(addon._warm_stream_cache(**kwargs))
                second = asyncio.create_task(addon._warm_stream_cache(**kwargs))
                await asyncio.gather(first, second)
                return resolve.await_count

        self.assertEqual(self._run(scenario()), 1)
        self.assertEqual(calls, ['plexio:rk-7'])

    def test_warm_failure_is_swallowed(self):
        settings.stream_prewarm = True
        cache = FakeCache()

        async def failing_resolve(**kwargs):
            raise RuntimeError('boom')

        async def scenario():
            resolve = mock.AsyncMock(side_effect=failing_resolve)
            patcher = mock.patch.object(addon, '_resolve_stream_media', new=resolve)
            with patcher:
                with mock.patch.object(addon.logger, 'exception'):
                    tasks = addon._schedule_stream_prewarm(**schedule_kwargs(cache))
                    await asyncio.gather(*tasks)
                    return list(addon._stream_prewarm_in_flight)

        self.assertEqual(self._run(scenario()), [])
        self.assertEqual(cache._store, {})

    def test_warm_kicks_transcode_session_when_enabled(self):
        settings.stream_prewarm = True
        settings.eac3_71_transcode = True
        cache = FakeCache()

        class FakeResponse:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return False

            async def read(self):
                return b'#EXTM3U'

        http = mock.Mock()
        http.get = mock.Mock(return_value=FakeResponse())
        media = FakeMedia([TRANSCODE_STREAM])

        async def scenario():
            resolve = mock.AsyncMock(return_value=[media])
            patcher = mock.patch.object(addon, '_resolve_stream_media', new=resolve)
            with patcher:
                tasks = addon._schedule_stream_prewarm(
                    **{
                        **schedule_kwargs(cache),
                        'http': http,
                    }
                )
                await asyncio.gather(*tasks)

        self._run(scenario())
        self.assertEqual(http.get.call_count, 1)
        url = http.get.call_args[0][0]
        self.assertIn('/video/:/transcode/universal/start.m3u8', url)
        self.assertEqual(set(cache._store), {self._warm_key('plexio:rk-1')})

    def test_warm_skips_transcode_kick_when_disabled(self):
        settings.stream_prewarm = True
        settings.eac3_71_transcode = False
        cache = FakeCache()
        http = mock.Mock()
        http.get = mock.Mock()
        media = FakeMedia([TRANSCODE_STREAM])

        async def scenario():
            resolve = mock.AsyncMock(return_value=[media])
            patcher = mock.patch.object(addon, '_resolve_stream_media', new=resolve)
            with patcher:
                tasks = addon._schedule_stream_prewarm(
                    **{
                        **schedule_kwargs(cache),
                        'http': http,
                    }
                )
                await asyncio.gather(*tasks)

        self._run(scenario())
        http.get.assert_not_called()
        self.assertEqual(set(cache._store), {self._warm_key('plexio:rk-1')})

    def test_warm_kick_failure_is_swallowed(self):
        settings.stream_prewarm = True
        settings.eac3_71_transcode = True
        cache = FakeCache()

        class FailingResponse:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return False

            async def read(self):
                raise RuntimeError('nope')

        http = mock.Mock()
        http.get = mock.Mock(return_value=FailingResponse())
        media = FakeMedia([TRANSCODE_STREAM])

        async def scenario():
            resolve = mock.AsyncMock(return_value=[media])
            patcher = mock.patch.object(addon, '_resolve_stream_media', new=resolve)
            with patcher:
                with mock.patch.object(addon.logger, 'warning'):
                    tasks = addon._schedule_stream_prewarm(
                        **{
                            **schedule_kwargs(cache),
                            'http': http,
                        }
                    )
                    await asyncio.gather(*tasks)
                    return list(addon._stream_prewarm_in_flight)

        self.assertEqual(self._run(scenario()), [])
        self.assertEqual(set(cache._store), {self._warm_key('plexio:rk-1')})

    def test_warm_resolves_next_up_sentinel_to_real_episode_key(self):
        settings.stream_prewarm = True
        cache = FakeCache()
        media = FakeMedia([STREAM])
        kwargs = {
            **schedule_kwargs(cache),
            'stremio_type': addon.StremioMediaType.series,
            'warm_ids': ['plexio:nextup-42|plexio:rk-9'],
        }

        async def scenario():
            resolve = mock.AsyncMock(return_value=[media])
            next_up = mock.AsyncMock(return_value='plexio:rk-7')
            patcher = mock.patch.object(addon, '_resolve_stream_media', new=resolve)
            with patcher, mock.patch.object(
                addon, 'get_next_up_media_id', new=next_up
            ):
                tasks = addon._schedule_stream_prewarm(**kwargs)
                await asyncio.gather(*tasks)
            return resolve.call_args_list

        calls = self._run(scenario())
        # sentinel was resolved to the real episode id (the device's /stream key),
        # and exactly one episode per show is warmed -- no second task for the
        # latest episode when a next-up exists
        self.assertEqual(
            set(cache._store), {self._warm_key('plexio:rk-7', 'series')}
        )
        self.assertEqual([call.kwargs['media_id'] for call in calls], ['plexio:rk-7'])

    def test_warm_aborts_when_sentinel_resolves_to_nothing(self):
        settings.stream_prewarm = True
        cache = FakeCache()
        media = FakeMedia([STREAM])
        kwargs = {
            **schedule_kwargs(cache),
            'stremio_type': addon.StremioMediaType.series,
            'warm_ids': ['plexio:nextup-42|plexio:rk-9'],
        }

        async def scenario():
            resolve = mock.AsyncMock(return_value=[media])
            next_up = mock.AsyncMock(return_value=None)
            patcher = mock.patch.object(addon, '_resolve_stream_media', new=resolve)
            with patcher, mock.patch.object(
                addon, 'get_next_up_media_id', new=next_up
            ):
                tasks = addon._schedule_stream_prewarm(**kwargs)
                await asyncio.gather(*tasks)
            return resolve.call_args_list

        calls = self._run(scenario())
        self.assertEqual(set(cache._store), set())
        self.assertEqual(calls, [])

    def test_next_up_media_id_falls_back_to_newest_episode(self):
        async def scenario():
            with mock.patch.object(
                addon, 'get_next_up_episode', new=mock.AsyncMock(return_value=None)
            ):
                return await addon.get_next_up_media_id(
                    http=mock.Mock(),
                    cache=FakeCache(),
                    configuration=mock.Mock(),
                    namespace='n',
                    stremio_type=addon.StremioMediaType.series,
                    media_id='plexio:nextup-42|plexio:rk-9',
                )

        self.assertEqual(self._run(scenario()), 'plexio:rk-9')

    def test_split_next_up(self):
        self.assertEqual(
            addon._split_next_up('plexio:nextup-42|plexio:rk-9'),
            ('42', 'plexio:rk-9'),
        )
        self.assertEqual(
            addon._split_next_up('plexio:nextup-42'), ('42', None)
        )

    def test_transcode_cap_limits_kicks_per_warm_cycle(self):
        settings.stream_prewarm = True
        settings.eac3_71_transcode = True

        class FakeResponse:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return False

            async def read(self):
                return b'#EXTM3U'

        cache = FakeCache()
        http = mock.Mock()
        http.get = mock.Mock(return_value=FakeResponse())
        media = FakeMedia([TRANSCODE_STREAM])
        kwargs = {
            **schedule_kwargs(cache),
            'stremio_type': addon.StremioMediaType.series,
            'warm_ids': ['plexio:rk-1', 'plexio:rk-2', 'plexio:rk-3'],
        }

        async def scenario():
            resolve = mock.AsyncMock(return_value=[media])
            patcher = mock.patch.object(addon, '_resolve_stream_media', new=resolve)
            with patcher:
                tasks = addon._schedule_stream_prewarm(
                    **{**kwargs, 'http': http}
                )
                await asyncio.gather(*tasks)

        self._run(scenario())
        # three 7.1-triggering rows share one transcode budget: one kick total
        self.assertEqual(http.get.call_count, 1)
        for i in (1, 2, 3):
            self.assertIn(
                self._warm_key(f'plexio:rk-{i}', 'series'), cache._store
            )


if __name__ == '__main__':
    unittest.main()
