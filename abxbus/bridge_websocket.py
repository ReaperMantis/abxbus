"""WebSocket bridge for forwarding events between runtimes.

Optional dependency: websockets>=12

Two classes are provided:

WebSocketEventBridge - local server and/or client connection modes.
  - Server mode (listen_on='ws://host:port'): starts a local WebSocket server that
    accepts client connections and broadcasts outbound events to all connected clients.
  - Client mode (send_to='ws://host:port'): connects to an external WebSocket server,
    maintains a persistent connection with automatic reconnect, and sends outbound
    events over that connection.
  Both modes can be active simultaneously on the same bridge instance.

WebSocketRelayEventBridge - external relay server mode.
  Both Python and browser clients connect to the same relay server as WebSocket clients.
  The relay server broadcasts all received messages to all connected clients on the same
  channel (identified by the URL path).

  Compatible relay servers:
  - Any WebSocket broadcast relay (custom or third-party)
  - Centrifugo (wss://host/connection/websocket with its own envelope protocol)
  - A minimal custom relay - see the "Minimal relay server" section below

Usage:
    # Server mode - browser clients connect to this
    bridge = WebSocketEventBridge(listen_on='ws://0.0.0.0:8765')
    bridge.on('*', bus.emit)
    await bridge.start()

    # Client mode - connects to an external WebSocket server
    bridge = WebSocketEventBridge(send_to='ws://peer.example.com:8765')
    bridge.on('*', bus.emit)
    await bridge.start()

    # Bidirectional with a bus
    bridge = WebSocketEventBridge(
        send_to='ws://peer.example.com:8765',
        listen_on='ws://0.0.0.0:8765',
    )
    bus.on('*', bridge.emit)
    bridge.on('*', bus.emit)

    # Relay bridge - both sides connect to an external relay
    bridge = WebSocketRelayEventBridge('ws://relay.example.com:8765/abxbus_events')
    bus.on('*', bridge.emit)
    bridge.on('*', bus.emit)
    await bridge.start()

Minimal relay server (Python, websockets>=12):
    import asyncio, websockets

    channels: dict[str, set] = {}

    async def handler(ws):
        path = ws.request.path
        channels.setdefault(path, set()).add(ws)
        try:
            async for message in ws:
                peers = channels.get(path, set()) - {ws}
                for peer in peers:
                    await peer.send(message)
        finally:
            channels.get(path, set()).discard(ws)

    async def main():
        async with websockets.serve(handler, '0.0.0.0', 8765):
            await asyncio.Future()

    asyncio.run(main())
"""

from __future__ import annotations

import asyncio
import importlib
import json
import logging
from collections.abc import Callable
from typing import Any
from urllib.parse import urlparse

from uuid_extensions import uuid7str

from abxbus.base_event import BaseEvent
from abxbus.event_bus import EventBus, EventPatternType, in_handler_context
from abxbus.helpers import QueueShutDown

logger = logging.getLogger('abxbus.bridge_websocket')

_WS_RECONNECT_DELAY = 1.0
_WS_CONNECT_TIMEOUT = 10.0
_DEFAULT_CHANNEL = 'abxbus_events'


def _load_websockets() -> Any:
    """Lazy-load the optional websockets dependency (shared by both bridge classes)."""
    try:
        return importlib.import_module('websockets')
    except ModuleNotFoundError as exc:
        raise RuntimeError('WebSocket bridges require optional dependency: pip install websockets') from exc


def _decode_ws_message(message: Any) -> Any:
    """Decode a WebSocket text or binary frame to a parsed JSON payload."""
    raw = message if isinstance(message, str) else message.decode('utf-8')
    return json.loads(raw)


def _parse_ws_url(url: str) -> tuple[str, str, int, str]:
    """Validate and parse a ws:// or wss:// URL into (scheme, host, port, path)."""
    parsed = urlparse(url)
    scheme = parsed.scheme.lower()
    if scheme not in ('ws', 'wss'):
        raise ValueError(f'WebSocket URL must use ws:// or wss://, got: {url}')
    if not parsed.hostname:
        raise ValueError(f'WebSocket URL missing hostname: {url}')
    default_port = 443 if scheme == 'wss' else 80
    port = parsed.port if parsed.port is not None else default_port
    path = parsed.path or '/'
    return scheme, parsed.hostname, port, path


def _parse_relay_url(url: str) -> tuple[str, str]:
    """Parse a ws:// or wss:// relay URL, returning (normalized_url, channel).

    The URL path segment is used as the channel name. If no path is given,
    defaults to 'abxbus_events'.
    """
    scheme, host, port, path = _parse_ws_url(url)
    channel = path.strip('/') or _DEFAULT_CHANNEL
    default_port = 443 if scheme == 'wss' else 80
    port_part = f':{port}' if port != default_port else ''
    normalized = f'{scheme}://{host}{port_part}/{channel}'
    return normalized, channel


class WebSocketEventBridge:
    """WebSocket bridge with optional local server and/or outbound client connection."""

    def __init__(
        self,
        send_to: str | None = None,
        listen_on: str | None = None,
        *,
        name: str | None = None,
    ):
        if send_to is None and listen_on is None:
            raise ValueError('WebSocketEventBridge requires at least one of send_to or listen_on')
        if send_to is not None:
            _parse_ws_url(send_to)
        if listen_on is not None:
            _parse_ws_url(listen_on)
        self.send_to = send_to
        self.listen_on = listen_on
        self._inbound_bus = EventBus(name=name or f'WebSocketEventBridge_{uuid7str()[-8:]}', max_history_size=0)

        self._running = False
        self._start_task: asyncio.Task[None] | None = None
        self._start_lock = asyncio.Lock()

        # Server mode state
        self._server: Any | None = None
        self._server_clients: set[Any] = set()
        self._server_clients_lock = asyncio.Lock()

        # Client mode state
        self._client_ws: Any | None = None
        self._client_task: asyncio.Task[None] | None = None
        self._client_connected: asyncio.Event = asyncio.Event()

    def on(self, event_pattern: EventPatternType, handler: Callable[[BaseEvent[Any]], Any]) -> None:
        self._ensure_started()
        self._inbound_bus.on(event_pattern, handler)

    async def emit(self, event: BaseEvent[Any]) -> BaseEvent[Any] | None:
        self._ensure_started()
        payload = json.dumps(event.model_dump(mode='json'), separators=(',', ':'))

        if self.send_to is not None:
            await self._client_send(payload)
        else:
            await self._server_broadcast(payload)

        if in_handler_context():
            return None
        return event

    async def dispatch(self, event: BaseEvent[Any]) -> BaseEvent[Any] | None:
        return await self.emit(event)

    async def start(self) -> None:
        current_task = asyncio.current_task()
        if self._start_task is not None and self._start_task is not current_task and not self._start_task.done():
            await self._start_task
            return

        if self._running:
            return

        try:
            async with self._start_lock:
                if self._running:
                    return

                websockets_module = _load_websockets()

                if self.listen_on is not None:
                    _, host, port, _ = _parse_ws_url(self.listen_on)
                    self._server = await websockets_module.serve(
                        self._handle_server_client,
                        host,
                        port,
                    )
                if self.send_to is not None:
                    try:
                        self._client_task = asyncio.create_task(
                            self._client_loop(),
                            name=f'WebSocketEventBridge-client-{uuid7str()[-8:]}',
                        )
                    except Exception:
                        if self._server is not None:
                            self._server.close()
                            await self._server.wait_closed()
                            self._server = None
                        raise

                self._running = True
        finally:
            if self._start_task is current_task:
                self._start_task = None

    async def close(self, *, clear: bool = True) -> None:
        if self._start_task is not None:
            self._start_task.cancel()
            await asyncio.gather(self._start_task, return_exceptions=True)
            self._start_task = None

        self._running = False
        self._client_connected.clear()

        if self._client_task is not None:
            self._client_task.cancel()
            await asyncio.gather(self._client_task, return_exceptions=True)
            self._client_task = None

        if self._client_ws is not None:
            try:
                await self._client_ws.close()
            except Exception:
                pass
            self._client_ws = None

        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

        self._server_clients.clear()

        await self._inbound_bus.destroy(clear=clear)

    def _ensure_started(self) -> None:
        if self._running:
            return
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return
        if self._start_task is None or self._start_task.done():
            self._start_task = asyncio.create_task(self.start())

    async def _server_broadcast(self, payload: str) -> None:
        async with self._server_clients_lock:
            clients = list(self._server_clients)
        if not clients:
            logger.debug('WebSocketEventBridge: no clients connected, event dropped')
            return
        for ws in clients:
            try:
                await ws.send(payload)
            except Exception:
                pass

    async def _client_send(self, payload: str) -> None:
        if not self._client_connected.is_set():
            try:
                await asyncio.wait_for(self._client_connected.wait(), timeout=_WS_CONNECT_TIMEOUT)
            except asyncio.TimeoutError:
                raise RuntimeError(f'WebSocketEventBridge: client not connected to {self.send_to}')
        ws = self._client_ws
        if ws is None:
            raise RuntimeError(f'WebSocketEventBridge: client not connected to {self.send_to}')
        await ws.send(payload)

    async def _handle_server_client(self, ws: Any) -> None:
        async with self._server_clients_lock:
            self._server_clients.add(ws)
        try:
            async for message in ws:
                if not self._running:
                    return
                try:
                    payload = _decode_ws_message(message)
                except Exception:
                    continue
                try:
                    await self._dispatch_inbound_payload(payload)
                except QueueShutDown:
                    return
        finally:
            async with self._server_clients_lock:
                self._server_clients.discard(ws)

    async def _client_loop(self) -> None:
        websockets_module = _load_websockets()
        while self._running:
            try:
                async with websockets_module.connect(self.send_to) as ws:
                    self._client_ws = ws
                    self._client_connected.set()
                    try:
                        async for message in ws:
                            if not self._running:
                                return
                            try:
                                payload = _decode_ws_message(message)
                            except Exception:
                                continue
                            try:
                                await self._dispatch_inbound_payload(payload)
                            except QueueShutDown:
                                return
                    finally:
                        self._client_connected.clear()
                        self._client_ws = None
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.debug('WebSocketEventBridge: connection lost (%s): %s', self.send_to, exc)
                self._client_connected.clear()
                self._client_ws = None

            if self._running:
                await asyncio.sleep(_WS_RECONNECT_DELAY)

    async def _dispatch_inbound_payload(self, payload: Any) -> None:
        event = BaseEvent[Any].model_validate(payload).event_reset()
        self._inbound_bus.emit(event)


class WebSocketRelayEventBridge:
    """WebSocket relay bridge - both sides connect to an external relay server as clients."""

    def __init__(self, relay_url: str, *, name: str | None = None):
        self.url, self.channel = _parse_relay_url(relay_url)
        self._inbound_bus = EventBus(name=name or f'WebSocketRelayEventBridge_{uuid7str()[-8:]}', max_history_size=0)

        self._running = False
        self._start_task: asyncio.Task[None] | None = None
        self._start_lock = asyncio.Lock()
        self._listener_task: asyncio.Task[None] | None = None
        self._ws: Any | None = None
        self._ws_connected: asyncio.Event = asyncio.Event()

    def on(self, event_pattern: EventPatternType, handler: Callable[[BaseEvent[Any]], Any]) -> None:
        self._ensure_started()
        self._inbound_bus.on(event_pattern, handler)

    async def emit(self, event: BaseEvent[Any]) -> BaseEvent[Any] | None:
        self._ensure_started()

        if not self._ws_connected.is_set():
            if not self._running:
                await self.start()
            try:
                await asyncio.wait_for(self._ws_connected.wait(), timeout=_WS_CONNECT_TIMEOUT)
            except asyncio.TimeoutError:
                raise RuntimeError(f'WebSocketRelayEventBridge: not connected to {self.url}')

        ws = self._ws
        if ws is None:
            raise RuntimeError(f'WebSocketRelayEventBridge: not connected to {self.url}')

        payload = event.model_dump(mode='json')
        await ws.send(json.dumps(payload, separators=(',', ':')))

        if in_handler_context():
            return None
        return event

    async def dispatch(self, event: BaseEvent[Any]) -> BaseEvent[Any] | None:
        return await self.emit(event)

    async def start(self) -> None:
        current_task = asyncio.current_task()
        if self._start_task is not None and self._start_task is not current_task and not self._start_task.done():
            await self._start_task
            return

        if self._running:
            return

        try:
            async with self._start_lock:
                if self._running:
                    return

                websockets_module = _load_websockets()
                ws = await websockets_module.connect(self.url)
                self._ws = ws
                self._ws_connected.set()
                self._running = True

                if self._listener_task is None or self._listener_task.done():
                    self._listener_task = asyncio.create_task(
                        self._listen_loop(),
                        name=f'WebSocketRelayEventBridge-listener-{uuid7str()[-8:]}',
                    )
        finally:
            if self._start_task is current_task:
                self._start_task = None

    async def close(self, *, clear: bool = True) -> None:
        if self._start_task is not None:
            self._start_task.cancel()
            await asyncio.gather(self._start_task, return_exceptions=True)
            self._start_task = None

        self._running = False
        self._ws_connected.clear()

        if self._listener_task is not None:
            self._listener_task.cancel()
            await asyncio.gather(self._listener_task, return_exceptions=True)
            self._listener_task = None

        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

        await self._inbound_bus.destroy(clear=clear)

    def _ensure_started(self) -> None:
        if self._running:
            return
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return
        if self._start_task is None or self._start_task.done():
            self._start_task = asyncio.create_task(self.start())

    async def _listen_loop(self) -> None:
        """Maintain the relay connection and dispatch inbound messages, with reconnect."""
        websockets_module = _load_websockets()
        while self._running:
            ws = self._ws
            if ws is None:
                # Reconnect after a dropped connection
                try:
                    ws = await websockets_module.connect(self.url)
                    self._ws = ws
                    self._ws_connected.set()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.debug('WebSocketRelayEventBridge: reconnect failed (%s): %s', self.url, exc)
                    if self._running:
                        await asyncio.sleep(_WS_RECONNECT_DELAY)
                    continue

            try:
                async for message in ws:
                    if not self._running:
                        return
                    try:
                        payload = _decode_ws_message(message)
                    except Exception:
                        continue
                    try:
                        await self._dispatch_inbound_payload(payload)
                    except QueueShutDown:
                        return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.debug('WebSocketRelayEventBridge: connection lost (%s): %s', self.url, exc)
            finally:
                self._ws_connected.clear()
                self._ws = None

            if self._running:
                await asyncio.sleep(_WS_RECONNECT_DELAY)

    async def _dispatch_inbound_payload(self, payload: Any) -> None:
        event = BaseEvent[Any].model_validate(payload).event_reset()
        self._inbound_bus.emit(event)
