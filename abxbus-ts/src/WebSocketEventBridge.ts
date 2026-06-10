/**    
 * WebSocket bridge for forwarding events between runtimes.    
 *    
 * Optional dependency (Node.js server mode only): ws    
 * Browser environments use the native WebSocket API for client mode.    
 *    
 * Two classes are provided:    
 *    
 * WebSocketEventBridge - client connection and/or local server modes.    
 *   - Client mode (send_to='ws://host:port'): connects to an external WebSocket server,    
 *     maintains a persistent connection with automatic reconnect, and sends outbound    
 *     events over that connection. Works in both Node.js and browser.    
 *   - Server mode (listen_on='ws://host:port'): starts a local WebSocket server that    
 *     accepts client connections and broadcasts outbound events to all connected clients.    
 *     Server mode is only supported in Node.js runtimes (requires the `ws` package).    
 *   Both modes can be active simultaneously on the same bridge instance.    
 *    
 * WebSocketRelayEventBridge - external relay server mode.    
 *   Bridge clients connect to the same relay server as WebSocket clients.    
 *   The relay server broadcasts all received messages to all connected clients on the same    
 *   channel (identified by the URL path). Works in both Node.js and browser.    
 *    
 * Usage:    
 *   // Client mode - connects to an external WebSocket server    
 *   const bridge = new WebSocketEventBridge({ send_to: 'ws://peer.example.com:8765' })    
 *   bridge.on('*', (event) => bus.emit(event))    
 *   await bridge.start()    
 *    
 *   // Server mode - browser clients connect to this (Node.js only)    
 *   const bridge = new WebSocketEventBridge({ listen_on: 'ws://0.0.0.0:8765' })    
 *   bridge.on('*', (event) => bus.emit(event))    
 *   await bridge.start()    
 *    
 *   // Bidirectional with a bus    
 *   const bridge = new WebSocketEventBridge({    
 *     send_to: 'ws://peer.example.com:8765',    
 *     listen_on: 'ws://0.0.0.0:8765',    
 *   })    
 *   bus.on('*', (event) => bridge.emit(event))    
 *   bridge.on('*', (event) => bus.emit(event))    
 *    
 *   // Relay bridge - both sides connect to an external relay    
 *   const bridge = new WebSocketRelayEventBridge('ws://relay.example.com:8765/abxbus_events')    
 *   bus.on('*', (event) => bridge.emit(event))    
 *   bridge.on('*', (event) => bus.emit(event))    
 *   await bridge.start()    
 */    
    
import { BaseEvent } from './BaseEvent.js'    
import { EventBus } from './EventBus.js'    
import { Deferred, withResolvers } from './LockManager.js'  
import { assertOptionalDependencyAvailable, importOptionalDependency, isNodeRuntime } from './optional_deps.js'    
import type { EventClass, EventHandlerCallable, EventPattern, UntypedEventHandlerFunction } from './types.js'    
    
// ─── Constants ───────────────────────────────────────────────────────────────    
    
const randomSuffix = (): string => Math.random().toString(36).slice(2, 10)    
const WS_RECONNECT_DELAY_MS = 1_000    
const WS_CONNECT_TIMEOUT_MS = 10_000    
const DEFAULT_CHANNEL = 'abxbus_events'    
    
// ─── URL helpers ─────────────────────────────────────────────────────────────    
    
type ParsedWsUrl = {    
  raw: string    
  scheme: 'ws' | 'wss'    
  host: string    
  port: number    
  path: string    
}    
    
function parseWsUrl(url: string): ParsedWsUrl {    
  let parsed: URL    
  try {    
    parsed = new URL(url)    
  } catch {    
    throw new Error(`WebSocket URL is invalid: ${url}`)    
  }    
  const scheme = parsed.protocol.replace(/:$/, '').toLowerCase()    
  if (scheme !== 'ws' && scheme !== 'wss') {    
    throw new Error(`WebSocket URL must use ws:// or wss://, got: ${url}`)    
  }    
  if (!parsed.hostname) {    
    throw new Error(`WebSocket URL missing hostname: ${url}`)    
  }    
  const default_port = scheme === 'wss' ? 443 : 80    
  const port = parsed.port ? Number(parsed.port) : default_port    
  const path = parsed.pathname || '/'    
  return { raw: url, scheme: scheme as 'ws' | 'wss', host: parsed.hostname, port, path }    
}    
    
function parseRelayUrl(url: string): { normalized: string; channel: string } {    
  const { scheme, host, port, path } = parseWsUrl(url)    
  const channel = path.replace(/^\/+|\/+$/g, '') || DEFAULT_CHANNEL    
  const default_port = scheme === 'wss' ? 443 : 80    
  const port_part = port !== default_port ? `:${port}` : ''    
  const normalized = `${scheme}://${host}${port_part}/${channel}`    
  return { normalized, channel }    
}    
    
// ─── WebSocket transport helpers ─────────────────────────────────────────────    
    
/** Lazy-load the `ws` npm package (Node.js only). */    
async function importWs(): Promise<any> {    
  return importOptionalDependency('WebSocketEventBridge', 'ws')    
}    
    
/**    
 * Open a WebSocket client connection.    
 * Uses the native browser WebSocket API when available, otherwise the `ws` package.    
 * Resolves with the connected socket or rejects on error.    
 */    
async function openWebSocket(url: string): Promise<any> {    
  const NativeWS = (globalThis as { WebSocket?: typeof WebSocket }).WebSocket    
  if (typeof NativeWS === 'function') {    
    // Browser path - use native WebSocket    
    return new Promise<WebSocket>((resolve, reject) => {    
      const ws = new NativeWS(url)    
      ws.addEventListener('open', () => resolve(ws), { once: true })    
      ws.addEventListener('error', () => reject(new Error(`WebSocket connection failed: ${url}`)), { once: true })    
    })    
  }    
    
  // Node.js path - use `ws` package    
  const mod = await importWs()    
  const WS: new (url: string) => any = mod.default ?? mod.WebSocket ?? mod    
  return new Promise<any>((resolve, reject) => {    
    const ws = new WS(url)    
    ws.once('open', () => resolve(ws))    
    ws.once('error', (err: unknown) => reject(err))    
  })    
}    
    
/**    
 * Returns true if the socket uses the EventEmitter API (ws package),    
 * false if it uses the browser addEventListener API.    
 */    
function isNodeWs(ws: any): boolean {    
  return typeof ws.on === 'function' && typeof ws.once === 'function'    
}    
    
/** Attach a persistent message listener that calls `onMessage` for each received text frame. */    
function attachMessageListener(ws: any, onMessage: (data: string) => void): void {    
  if (isNodeWs(ws)) {    
    ws.on('message', (data: Buffer | string) => {    
      onMessage(typeof data === 'string' ? data : data.toString('utf8'))    
    })    
  } else {    
    ws.addEventListener('message', (ev: MessageEvent) => {    
      if (typeof ev.data === 'string') onMessage(ev.data)    
    })    
  }    
}    
    
/** Attach a one-shot close listener. */    
function attachCloseListener(ws: any, onClose: () => void): void {    
  if (isNodeWs(ws)) {    
    ws.once('close', onClose)    
  } else {    
    ws.addEventListener('close', onClose, { once: true })    
  }    
}    
    
/** Attach a one-shot error listener. */    
function attachErrorListener(ws: any, onError: (err: unknown) => void): void {    
  if (isNodeWs(ws)) {    
    ws.once('error', onError)    
  } else {    
    ws.addEventListener('error', (ev: Event) => onError(ev), { once: true })    
  }    
}    
    
/** Use callback form for ws package to surface send errors. */  
function wsSend(ws: any, payload: string): void {    
  if (isNodeWs(ws)) {    
    ws.send(payload, (err?: Error) => {    
      if (err) console.debug('[abxbus] WebSocketEventBridge: send error', err)    
    })    
  } else {    
    ws.send(payload)    
  }    
}    
  
/**  
 * Race a promise against a connect timeout.  
 * Clears the timer whether the promise resolves or rejects, preventing timer leaks.  
 */  
async function raceWithConnectTimeout<T>(promise: Promise<T>, label: string): Promise<T> {  
  let timeout_id: ReturnType<typeof setTimeout>  
  const timeout_promise = new Promise<never>((_, reject) => {  
    timeout_id = setTimeout(  
      () => reject(new Error(`${label}: connect timeout`)),  
      WS_CONNECT_TIMEOUT_MS,  
    )  
  })  
  try {  
    return await Promise.race([promise, timeout_promise])  
  } finally {  
    clearTimeout(timeout_id!)  
  }  
}  
  
/** Dispatch a raw inbound JSON payload onto a bus. */  
function dispatchInboundPayload(bus: EventBus, payload: unknown): void {  
  const event = BaseEvent.fromJSON(payload).eventReset()  
  bus.emit(event)  
}  
  
// ─── WebSocketEventBridge ────────────────────────────────────────────────────    
    
export type WebSocketEventBridgeOptions = {    
  send_to?: string | null    
  listen_on?: string | null    
  name?: string    
}    
    
export class WebSocketEventBridge {    
  readonly send_to: ParsedWsUrl | null    
  readonly listen_on: ParsedWsUrl | null    
  readonly name: string    
    
  private readonly inbound_bus: EventBus    
  private running: boolean    
  private start_promise: Promise<void> | null    
    
  // Server mode state (Node.js only)    
  private ws_server: any | null    
  private server_clients: Set<any>    
    
  // Client mode state    
  private client_ws: any | null    
  private client_connected: Deferred<void> | null    
  private client_loop_promise: Promise<void> | null    
    
  constructor(options: WebSocketEventBridgeOptions)    
  constructor(send_to?: string | null, listen_on?: string | null, name?: string)    
  constructor(    
    send_to_or_options?: string | null | WebSocketEventBridgeOptions,    
    listen_on?: string | null,    
    name?: string,    
  ) {    
    const options: WebSocketEventBridgeOptions =    
      typeof send_to_or_options === 'object' && send_to_or_options !== null    
        ? send_to_or_options    
        : { send_to: send_to_or_options ?? undefined, listen_on: listen_on ?? undefined, name }    
    
    if (!options.send_to && !options.listen_on) {    
      throw new Error('WebSocketEventBridge requires at least one of send_to or listen_on')    
    }    
    
    this.send_to = options.send_to ? parseWsUrl(options.send_to) : null    
    this.listen_on = options.listen_on ? parseWsUrl(options.listen_on) : null    
    this.name = options.name ?? `WebSocketEventBridge_${randomSuffix()}`    
    this.inbound_bus = new EventBus(this.name, { max_history_size: 0 })    
    this.running = false    
    this.start_promise = null    
    this.ws_server = null    
    this.server_clients = new Set()    
    this.client_ws = null    
    this.client_connected = null    
    this.client_loop_promise = null    
    
    // Assert ws is available at construction time for server mode    
    if (this.listen_on && isNodeRuntime()) {    
      assertOptionalDependencyAvailable('WebSocketEventBridge', 'ws')    
    }    
    
    if (this.listen_on && !isNodeRuntime()) {    
      throw new Error(`${this.constructor.name} listen_on is only supported in Node.js runtimes`)    
    }    
    
    this.dispatch = this.dispatch.bind(this)    
    this.emit = this.emit.bind(this)    
    this.on = this.on.bind(this)    
  }    
    
  on<T extends BaseEvent>(event_pattern: EventClass<T>, handler: EventHandlerCallable<T>): void    
  on<T extends BaseEvent>(event_pattern: string | '*', handler: UntypedEventHandlerFunction<T>): void    
  on(event_pattern: EventPattern | '*', handler: EventHandlerCallable | UntypedEventHandlerFunction): void {    
    this.ensureStarted()    
    if (typeof event_pattern === 'string') {    
      this.inbound_bus.on(event_pattern, handler as UntypedEventHandlerFunction<BaseEvent>)    
      return    
    }    
    this.inbound_bus.on(event_pattern as EventClass<BaseEvent>, handler as EventHandlerCallable<BaseEvent>)    
  }    
    
  async emit<T extends BaseEvent>(event: T): Promise<void> {    
    this.ensureStarted()    
    const payload = JSON.stringify(event.toJSON())    
  
    if (this.send_to) {    
      await this.clientSend(payload)    
    }    
    if (this.listen_on) {    
      this.serverBroadcast(payload)    
    }    
  }    
    
  async dispatch<T extends BaseEvent>(event: T): Promise<void> {    
    return this.emit(event)    
  }    
    
  async start(): Promise<void> {    
    if (this.running) return    
    if (this.start_promise) {    
      await this.start_promise    
      return    
    }    
    
    this.start_promise = (async () => {    
      if (this.listen_on) {    
        await this.startServer(this.listen_on)    
      }  
      this.running = true  
      if (this.send_to) {  
        // Fire-and-forget: loop runs in the background    
        this.client_connected = withResolvers<void>()    
        this.client_loop_promise = this.runClientLoop(this.send_to.raw)    
      }    
    })()    
    
    try {    
      await this.start_promise    
    } finally {    
      this.start_promise = null    
    }    
  }    
    
  async close(): Promise<void> {    
    if (this.start_promise) {    
      await this.start_promise.catch(() => {})    
    }    
    
    this.running = false    
    
    // Unblock any callers waiting in clientSend()    
    if (this.client_connected) {    
      this.client_connected.reject(new Error('WebSocketEventBridge closed'))    
      this.client_connected = null    
    }    
    
    // Close client connection (triggers close event, ending the loop)    
    if (this.client_ws) {    
      try { this.client_ws.close() } catch { /* ignore */ }    
      this.client_ws = null    
    }    
    
    if (this.client_loop_promise) {    
      await this.client_loop_promise.catch(() => {})    
      this.client_loop_promise = null    
    }    
    
    // Terminate all server clients before closing so ws_server.close() doesn't hang    
    if (this.ws_server) {    
      for (const ws of this.server_clients) {    
        try { ws.terminate() } catch { /* ignore */ }    
      }    
      this.server_clients.clear()    
      await new Promise<void>((resolve) => this.ws_server.close(() => resolve()))    
      this.ws_server = null    
    }    
    
    this.inbound_bus.destroy()    
  }    
    
  private ensureStarted(): void {    
    if (this.running) return    
    if (this.start_promise) return    
    void this.start().catch((error: unknown) => {    
      console.error('[abxbus] WebSocketEventBridge failed to start', error)    
    })    
  }    
    
  private async clientSend(payload: string): Promise<void> {    
    if (!this.client_ws) {    
      if (!this.client_connected) {    
        throw new Error(`WebSocketEventBridge: client not connected to ${this.send_to?.raw}`)    
      }
      await raceWithConnectTimeout(  
        this.client_connected.promise,  
        `WebSocketEventBridge to ${this.send_to?.raw}`,  
      )  
    }    
    const ws = this.client_ws    
    if (!ws) throw new Error(`WebSocketEventBridge: client not connected to ${this.send_to?.raw}`)    
    wsSend(ws, payload)    
  }    
    
  private serverBroadcast(payload: string): void {    
    const clients = [...this.server_clients]    
    if (clients.length === 0) {    
      console.debug('[abxbus] WebSocketEventBridge: no clients connected, event dropped')    
      return    
    }    
    for (const ws of clients) {    
      try { wsSend(ws, payload) } catch { /* ignore individual client errors */ }    
    }    
  }    
    
  private async startServer(endpoint: ParsedWsUrl): Promise<void> {    
    const mod = await importWs()    
    const WebSocketServer: new (opts: object) => any =    
      mod.WebSocketServer ?? mod.Server ?? mod.default?.WebSocketServer ?? mod.default?.Server    
    if (!WebSocketServer) {    
      throw new Error('WebSocketEventBridge: could not find WebSocketServer in ws package')    
    }    
    
    const server = new WebSocketServer({ host: endpoint.host, port: endpoint.port })    
    
    // Assign ws_server only after the server is confirmed listening    
    await new Promise<void>((resolve, reject) => {    
      server.once('listening', () => {    
        this.ws_server = server    
        resolve()    
      })    
      server.once('error', reject)    
    })    
    
    server.on('connection', (ws: any) => {    
      this.server_clients.add(ws)    
      attachMessageListener(ws, (raw) => {    
        if (!this.running) return    
        try {    
          const payload: unknown = JSON.parse(raw)    
          dispatchInboundPayload(this.inbound_bus, payload)  
        } catch { /* ignore malformed frames */ }    
      })    
      const cleanup = () => this.server_clients.delete(ws)    
      attachCloseListener(ws, cleanup)    
      attachErrorListener(ws, cleanup)    
    })    
  }    
    
  private async runClientLoop(url: string): Promise<void> {    
    while (this.running) {    
      let connected = false    
      try {    
        const ws = await openWebSocket(url)    
        if (!this.running) {    
          try { ws.close() } catch { /* ignore */ }    
          return    
        }    
        this.client_ws = ws    
        connected = true    
        // Unblock any callers waiting in clientSend()    
        this.client_connected?.resolve()    
    
        await new Promise<void>((resolve) => {    
          attachMessageListener(ws, (raw) => {    
            if (!this.running) { ws.close(); resolve(); return }    
            try {    
              const payload: unknown = JSON.parse(raw)    
              dispatchInboundPayload(this.inbound_bus, payload)  
            } catch { /* ignore malformed frames */ }    
          })    
          attachCloseListener(ws, resolve)    
          attachErrorListener(ws, () => resolve())    
        })    
      } catch (err) {    
        console.debug('[abxbus] WebSocketEventBridge: connection failed', url, err)    
      } finally {    
        this.client_ws = null    
        // Reject the old deferred so waiting callers fail fast instead of timing out    
        if (this.running && connected) {    
          const old = this.client_connected    
          this.client_connected = withResolvers<void>()    
          old?.reject(new Error('WebSocketEventBridge: connection lost, reconnecting'))    
        }    
      }    
    
      if (this.running) {    
        await new Promise<void>((resolve) => setTimeout(resolve, WS_RECONNECT_DELAY_MS))    
      }    
    }    
  }    
}    
    
// ─── WebSocketRelayEventBridge ───────────────────────────────────────────────    
    
export class WebSocketRelayEventBridge {    
  readonly url: string    
  readonly channel: string    
  readonly name: string    
    
  private readonly inbound_bus: EventBus    
  private running: boolean    
  private start_promise: Promise<void> | null    
  private ws: any | null    
  private ws_connected: Deferred<void> | null    
  private listener_promise: Promise<void> | null    
    
  constructor(relay_url: string, name?: string) {    
    const { normalized, channel } = parseRelayUrl(relay_url)    
    this.url = normalized    
    this.channel = channel    
    this.name = name ?? `WebSocketRelayEventBridge_${randomSuffix()}`    
    this.inbound_bus = new EventBus(this.name, { max_history_size: 0 })    
    this.running = false    
    this.start_promise = null    
    this.ws = null    
    this.ws_connected = null    
    this.listener_promise = null    
    
    this.dispatch = this.dispatch.bind(this)    
    this.emit = this.emit.bind(this)    
    this.on = this.on.bind(this)    
  }    
    
  on<T extends BaseEvent>(event_pattern: EventClass<T>, handler: EventHandlerCallable<T>): void    
  on<T extends BaseEvent>(event_pattern: string | '*', handler: UntypedEventHandlerFunction<T>): void    
  on(event_pattern: EventPattern | '*', handler: EventHandlerCallable | UntypedEventHandlerFunction): void {    
    this.ensureStarted()    
    if (typeof event_pattern === 'string') {    
      this.inbound_bus.on(event_pattern, handler as UntypedEventHandlerFunction<BaseEvent>)    
      return    
    }    
    this.inbound_bus.on(event_pattern as EventClass<BaseEvent>, handler as EventHandlerCallable<BaseEvent>)    
  }    
    
  async emit<T extends BaseEvent>(event: T): Promise<void> {    
    this.ensureStarted()    
    
    if (!this.ws) {    
      if (this.start_promise) await this.start_promise.catch(() => {})    
      if (!this.ws_connected) {    
        throw new Error(`WebSocketRelayEventBridge: not connected to ${this.url}`)    
      }    
      await raceWithConnectTimeout(  
        this.ws_connected.promise,  
        `WebSocketRelayEventBridge to ${this.url}`,  
      )  
    }    
    
    const ws = this.ws    
    if (!ws) throw new Error(`WebSocketRelayEventBridge: not connected to ${this.url}`)    
    wsSend(ws, JSON.stringify(event.toJSON()))    
  }    
    
  async dispatch<T extends BaseEvent>(event: T): Promise<void> {    
    return this.emit(event)    
  }    
    
  async start(): Promise<void> {    
    if (this.running) return    
    if (this.start_promise) {    
      await this.start_promise    
      return    
    }    
    
    this.start_promise = (async () => {    
      this.running = true  
      this.ws_connected = withResolvers<void>()    
      this.listener_promise = this.runListenLoop()    
      try {    
        await raceWithConnectTimeout(  
          this.ws_connected.promise,  
          `WebSocketRelayEventBridge to ${this.url}`,  
        )  
      } catch (err) {    
        this.running = false    
        throw err    
      }    
    })()    
    
    try {    
      await this.start_promise    
    } finally {    
      this.start_promise = null    
    }    
  }    
    
  async close(): Promise<void> {    
    if (this.start_promise) {    
      await this.start_promise.catch(() => {})    
    }    
    
    this.running = false    
    
    // Unblock any callers waiting in emit()    
    if (this.ws_connected) {    
      this.ws_connected.reject(new Error('WebSocketRelayEventBridge closed'))    
      this.ws_connected = null    
    }    
    
    // Close connection (triggers close event, ending the loop)    
    if (this.ws) {    
      try { this.ws.close() } catch { /* ignore */ }    
      this.ws = null    
    }    
    
    if (this.listener_promise) {    
      await this.listener_promise.catch(() => {})    
      this.listener_promise = null    
    }    
    
    this.inbound_bus.destroy()    
  }    
    
  private ensureStarted(): void {    
    if (this.running) return    
    if (this.start_promise) return    
    void this.start().catch((error: unknown) => {    
      console.error('[abxbus] WebSocketRelayEventBridge failed to start', error)    
    })    
  }    
    
  private async runListenLoop(): Promise<void> {    
    while (this.running) {    
      let connected = false    
      try {    
        const ws = await openWebSocket(this.url)    
        if (!this.running) {    
          try { ws.close() } catch { /* ignore */ }    
          return    
        }    
        this.ws = ws    
        connected = true    
        // Unblock any callers waiting in emit()    
        this.ws_connected?.resolve()    
    
        await new Promise<void>((resolve) => {    
          attachMessageListener(ws, (raw) => {    
            if (!this.running) { ws.close(); resolve(); return }    
            try {    
              const payload: unknown = JSON.parse(raw)    
              dispatchInboundPayload(this.inbound_bus, payload)  
            } catch { /* ignore malformed frames */ }    
          })    
          attachCloseListener(ws, resolve)    
          attachErrorListener(ws, () => resolve())    
        })    
      } catch (err) {    
        console.debug('[abxbus] WebSocketRelayEventBridge: connection failed', this.url, err)    
      } finally {    
        this.ws = null    
        // Reject the old deferred so waiting callers fail fast instead of timing out    
        if (this.running && connected) {    
          const old = this.ws_connected    
          this.ws_connected = withResolvers<void>()    
          old?.reject(new Error('WebSocketRelayEventBridge: connection lost, reconnecting'))    
        }    
      }    
    
      if (this.running) {    
        await new Promise<void>((resolve) => setTimeout(resolve, WS_RECONNECT_DELAY_MS))    
      }    
    }    
  }    
}