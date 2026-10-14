import { createHash } from "node:crypto"
import { readFile, rename, writeFile } from "node:fs/promises"
import WebSocket, { type RawData } from "ws"
import { SocketModeClient } from "@slack/socket-mode"
import { WebClient } from "@slack/web-api"

const OPTIONS_PATH = "/data/options.json"
const STATE_PATH = "/data/state.json"
const STATE_TEMP_PATH = "/data/state.json.tmp"
const HA_WEBSOCKET_URL = "ws://supervisor/core/websocket"

const SLACK_INTERVAL_MS = 1_100
const METADATA_REFRESH_MS = 5 * 60 * 1_000
const REPLAY_OVERLAP_SECONDS = 2
const RECENT_KEY_LIMIT = 5_000
const SECURITY_MESSAGE_PREFIX = "THEATRE armed"
const DOOR_RELEASE_MS = 2_000

interface Options {
  slack_app_token: string
  slack_bot_token: string
  slack_channel_id: string
  security_channel_id: string
  door_switch_entity_id: string
  initial_lookback_seconds: number
  include_area: boolean
}

interface PersistentState {
  lastWhen: number
  recentKeys: string[]
}

interface HassState {
  entity_id: string
  state: string
  attributes: Record<string, unknown>
  last_changed: string
  last_updated: string
}

interface HassConfig {
  time_zone?: string
}

interface EntityRegistryDisplayEntry {
  // entity id
  ei: string

  // area id
  ai?: string

  // device id
  di?: string

  // configured entity name
  en?: string
}

interface EntityRegistryDisplayResult {
  entities: EntityRegistryDisplayEntry[]
}

interface DeviceRegistryEntry {
  id: string
  area_id?: string | null
  name?: string | null
  name_by_user?: string | null
}

interface AreaRegistryEntry {
  area_id: string
  name: string
}

interface LogbookEntry {
  when: number

  entity_id?: string | null
  state?: string | null
  name?: string | null
  message?: string | null
  source?: string | null
  domain?: string | null
  icon?: string | null
  attributes?: Record<string, unknown>

  context_domain?: string | null
  context_name?: string | null
  context_entity_id?: string | null
  context_entity_id_name?: string | null
  context_message?: string | null
  context_source?: string | null
  context_service?: string | null
  context_user_id?: string | null
}

interface LogbookStreamEvent {
  events?: LogbookEntry[]
  start_time?: number
  end_time?: number
  partial?: boolean
}

interface SlackMessage {
  channel: string
  text: string
  blocks: unknown[]
  unfurl_links: boolean
  unfurl_media: boolean
}

interface WebSocketMessage {
  id?: number
  type: string
  success?: boolean
  result?: unknown
  error?: {
    code?: string
    message?: string
  }
  event?: unknown
  message?: string
}

interface PendingRequest {
  resolve: (value: unknown) => void
  reject: (error: Error) => void
}

const sleep = (milliseconds: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, milliseconds))

class HomeAssistantClient {
  private socket!: WebSocket
  private nextId = 1
  private authenticated = false

  private readonly pending = new Map<number, PendingRequest>()
  private readonly subscriptions = new Map<number, (event: unknown) => void>()

  private closeResolver!: () => void
  public readonly closed: Promise<void>

  public constructor(private readonly token: string) {
    this.closed = new Promise((resolve) => {
      this.closeResolver = resolve
    })
  }

  public async connect(): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      let connectionSettled = false

      const rejectConnection = (error: Error): void => {
        if (!connectionSettled) {
          connectionSettled = true
          reject(error)
        }
      }

      this.socket = new WebSocket(HA_WEBSOCKET_URL)

      this.socket.on("message", (raw: RawData) => {
        let message: WebSocketMessage

        try {
          message = JSON.parse(raw.toString()) as WebSocketMessage
        } catch (error) {
          console.error("Could not parse Home Assistant message", error)
          return
        }

        if (message.type === "auth_required") {
          this.socket.send(
            JSON.stringify({
              type: "auth",
              access_token: this.token,
            }),
          )

          return
        }

        if (message.type === "auth_ok") {
          this.authenticated = true

          if (!connectionSettled) {
            connectionSettled = true
            resolve()
          }

          return
        }

        if (message.type === "auth_invalid") {
          rejectConnection(
            new Error(
              `Home Assistant authentication failed; ${
                message.message ?? "unknown error"
              }`,
            ),
          )
          return
        }

        if (message.type === "result" && message.id !== undefined) {
          const pending = this.pending.get(message.id)

          if (!pending) {
            return
          }

          this.pending.delete(message.id)

          if (message.success) {
            pending.resolve(message.result)
          } else {
            pending.reject(
              new Error(
                `Home Assistant command failed: ${
                  message.error?.code ?? "unknown"
                }: ${message.error?.message ?? "unknown error"}`,
              ),
            )
          }

          return
        }

        if (message.type === "event" && message.id !== undefined) {
          this.subscriptions.get(message.id)?.(message.event)
        }
      })

      this.socket.on("error", (error) => {
        console.error("Home Assistant WebSocket error", error)

        if (!this.authenticated) {
          rejectConnection(error)
        }
      })

      this.socket.on("close", () => {
        if (!this.authenticated) {
          rejectConnection(
            new Error("Home Assistant WebSocket closed before authentication"),
          )
        }

        for (const request of this.pending.values()) {
          request.reject(new Error("Home Assistant WebSocket disconnected"))
        }

        this.pending.clear()
        this.subscriptions.clear()
        this.closeResolver()
      })
    })
  }

  public request<T>(
    type: string,
    payload: Record<string, unknown> = {},
  ): Promise<T> {
    const id = this.nextId++

    return new Promise<T>((resolve, reject) => {
      this.pending.set(id, {
        resolve: (value) => resolve(value as T),
        reject,
      })

      this.send({
        id,
        type,
        ...payload,
      })
    })
  }

  public subscribe<T>(
    type: string,
    payload: Record<string, unknown>,
    handler: (event: T) => void,
  ): Promise<void> {
    const id = this.nextId++
    this.subscriptions.set(id, (event) => handler(event as T))

    return new Promise<void>((resolve, reject) => {
      this.pending.set(id, {
        resolve: () => resolve(),
        reject: (error) => {
          this.subscriptions.delete(id)
          reject(error)
        },
      })

      this.send({
        id,
        type,
        ...payload,
      })
    })
  }

  private send(message: Record<string, unknown>): void {
    if (this.socket.readyState !== WebSocket.OPEN) {
      throw new Error("Home Assistant WebSocket is not open")
    }

    this.socket.send(JSON.stringify(message))
  }
}

class MetadataIndex {
  private readonly states = new Map<string, HassState>()
  private readonly entities = new Map<string, EntityRegistryDisplayEntry>()
  private readonly devices = new Map<string, DeviceRegistryEntry>()
  private readonly areas = new Map<string, AreaRegistryEntry>()

  public constructor(
    states: HassState[],
    entityRegistry: EntityRegistryDisplayResult,
    devices: DeviceRegistryEntry[],
    areas: AreaRegistryEntry[],
    public readonly timeZone: string,
  ) {
    for (const state of states) {
      this.states.set(state.entity_id, state)
    }

    for (const entity of entityRegistry.entities) {
      this.entities.set(entity.ei, entity)
    }

    for (const device of devices) {
      this.devices.set(device.id, device)
    }

    for (const area of areas) {
      this.areas.set(area.area_id, area)
    }
  }

  public getState(entityId: string | null | undefined): HassState | undefined {
    return entityId ? this.states.get(entityId) : undefined
  }

  public getFriendlyName(entry: LogbookEntry): string {
    if (entry.name) {
      return entry.name
    }

    if (entry.entity_id) {
      const state = this.states.get(entry.entity_id)
      const friendlyName = state?.attributes.friendly_name

      if (typeof friendlyName === "string" && friendlyName.length > 0) {
        return friendlyName
      }

      const registryName = this.entities.get(entry.entity_id)?.en

      if (registryName) {
        return registryName
      }

      return entry.entity_id
    }

    if (entry.domain) {
      return titleCase(entry.domain)
    }

    return "Home Assistant"
  }

  public getAreaName(entityId: string | null | undefined): string | undefined {
    if (!entityId) {
      return undefined
    }

    const entity = this.entities.get(entityId)

    if (!entity) {
      return undefined
    }

    const areaId =
      entity.ai ??
      (entity.di
        ? (this.devices.get(entity.di)?.area_id ?? undefined)
        : undefined)

    if (!areaId) {
      return undefined
    }

    return this.areas.get(areaId)?.name
  }
}

async function loadMetadata(
  client: HomeAssistantClient,
): Promise<MetadataIndex> {
  const [states, entityRegistry, devices, areas, config] = await Promise.all([
    client.request<HassState[]>("get_states"),
    client.request<EntityRegistryDisplayResult>(
      "config/entity_registry/list_for_display",
    ),
    client.request<DeviceRegistryEntry[]>("config/device_registry/list"),
    client.request<AreaRegistryEntry[]>("config/area_registry/list"),
    client.request<HassConfig>("get_config"),
  ])

  return new MetadataIndex(
    states,
    entityRegistry,
    devices,
    areas,
    config.time_zone ?? "UTC",
  )
}

class SlackPoster {
  private readonly queue: Array<{
    message: SlackMessage
    onSuccess: () => Promise<void>
  }> = []

  private readonly client: WebClient
  private draining = false

  public constructor(
    token: string,
    private readonly channel: string,
  ) {
    const normalizedToken = token.trim()

    if (!normalizedToken.startsWith("xoxb-")) {
      throw new Error("Slack token does not look like a Bot User OAuth Token")
    }

    this.client = new WebClient(normalizedToken)
  }

  public async testAuthentication(): Promise<void> {
    const result = await this.client.auth.test()

    console.log("Slack authentication succeeded", {
      team: result.team,
      user: result.user,
      botId: result.bot_id,
    })
  }

  public enqueue(message: SlackMessage, onSuccess: () => Promise<void>): void {
    this.queue.push({ message, onSuccess })
    void this.drain()
  }

  private async drain(): Promise<void> {
    if (this.draining) {
      return
    }

    this.draining = true

    try {
      while (this.queue.length > 0) {
        const job = this.queue[0]

        if (!job) {
          break
        }

        try {
          await this.post(job.message)
        } catch (error) {
          console.error(
            "Could not send Activity entry to Slack; retrying",
            error,
          )

          await sleep(5_000)
          continue
        }

        this.queue.shift()

        try {
          await job.onSuccess()
        } catch (error) {
          console.error("Could not persist Activity cursor", error)
        }

        await sleep(SLACK_INTERVAL_MS)
      }
    } finally {
      this.draining = false

      if (this.queue.length > 0) {
        void this.drain()
      }
    }
  }

  private async post(message: SlackMessage): Promise<void> {
    const result = await this.client.chat.postMessage({
      channel: this.channel,
      text: message.text,
      blocks: message.blocks as any,
      unfurl_links: message.unfurl_links,
      unfurl_media: message.unfurl_media,
    })

    console.log("Posted Activity entry to Slack", {
      channel: result.channel,
      timestamp: result.ts,
    })
  }
}

class SlackSecurityListener {
  private readonly client: SocketModeClient
  private pulse: Promise<void> = Promise.resolve()

  public constructor(
    appToken: string,
    private readonly channelId: string,
    private readonly switchEntityId: string,
    private readonly getHomeAssistantClient: () => HomeAssistantClient | undefined,
  ) {
    this.client = new SocketModeClient({
      appToken,
    })

    this.client.on("message", ({ event, ack }) => {
      void ack()

      if (
        event.channel !== this.channelId ||
        !event.text.startsWith(SECURITY_MESSAGE_PREFIX)
      ) {
        return
      }

      console.log("Received theatre arming request from Slack")
      this.enqueuePulse()
    })
  }

  public async start(): Promise<void> {
    await this.client.start()
    console.log("Slack Socket Mode connected for security channel")
  }

  private enqueuePulse(): void {
    this.pulse = this.pulse
      .catch(() => undefined)
      .then(async () => {
        const homeAssistant = this.getHomeAssistantClient()

        if (!homeAssistant) {
          throw new Error("Home Assistant is not connected")
        }

        console.log("Releasing theatre door magnet")
        await homeAssistant.request("call_service", {
          domain: "switch",
          service: "turn_off",
          service_data: { entity_id: this.switchEntityId },
        })

        try {
          await sleep(DOOR_RELEASE_MS)
        } finally {
          console.log("Restoring theatre door magnet")
          await homeAssistant.request("call_service", {
            domain: "switch",
            service: "turn_on",
            service_data: { entity_id: this.switchEntityId },
          })
        }
      })
      .catch((error) => {
        console.error("Could not operate theatre door magnet", error)
      })
  }
}

function formatSlackMessage(
  entry: LogbookEntry,
  metadata: MetadataIndex,
  options: Options,
): SlackMessage {
  const name = metadata.getFriendlyName(entry)
  const state = metadata.getState(entry.entity_id)
  const area = options.include_area
    ? metadata.getAreaName(entry.entity_id)
    : undefined

  const formattedState =
    entry.state !== null && entry.state !== undefined
      ? formatEntityState(entry, state)
      : undefined

  const plainHeadline = entry.message
    ? `${name} ${entry.message}`
    : formattedState
      ? `${name} -> ${formattedState}`
      : name

  const markdownHeadline = entry.message
    ? `*${escapeSlack(name)}* ${escapeSlack(entry.message)}`
    : formattedState
      ? `*${escapeSlack(name)}* -> ${escapeSlack(formattedState)}`
      : `*${escapeSlack(name)}*`

  const cause = formatCause(entry)

  const secondaryLines = [area, cause]
    .filter((value): value is string => Boolean(value))
    .map((value) => `_${escapeSlack(value)}_`)

  const messageBody = [markdownHeadline, ...secondaryLines].join("\n")

  return {
    channel: "",
    text: `${plainHeadline}${area ? ` — ${area}` : ""}`,
    blocks: [
      {
        type: "section",
        text: {
          type: "mrkdwn",
          text: messageBody,
        },
      },
    ],
    unfurl_links: false,
    unfurl_media: false,
  }
}

function formatEntityState(
  entry: LogbookEntry,
  state: HassState | undefined,
): string {
  const rawState = entry.state ?? ""
  const domain = entry.entity_id?.split(".", 1)[0] ?? entry.domain ?? ""

  const deviceClassValue =
    state?.attributes.device_class ?? entry.attributes?.device_class

  const deviceClass =
    typeof deviceClassValue === "string" ? deviceClassValue : ""

  if (domain === "binary_sensor") {
    const pairs: Record<string, [off: string, on: string]> = {
      occupancy: ["Clear", "Detected"],
      motion: ["Clear", "Detected"],
      presence: ["Clear", "Detected"],

      door: ["Closed", "Open"],
      window: ["Closed", "Open"],
      opening: ["Closed", "Open"],
      garage_door: ["Closed", "Open"],

      moisture: ["Dry", "Wet"],
      connectivity: ["Disconnected", "Connected"],

      smoke: ["Clear", "Detected"],
      gas: ["Clear", "Detected"],
      problem: ["Clear", "Detected"],
      safety: ["Clear", "Detected"],

      battery: ["Normal", "Low"],
      cold: ["Normal", "Cold"],
      heat: ["Normal", "Hot"],
    }

    const pair = pairs[deviceClass]

    if (pair && (rawState === "on" || rawState === "off")) {
      return rawState === "on" ? pair[1] : pair[0]
    }
  }

  const standardStates: Record<string, string> = {
    on: "On",
    off: "Off",
    open: "Open",
    closed: "Closed",
    opening: "Opening",
    closing: "Closing",
    locked: "Locked",
    unlocked: "Unlocked",
    locking: "Locking",
    unlocking: "Unlocking",
    unavailable: "Unavailable",
    unknown: "Unknown",
    home: "Home",
    not_home: "Away",
  }

  return standardStates[rawState] ?? titleCase(rawState)
}

function formatCause(entry: LogbookEntry): string | undefined {
  const source =
    entry.context_name ??
    entry.context_entity_id_name ??
    entry.context_message ??
    entry.context_source

  if (!source) {
    return undefined
  }

  switch (entry.context_domain) {
    case "automation":
      return `By automation ${source}`

    case "script":
      return `By script ${source}`

    default:
      return source
  }
}

function iconFor(entry: LogbookEntry, state: HassState | undefined): string {
  const domain = entry.entity_id?.split(".", 1)[0] ?? entry.domain ?? ""

  const deviceClass =
    typeof state?.attributes.device_class === "string"
      ? state.attributes.device_class
      : ""

  if (
    entry.icon?.includes("light-switch") ||
    entry.icon?.includes("toggle-switch")
  ) {
    return "🎚️"
  }

  if (domain === "light") {
    return "💡"
  }

  if (domain === "switch") {
    return "🔘"
  }

  if (domain === "binary_sensor") {
    if (deviceClass === "motion") {
      return "🚶"
    }

    if (deviceClass === "occupancy" || deviceClass === "presence") {
      return "🏠"
    }

    if (
      deviceClass === "door" ||
      deviceClass === "window" ||
      deviceClass === "opening"
    ) {
      return "🚪"
    }

    if (deviceClass === "moisture") {
      return "💧"
    }

    return "◉"
  }

  if (domain === "automation") {
    return "🤖"
  }

  if (domain === "script") {
    return "📜"
  }

  if (domain === "lock") {
    return "🔒"
  }

  if (domain === "cover") {
    return "🪟"
  }

  if (domain === "button" || entry.message) {
    return "🎛️"
  }

  return "▪️"
}

function activityKey(entry: LogbookEntry): string {
  return createHash("sha256").update(JSON.stringify(entry)).digest("hex")
}

function titleCase(value: string): string {
  return value
    .replaceAll("_", " ")
    .replace(/\b\p{L}/gu, (character) => character.toUpperCase())
}

function escapeSlack(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
}

function shouldIgnoreActivityEntry(entry: LogbookEntry): boolean {
  const entityId = entry.entity_id ?? ""
  const domain =
    entry.domain ?? (entityId.includes(".") ? entityId.split(".")[0] : "")

  return (
    entityId === "sun.sun" ||
    entityId.startsWith("sensor.sun_next_") ||
    domain === "weather"
  )
}

async function readOptions(): Promise<Options> {
  const value = JSON.parse(
    await readFile(OPTIONS_PATH, "utf8"),
  ) as Partial<Options>

  const slackBotToken =
    typeof value.slack_bot_token === "string"
      ? value.slack_bot_token.trim()
      : ""

  const slackAppToken =
    typeof value.slack_app_token === "string"
      ? value.slack_app_token.trim()
      : ""

  const slackChannelId =
    typeof value.slack_channel_id === "string"
      ? value.slack_channel_id.trim()
      : ""

  const securityChannelId =
    typeof value.security_channel_id === "string"
      ? value.security_channel_id.trim()
      : ""

  const doorSwitchEntityId =
    typeof value.door_switch_entity_id === "string"
      ? value.door_switch_entity_id.trim()
      : ""

  console.log("Slack configuration loaded", {
    appTokenPresent: slackAppToken.length > 0,
    appTokenLooksLikeAppToken: slackAppToken.startsWith("xapp-"),
    tokenPresent: slackBotToken.length > 0,
    tokenLooksLikeBotToken: slackBotToken.startsWith("xoxb-"),
    tokenLength: slackBotToken.length,
    channelId: slackChannelId,
  })

  if (!slackBotToken) {
    throw new Error("slack_bot_token is missing or empty")
  }

  if (!slackBotToken.startsWith("xoxb-")) {
    throw new Error("slack_bot_token does not look like a Bot User OAuth Token")
  }

  if (!slackAppToken) {
    throw new Error("slack_app_token is missing or empty")
  }

  if (!slackAppToken.startsWith("xapp-")) {
    throw new Error("slack_app_token does not look like an App-Level Token")
  }

  if (!slackChannelId) {
    throw new Error("slack_channel_id is missing or empty")
  }

  if (!securityChannelId) {
    throw new Error("security_channel_id is missing or empty")
  }

  if (!doorSwitchEntityId.startsWith("switch.")) {
    throw new Error("door_switch_entity_id must be a switch entity ID")
  }

  return {
    slack_app_token: slackAppToken,
    slack_bot_token: slackBotToken,
    slack_channel_id: slackChannelId,
    security_channel_id: securityChannelId,
    door_switch_entity_id: doorSwitchEntityId,
    initial_lookback_seconds: value.initial_lookback_seconds ?? 10,
    include_area: value.include_area ?? true,
  }
}

async function readPersistentState(): Promise<PersistentState> {
  try {
    const value = JSON.parse(
      await readFile(STATE_PATH, "utf8"),
    ) as Partial<PersistentState>

    return {
      lastWhen: value.lastWhen ?? 0,
      recentKeys: value.recentKeys ?? [],
    }
  } catch {
    return {
      lastWhen: 0,
      recentKeys: [],
    }
  }
}

async function savePersistentState(state: PersistentState): Promise<void> {
  await writeFile(STATE_TEMP_PATH, JSON.stringify(state, null, 2), "utf8")

  await rename(STATE_TEMP_PATH, STATE_PATH)
}

async function main(): Promise<void> {
  const supervisorToken = process.env.SUPERVISOR_TOKEN

  if (!supervisorToken) {
    throw new Error("SUPERVISOR_TOKEN was not provided")
  }

  const options = await readOptions()
  const persistentState = await readPersistentState()
  const seenKeys = new Set(persistentState.recentKeys)

  const slack = new SlackPoster(
    options.slack_bot_token,
    options.slack_channel_id,
  )

  await slack.testAuthentication()

  console.log("Slack authentication verified; starting Activity bridge")

  let homeAssistant: HomeAssistantClient | undefined
  const securityListener = new SlackSecurityListener(
    options.slack_app_token,
    options.security_channel_id,
    options.door_switch_entity_id,
    () => homeAssistant,
  )

  await securityListener.start()

  let reconnectDelay = 1_000

  for (;;) {
    let refreshTimer: NodeJS.Timeout | undefined

    try {
      console.log("Connecting to Home Assistant")

      const client = new HomeAssistantClient(supervisorToken)
      await client.connect()
      homeAssistant = client

      console.log("Connected; loading metadata")

      let metadata = await loadMetadata(client)

      const startWhen =
        persistentState.lastWhen > 0
          ? Math.max(0, persistentState.lastWhen - REPLAY_OVERLAP_SECONDS)
          : Date.now() / 1_000 - options.initial_lookback_seconds

      console.log(
        `Subscribing to Activity from ${new Date(
          startWhen * 1_000,
        ).toISOString()}`,
      )

      await client.subscribe<LogbookStreamEvent>(
        "logbook/event_stream",
        {
          start_time: new Date(startWhen * 1_000).toISOString(),

          // deliberately no entity IDs or device IDs,
          // which will request the complete Activity stream
        },
        (streamEvent) => {
          for (const entry of streamEvent.events ?? []) {
            if (!Number.isFinite(entry.when)) {
              console.warn("Ignoring Activity entry without a timestamp", entry)
              continue
            }

            if (shouldIgnoreActivityEntry(entry)) {
              console.log(`Ignoring Activity entry from ${entry.entity_id}`)
              continue
            }

            const key = activityKey(entry)

            if (seenKeys.has(key)) {
              continue
            }

            // mark it immediately so overlapping historical/live responses
            // do not enqueue the same entry twice
            seenKeys.add(key)

            const slackMessage = formatSlackMessage(entry, metadata, options)

            slack.enqueue(slackMessage, async () => {
              persistentState.lastWhen = Math.max(
                persistentState.lastWhen,
                entry.when,
              )

              persistentState.recentKeys.push(key)

              while (persistentState.recentKeys.length > RECENT_KEY_LIMIT) {
                const removed = persistentState.recentKeys.shift()

                if (removed) {
                  seenKeys.delete(removed)
                }
              }

              await savePersistentState(persistentState)
            })
          }
        },
      )

      refreshTimer = setInterval(() => {
        void loadMetadata(client)
          .then((refreshedMetadata) => {
            metadata = refreshedMetadata
            console.log("Refreshed Home Assistant metadata")
          })
          .catch((error) => {
            console.error("Could not refresh metadata", error)
          })
      }, METADATA_REFRESH_MS)

      reconnectDelay = 1_000
      await client.closed

      console.warn("Home Assistant connection closed")
    } catch (error) {
      console.error("Activity bridge failed", error)
    } finally {
      homeAssistant = undefined

      if (refreshTimer) {
        clearInterval(refreshTimer)
      }
    }

    console.log(`Reconnecting in ${reconnectDelay} ms`)
    await sleep(reconnectDelay)

    reconnectDelay = Math.min(reconnectDelay * 2, 60_000)
  }
}

main().catch((error) => {
  console.error("Fatal error", error)
  process.exitCode = 1
})
