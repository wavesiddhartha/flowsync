// All message types the client can SEND
export type ClientMessage =
  | { type: "AUTH";        token: string; node_id?: string }
  | { type: "PUSH";        stream: string; value: unknown; ts: number; metadata?: Record<string, any> }
  | { type: "GET";         stream: string; req_id: string }
  | { type: "SUBSCRIBE";   stream: string }
  | { type: "UNSUBSCRIBE"; stream: string }
  | { type: "JOIN_ROOM";   room: string }
  | { type: "LEAVE_ROOM";  room: string }
  | { type: "RECONNECT";   node_id: string; last_seen_ts: number; pending: PendingChange[]; token?: string }
  | { type: "PING" }

// All message types the client can RECEIVE
export type ServerMessage =
  | { type: "AUTH_OK";       node_id: string }
  | { type: "AUTH_FAIL";     reason: string }
  | { type: "UPDATE";        stream: string; value: unknown; ts: number; node_id: string; metadata?: Record<string, any> }
  | { type: "GET_REPLY";     stream: string; value: unknown; req_id: string; ts?: number; node_id?: string }
  | { type: "RECONNECT_ACK"; missed_updates: MissedUpdate[] }
  | { type: "BROADCAST";     event: string; data: unknown }
  | { type: "ERROR";         code: number; message: string }
  | { type: "PONG" }

export interface PendingChange {
  stream:    string
  value:     unknown
  timestamp: number
}

export interface MissedUpdate {
  stream:  string
  value:   unknown
  ts:      number
  node_id: string
  metadata?: Record<string, any>
}

export interface Meta {
  nodeId:    string
  timestamp: number
  stream:    string
  mergeRule?: string
}

// Encode a client message to JSON string
export function encode(msg: ClientMessage): string {
  return JSON.stringify(msg)
}

const VALID_SERVER_TYPES = new Set([
  "AUTH_OK", "AUTH_FAIL", "UPDATE", "GET_REPLY", "RECONNECT_ACK", "BROADCAST", "ERROR", "PONG"
])

// Decode a raw string from the server with sanitization against prototype pollution
export function decode(raw: string): ServerMessage {
  try {
    const parsed = JSON.parse(raw)
    
    // Mitigate prototype pollution vectors recursively
    const sanitize = (obj: any): void => {
      if (obj && typeof obj === "object") {
        if ("__proto__" in obj) delete obj.__proto__
        if ("constructor" in obj) delete obj.constructor
        if ("prototype" in obj) delete obj.prototype
        for (const key in obj) {
          if (Object.prototype.hasOwnProperty.call(obj, key)) {
            sanitize(obj[key])
          }
        }
      }
    }
    
    sanitize(parsed)
    
    if (!parsed || typeof parsed !== "object" || !VALID_SERVER_TYPES.has(parsed.type)) {
      throw new Error("Invalid server message structure or type")
    }
    
    return parsed as ServerMessage
  } catch (err: any) {
    throw new Error(`FlowSync: failed to decode message: ${err?.message || err}`)
  }
}
