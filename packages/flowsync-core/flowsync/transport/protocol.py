import json
import time
from typing import Any, Dict, Optional, Union, Literal, Type
from pydantic import BaseModel, Field

# =====================================================================
# Client to Hub Messages
# =====================================================================

class AuthMessage(BaseModel):
    """Initial message sent by a client to authenticate with the Hub."""
    type: Literal["AUTH"] = "AUTH"
    token: str
    node_id: Optional[str] = None


class PushMessage(BaseModel):
    """Sent by a client to update the value of a specific stream."""
    type: Literal["PUSH"] = "PUSH"
    stream: str
    value: Any
    ts: float = Field(default_factory=lambda: time.time())
    metadata: Optional[Dict[str, Any]] = None


class GetMessage(BaseModel):
    """Sent by a client to retrieve the current value of a stream."""
    type: Literal["GET"] = "GET"
    stream: str
    req_id: str


class SubscribeMessage(BaseModel):
    """Sent by a client to subscribe to real-time updates for a stream."""
    type: Literal["SUBSCRIBE"] = "SUBSCRIBE"
    stream: str


class UnsubscribeMessage(BaseModel):
    """Sent by a client to stop receiving updates for a stream."""
    type: Literal["UNSUBSCRIBE"] = "UNSUBSCRIBE"
    stream: str


class JoinRoomMessage(BaseModel):
    """Sent by a client to join a communication room/namespace."""
    type: Literal["JOIN_ROOM"] = "JOIN_ROOM"
    room: str


class LeaveRoomMessage(BaseModel):
    """Sent by a client to leave a room/namespace."""
    type: Literal["LEAVE_ROOM"] = "LEAVE_ROOM"
    room: str


class PingMessage(BaseModel):
    """Heartbeat check sent by a client."""
    type: Literal["PING"] = "PING"


class ReconnectMessage(BaseModel):
    """Sent by a client attempting to reconnect and replay offline queue."""
    type: Literal["RECONNECT"] = "RECONNECT"
    node_id: str
    last_seen_ts: float
    pending: list[dict] = Field(default_factory=list)
    token: Optional[str] = None


# =====================================================================
# Hub to Client Messages
# =====================================================================

class AuthOkMessage(BaseModel):
    """Sent by the Hub on successful authentication."""
    type: Literal["AUTH_OK"] = "AUTH_OK"
    node_id: str


class AuthFailMessage(BaseModel):
    """Sent by the Hub on failed authentication."""
    type: Literal["AUTH_FAIL"] = "AUTH_FAIL"
    reason: str


class UpdateMessage(BaseModel):
    """Broadcast by the Hub to all subscribers when a stream changes."""
    type: Literal["UPDATE"] = "UPDATE"
    stream: str
    value: Any
    ts: float
    node_id: str
    metadata: Optional[Dict[str, Any]] = None


class GetReplyMessage(BaseModel):
    """Sent by the Hub in response to a GET message."""
    type: Literal["GET_REPLY"] = "GET_REPLY"
    stream: str
    value: Any
    req_id: str
    ts: Optional[float] = None
    node_id: Optional[str] = None


class BroadcastMessage(BaseModel):
    """Custom event message sent to all connected clients or room members."""
    type: Literal["BROADCAST"] = "BROADCAST"
    event: str
    data: Any


class ErrorMessage(BaseModel):
    """Sent by the Hub to indicate an error has occurred."""
    type: Literal["ERROR"] = "ERROR"
    code: int
    message: str


class PongMessage(BaseModel):
    """Heartbeat response sent by the Hub."""
    type: Literal["PONG"] = "PONG"


class ReconnectAckMessage(BaseModel):
    """Sent by the Hub on successful reconnection with missed updates."""
    type: Literal["RECONNECT_ACK"] = "RECONNECT_ACK"
    missed_updates: list[dict] = Field(default_factory=list)


# =====================================================================
# Type Aliases and Mapping
# =====================================================================

ClientMessage = Union[
    AuthMessage,
    PushMessage,
    GetMessage,
    SubscribeMessage,
    UnsubscribeMessage,
    JoinRoomMessage,
    LeaveRoomMessage,
    PingMessage,
    ReconnectMessage,
]

HubMessage = Union[
    AuthOkMessage,
    AuthFailMessage,
    UpdateMessage,
    GetReplyMessage,
    BroadcastMessage,
    ErrorMessage,
    PongMessage,
    ReconnectAckMessage,
]

FlowSyncMessage = Union[ClientMessage, HubMessage]

_MESSAGE_CLASS_MAP: Dict[str, Type[BaseModel]] = {
    # Client to Hub
    "AUTH": AuthMessage,
    "PUSH": PushMessage,
    "GET": GetMessage,
    "SUBSCRIBE": SubscribeMessage,
    "UNSUBSCRIBE": UnsubscribeMessage,
    "JOIN_ROOM": JoinRoomMessage,
    "LEAVE_ROOM": LeaveRoomMessage,
    "PING": PingMessage,
    "RECONNECT": ReconnectMessage,
    # Hub to Client
    "AUTH_OK": AuthOkMessage,
    "AUTH_FAIL": AuthFailMessage,
    "UPDATE": UpdateMessage,
    "GET_REPLY": GetReplyMessage,
    "BROADCAST": BroadcastMessage,
    "ERROR": ErrorMessage,
    "PONG": PongMessage,
    "RECONNECT_ACK": ReconnectAckMessage,
}

# =====================================================================
# Serialization Functions
# =====================================================================

def encode(message: FlowSyncMessage) -> bytes:
    """
    Encodes a FlowSyncMessage object into JSON bytes.
    
    Args:
        message: A FlowSyncMessage instance.
        
    Returns:
        The encoded bytes representation.
    """
    return message.model_dump_json().encode("utf-8")


def decode(raw_data: Union[bytes, str]) -> FlowSyncMessage:
    """
    Decodes raw JSON (bytes or string) into the appropriate FlowSyncMessage object.
    
    Args:
        raw_data: The JSON string or bytes to decode.
        
    Returns:
        The instantiated FlowSyncMessage subclass.
        
    Raises:
        ValueError: If JSON is malformed, lacks a 'type' field, or matches an unknown message type.
    """
    MAX_PAYLOAD_SIZE = 10 * 1024 * 1024  # 10MB limit
    if isinstance(raw_data, bytes):
        if len(raw_data) > MAX_PAYLOAD_SIZE:
            raise ValueError("Payload size exceeds maximum limit of 10MB")
        raw_str = raw_data.decode("utf-8")
    else:
        if len(raw_data) > MAX_PAYLOAD_SIZE:
            raise ValueError("Payload size exceeds maximum limit of 10MB")
        raw_str = raw_data

    try:
        data = json.loads(raw_str)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON format: {e}")

    if not isinstance(data, dict):
        raise ValueError("Message must be a JSON object")

    msg_type = data.get("type")
    if not msg_type:
        raise ValueError("Message missing 'type' field")

    model_cls = _MESSAGE_CLASS_MAP.get(msg_type)
    if not model_cls:
        raise ValueError(f"Unknown message type: '{msg_type}'")

    try:
        return model_cls(**data)
    except Exception:
        raise ValueError(f"Failed to parse {msg_type} message")


# End of protocol.py
