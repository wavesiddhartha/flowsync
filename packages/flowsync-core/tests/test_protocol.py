import pytest
from flowsync.transport import protocol
from flowsync.transport.protocol import (
    AuthMessage,
    AuthOkMessage,
    AuthFailMessage,
    PushMessage,
    GetMessage,
    GetReplyMessage,
    SubscribeMessage,
    UnsubscribeMessage,
    JoinRoomMessage,
    LeaveRoomMessage,
    PingMessage,
    PongMessage,
    BroadcastMessage,
    ErrorMessage,
    ReconnectMessage,
    ReconnectAckMessage,
)

def test_auth_message():
    msg = AuthMessage(token="my-secret-token", node_id="client-abc")
    data = protocol.encode(msg)
    decoded = protocol.decode(data)
    assert isinstance(decoded, AuthMessage)
    assert decoded.token == "my-secret-token"
    assert decoded.node_id == "client-abc"

def test_push_message():
    msg = PushMessage(stream="game:score", value=100, ts=1234.56, metadata={"device": "mobile"})
    data = protocol.encode(msg)
    decoded = protocol.decode(data)
    assert isinstance(decoded, PushMessage)
    assert decoded.stream == "game:score"
    assert decoded.value == 100
    assert decoded.ts == 1234.56
    assert decoded.metadata == {"device": "mobile"}

def test_get_and_reply_messages():
    get_msg = GetMessage(stream="game:score", req_id="req-1")
    get_data = protocol.encode(get_msg)
    decoded_get = protocol.decode(get_data)
    assert isinstance(decoded_get, GetMessage)
    assert decoded_get.stream == "game:score"
    assert decoded_get.req_id == "req-1"

    reply_msg = GetReplyMessage(stream="game:score", value=100, req_id="req-1", ts=1234.56, node_id="server")
    reply_data = protocol.encode(reply_msg)
    decoded_reply = protocol.decode(reply_data)
    assert isinstance(decoded_reply, GetReplyMessage)
    assert decoded_reply.stream == "game:score"
    assert decoded_reply.value == 100
    assert decoded_reply.req_id == "req-1"
    assert decoded_reply.ts == 1234.56
    assert decoded_reply.node_id == "server"

def test_subscribe_and_unsubscribe():
    sub = SubscribeMessage(stream="test-stream")
    decoded_sub = protocol.decode(protocol.encode(sub))
    assert isinstance(decoded_sub, SubscribeMessage)
    assert decoded_sub.stream == "test-stream"

    unsub = UnsubscribeMessage(stream="test-stream")
    decoded_unsub = protocol.decode(protocol.encode(unsub))
    assert isinstance(decoded_unsub, UnsubscribeMessage)
    assert decoded_unsub.stream == "test-stream"

def test_room_messages():
    join = JoinRoomMessage(room="lobby")
    decoded_join = protocol.decode(protocol.encode(join))
    assert isinstance(decoded_join, JoinRoomMessage)
    assert decoded_join.room == "lobby"

    leave = LeaveRoomMessage(room="lobby")
    decoded_leave = protocol.decode(protocol.encode(leave))
    assert isinstance(decoded_leave, LeaveRoomMessage)
    assert decoded_leave.room == "lobby"

def test_ping_pong():
    ping = PingMessage()
    decoded_ping = protocol.decode(protocol.encode(ping))
    assert isinstance(decoded_ping, PingMessage)

    pong = PongMessage()
    decoded_pong = protocol.decode(protocol.encode(pong))
    assert isinstance(decoded_pong, PongMessage)

def test_broadcast_and_error():
    broadcast = BroadcastMessage(event="alert", data={"msg": "hello"})
    decoded_bc = protocol.decode(protocol.encode(broadcast))
    assert isinstance(decoded_bc, BroadcastMessage)
    assert decoded_bc.event == "alert"
    assert decoded_bc.data == {"msg": "hello"}

    err = ErrorMessage(code=404, message="Stream not found")
    decoded_err = protocol.decode(protocol.encode(err))
    assert isinstance(decoded_err, ErrorMessage)
    assert decoded_err.code == 404
    assert decoded_err.message == "Stream not found"

def test_reconnect_message():
    rec = ReconnectMessage(node_id="client-abc", last_seen_ts=100.0, pending=[{"stream": "s", "value": 1, "ts": 101.0}])
    decoded = protocol.decode(protocol.encode(rec))
    assert isinstance(decoded, ReconnectMessage)
    assert decoded.node_id == "client-abc"
    assert decoded.last_seen_ts == 100.0
    assert len(decoded.pending) == 1
    assert decoded.pending[0]["stream"] == "s"

def test_reconnect_ack_message():
    ack = ReconnectAckMessage(missed_updates=[{"stream": "s", "value": 2, "ts": 102.0, "node_id": "other"}])
    decoded = protocol.decode(protocol.encode(ack))
    assert isinstance(decoded, ReconnectAckMessage)
    assert len(decoded.missed_updates) == 1
    assert decoded.missed_updates[0]["stream"] == "s"
    assert decoded.missed_updates[0]["value"] == 2

def test_invalid_messages():
    with pytest.raises(ValueError):
        protocol.decode("not a json")

    with pytest.raises(ValueError):
        protocol.decode('{"no_type": "field"}')

    with pytest.raises(ValueError):
        protocol.decode('{"type": "UNKNOWN_ACTION"}')
