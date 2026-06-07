import asyncio
import logging
import time
from typing import Any, Callable, Dict, List, Optional, Set

from flowsync.merge.crdt_counter import CRDTCounter
from flowsync.merge.crdt_set import CRDTSet
from flowsync.merge.crdt_text import CRDTText

logger = logging.getLogger("flowsync.stream")

# =====================================================================
# Merge Rule Implementation Functions
# =====================================================================

def lww_merge(val_a: Any, val_b: Any, meta_a: dict, meta_b: dict) -> tuple[Any, dict]:
    """Last-Write-Wins: higher timestamp wins. Tie-break via node_id."""
    ts_a = meta_a.get("ts", 0.0)
    ts_b = meta_b.get("ts", 0.0)
    node_a = meta_a.get("node_id", "")
    node_b = meta_b.get("node_id", "")
    
    if ts_b > ts_a:
        return val_b, meta_b
    elif ts_b == ts_a:
        if node_b > node_a:
            return val_b, meta_b
    return val_a, meta_a


def crdt_counter_merge(val_a: Any, val_b: Any, meta_a: dict, meta_b: dict) -> tuple[Any, dict]:
    """Applies Counter operations or merges two PN-Counter states."""
    ts = max(meta_a.get("ts", 0.0), meta_b.get("ts", 0.0))
    node_id_b = meta_b.get("node_id", "server")
    node_id = meta_a.get("node_id", "") if meta_a.get("ts", 0.0) > meta_b.get("ts", 0.0) else node_id_b
    
    # 1. Parse existing state
    state_a = val_a if isinstance(val_a, dict) else CRDTCounter(node_id="server").to_state()
    
    # 2. Check if val_b is an operation
    if isinstance(val_b, dict) and "op" in val_b:
        counter = CRDTCounter.from_state(state_a, node_id_b)
        op = val_b.get("op")
        amount = val_b.get("amount", 1)
        if op == "increment":
            counter.increment(amount)
        elif op == "decrement":
            counter.decrement(amount)
        merged_val = counter.to_state()
    else:
        # It's a full state
        merged_val = CRDTCounter.merge(state_a, val_b)
        
    return merged_val, {**meta_b, "ts": ts, "node_id": node_id}


def crdt_set_merge(val_a: Any, val_b: Any, meta_a: dict, meta_b: dict) -> tuple[Any, dict]:
    """Applies Set operations or merges two 2P-Set states."""
    ts = max(meta_a.get("ts", 0.0), meta_b.get("ts", 0.0))
    node_id_b = meta_b.get("node_id", "server")
    node_id = meta_a.get("node_id", "") if meta_a.get("ts", 0.0) > meta_b.get("ts", 0.0) else node_id_b

    # 1. Parse existing state
    state_a = val_a if isinstance(val_a, dict) else CRDTSet().to_state()

    # 2. Check if val_b is an operation
    if isinstance(val_b, dict) and "op" in val_b:
        crdt_set = CRDTSet.from_state(state_a)
        op = val_b.get("op")
        item = val_b.get("item")
        if op == "add":
            crdt_set.add(item)
        elif op == "remove":
            crdt_set.remove(item)
        merged_val = crdt_set.to_state()
    else:
        # It's a full state
        merged_val = CRDTSet.merge(state_a, val_b)

    return merged_val, {**meta_b, "ts": ts, "node_id": node_id}


def crdt_text_merge(val_a: Any, val_b: Any, meta_a: dict, meta_b: dict) -> tuple[Any, dict]:
    """Merges concurrent text operation logs."""
    import uuid
    ts = max(meta_a.get("ts", 0.0), meta_b.get("ts", 0.0))
    node_id = meta_a.get("node_id", "") if meta_a.get("ts", 0.0) > meta_b.get("ts", 0.0) else meta_b.get("node_id", "")

    list_a = val_a if isinstance(val_a, list) else []
    
    def enrich_op(op, fallback_node, fallback_ts):
        if isinstance(op, dict) and "op" in op:
            if "op_id" not in op:
                op = op.copy()
                op["op_id"] = f"{fallback_node}:{time.time_ns()}:{uuid.uuid4().hex[:4]}"
            if "ts" not in op:
                op = op.copy()
                op["ts"] = fallback_ts
        return op

    # Check if incoming is single operation or list of operations
    if isinstance(val_b, dict) and "op" in val_b:
        list_b = [enrich_op(val_b, meta_b.get("node_id", "server"), meta_b.get("ts", ts))]
    elif isinstance(val_b, list):
        list_b = [enrich_op(op, meta_b.get("node_id", "server"), meta_b.get("ts", ts)) for op in val_b]
    else:
        list_b = []
        
    merged_val = CRDTText.merge_ops(list_a, list_b)
    
    # ── SECURITY: Compact if ops exceed limit ──
    MAX_CRDT_TEXT_OPS = 50_000
    if len(merged_val) > MAX_CRDT_TEXT_OPS:
        final_text = CRDTText.apply_all(merged_val)
        merged_val = [{
            "op": "insert",
            "pos": 0,
            "char": final_text,
            "op_id": f"compact:{time.time_ns()}",
            "ts": ts
        }]
        
    return merged_val, {**meta_b, "ts": ts, "node_id": node_id}


def min_merge(val_a: Any, val_b: Any, meta_a: dict, meta_b: dict) -> tuple[Any, dict]:
    """Always keeps the lower numeric value."""
    if val_a is None:
        return val_b, meta_b
    if val_b is None:
        return val_a, meta_a
    try:
        if val_b < val_a:
            return val_b, meta_b
    except Exception:
        pass
    return val_a, meta_a


def max_merge(val_a: Any, val_b: Any, meta_a: dict, meta_b: dict) -> tuple[Any, dict]:
    """Always keeps the higher numeric value."""
    if val_a is None:
        return val_b, meta_b
    if val_b is None:
        return val_a, meta_a
    try:
        if val_b > val_a:
            return val_b, meta_b
    except Exception:
        pass
    return val_a, meta_a


def append_merge(val_a: Any, val_b: Any, meta_a: dict, meta_b: dict) -> tuple[Any, dict]:
    """Append-only log: preserves all elements in order."""
    list_a = val_a if isinstance(val_a, list) else ([val_a] if val_a is not None else [])
    list_b = val_b if isinstance(val_b, list) else ([val_b] if val_b is not None else [])
    merged_val = list_a + list_b
    ts = max(meta_a.get("ts", 0.0), meta_b.get("ts", 0.0))
    node_id = meta_a.get("node_id", "") if meta_a.get("ts", 0.0) > meta_b.get("ts", 0.0) else meta_b.get("node_id", "")
    return merged_val, {**meta_b, "ts": ts, "node_id": node_id}


class MergeRuleRegistry:
    """Maps rule names to merge functions."""
    
    RULES = {
        "lww":          lww_merge,
        "crdt-counter": crdt_counter_merge,
        "crdt-set":     crdt_set_merge,
        "crdt-text":    crdt_text_merge,
        "max":          max_merge,
        "min":          min_merge,
        "append":       append_merge,
    }
    
    @classmethod
    def get(cls, rule_name: str) -> Callable[[Any, Any, dict, dict], tuple[Any, dict]]:
        if rule_name not in cls.RULES:
            raise ValueError(f"Unknown merge rule: {rule_name}")
        return cls.RULES[rule_name]


# =====================================================================
# Stream Class
# =====================================================================

class Stream:
    """
    A Stream is a named, shared, reactive piece of state.
    It lives on the Hub, and all connected Nodes can subscribe to it.
    """

    def __init__(
        self,
        name: str,
        initial: Any = None,
        merge_rule: str = "lww",
        persist: bool = False,
        history_limit: int = 100,
        access: Optional[Dict[str, List[str]]] = None,
        merge_fn: Optional[Callable[[Any, Any, Dict[str, Any], Dict[str, Any]], tuple[Any, Dict[str, Any]]]] = None,
    ):
        self.name = name
        self.merge_rule = merge_rule
        self.persist = persist
        self.history_limit = history_limit
        self.access = access or {"read": ["*"], "write": ["*"]}
        
        # Verify merge rule is known or custom
        if merge_rule != "custom" and merge_rule not in MergeRuleRegistry.RULES:
            raise ValueError(f"Unknown merge rule: {merge_rule}")

        # Initialize internal state
        self._ts = 0.0
        self._node_id = "system"
        self._metadata: Dict[str, Any] = {"ts": 0.0, "node_id": "system"}
        self.conflicts_resolved = 0
        
        # Set up initial state according to rule
        self._value = self._initialize_value(initial, merge_rule)
        
        self._history: List[Dict[str, Any]] = []
        if self._value is not None or self.merge_rule in ("crdt-counter", "crdt-set", "crdt-text"):
            self._history.append({
                "value": self._value,
                "ts": self._ts,
                "node_id": self._node_id,
                "metadata": self._metadata
            })
            
        self._subscribers: Set[Callable[[Any, Dict[str, Any]], Any]] = set()
        self._merge_fn = merge_fn
        self._notify_semaphore = asyncio.Semaphore(1000)
        self._lock = asyncio.Lock()

    def _initialize_value(self, initial: Any, merge_rule: str) -> Any:
        if initial is not None:
            return initial
        # For CRDT rules, default to appropriate empty state structures
        if merge_rule == "crdt-counter":
            return CRDTCounter(node_id="system").to_state()
        elif merge_rule == "crdt-set":
            return CRDTSet().to_state()
        elif merge_rule == "crdt-text":
            return []
        return None

    async def push(self, value: Any, metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Push a new value. Triggers all subscribers instantly if the update is accepted.

        Args:
            value: The new state value or modifying operation.
            metadata: Metadata containing 'ts' (timestamp) and 'node_id' of the client pushing.
        """
        async with self._lock:
            meta = metadata or {}
            ts = meta.get("ts")
            if ts is None:
                ts = time.time()
            node_id = meta.get("node_id") or "server"

            full_meta = {**meta, "ts": ts, "node_id": node_id}
            updated = False

            if self._value is not None:
                # Merge rule path (since state already exists)
                merge_fn = self._merge_fn if self.merge_rule == "custom" else MergeRuleRegistry.get(self.merge_rule)
                if merge_fn is None:
                    raise ValueError(f"Custom merge rule selected but no merge_fn provided for stream '{self.name}'")

                merged_val, merged_meta = merge_fn(
                    self._value, value, self._metadata, full_meta
                )

                # Check for timestamp overlap conflict
                is_conflict = (node_id != self._node_id and ts <= self._ts)
                if is_conflict:
                    self.conflicts_resolved += 1
                    merged_meta = {
                        **merged_meta,
                        "conflict_resolved": True,
                        "resolved_by": self.merge_rule,
                        "original_values": {"existing": self._value, "incoming": value}
                    }

                self._value = merged_val
                self._ts = merged_meta.get("ts", ts)
                self._node_id = merged_meta.get("node_id", node_id)
                self._metadata = merged_meta
                updated = True
            else:
                # First push path (initialize/construct)
                if self.merge_rule == "crdt-counter":
                    if isinstance(value, dict) and "op" in value:
                        counter = CRDTCounter(node_id=node_id)
                        op = value.get("op")
                        amount = value.get("amount", 1)
                        if op == "increment":
                            counter.increment(amount)
                        elif op == "decrement":
                            counter.decrement(amount)
                        self._value = counter.to_state()
                    else:
                        self._value = value
                elif self.merge_rule == "crdt-set":
                    if isinstance(value, dict) and "op" in value:
                        crdt_set = CRDTSet()
                        op = value.get("op")
                        item = value.get("item")
                        if op == "add":
                            crdt_set.add(item)
                        elif op == "remove":
                            crdt_set.remove(item)
                        self._value = crdt_set.to_state()
                    else:
                        self._value = value
                elif self.merge_rule == "crdt-text":
                    current_ops = []
                    new_ops = [value] if (isinstance(value, dict) and "op" in value) else (value if isinstance(value, list) else [])
                    self._value = CRDTText.merge_ops(current_ops, new_ops)
                else:
                    self._value = value

                self._ts = ts
                self._node_id = node_id
                self._metadata = full_meta
                updated = True

            if updated:
                self._add_to_history(self._value, self._ts, self._node_id, self._metadata)
                await self._notify_subscribers()

    def _add_to_history(self, value: Any, ts: float, node_id: str, metadata: Dict[str, Any]) -> None:
        """Appends a new state to the stream history, enforcing the history limit."""
        import copy
        history_entry = {
            "value": copy.deepcopy(value),
            "ts": ts,
            "node_id": node_id,
            "metadata": {k: v for k, v in metadata.items() if k != "original_values"}
        }
        self._history.append(history_entry)
        if len(self._history) > self.history_limit:
            self._history.pop(0)

    async def _notify_subscribers(self) -> None:
        """Triggers all subscriber callbacks concurrently, passing raw CRDT values and metadata."""
        if not self._subscribers:
            return
        
        meta = {**self._metadata, "merge_rule": self.merge_rule}
        
        tasks = []
        for cb in list(self._subscribers):
            if asyncio.iscoroutinefunction(cb):
                async def guarded_cb(callback=cb):
                    async with self._notify_semaphore:
                        try:
                            await callback(self._value, meta)
                        except Exception as e:
                            logger.error(f"Error in async subscriber callback: {e}", exc_info=True)
                tasks.append(asyncio.create_task(guarded_cb()))
            else:
                try:
                    cb(self._value, meta)
                except Exception as e:
                    logger.error(f"Error in subscriber callback: {e}", exc_info=True)
                    
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def _get_computed_value(self) -> Any:
        """Helper to resolve internal CRDT states to user-facing values."""
        if self.merge_rule == "crdt-counter":
            if isinstance(self._value, dict):
                return sum(self._value.get("increments", {}).values()) - sum(self._value.get("decrements", {}).values())
            return self._value or 0
        elif self.merge_rule == "crdt-set":
            if isinstance(self._value, dict):
                added = set(self._value.get("added", []))
                removed = set(self._value.get("removed", []))
                return list(added - removed)
            return self._value or []
        elif self.merge_rule == "crdt-text":
            if isinstance(self._value, list):
                return CRDTText.apply_all(self._value)
            return self._value or ""
        return self._value

    async def get(self) -> Any:
        """Get the current resolved value of the stream."""
        return self._get_computed_value()

    async def get_raw(self) -> Any:
        """Get the raw state representation (CRDT structure)."""
        return self._value

    async def history(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get the past N states with timestamps and node IDs."""
        return self._history[-limit:]

    def subscribe(self, callback: Callable[[Any, Dict[str, Any]], Any]) -> Callable[[], None]:
        """Subscribe to stream changes. Receives (computed_value, metadata)."""
        self._subscribers.add(callback)
        
        def unsubscribe() -> None:
            self._subscribers.discard(callback)
            
        return unsubscribe

    async def delete(self) -> None:
        """Delete the stream and notify all subscribers of deletion."""
        async with self._lock:
            deletion_meta = {"ts": time.time(), "node_id": "system", "deleted": True, "merge_rule": self.merge_rule}
            tasks = []
            for cb in list(self._subscribers):
                if asyncio.iscoroutinefunction(cb):
                    tasks.append(asyncio.create_task(cb(None, deletion_meta)))
                else:
                    try:
                        cb(None, deletion_meta)
                    except Exception as e:
                        logger.error(f"Error in delete subscriber callback: {e}", exc_info=True)
            
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
                
            self._subscribers.clear()
            self._history.clear()
            self._value = self._initialize_value(None, self.merge_rule)
            self._ts = 0.0
            self._metadata = {"ts": 0.0, "node_id": "system", "deleted": True}
