import time
import uuid
from typing import Any, List, Dict

class CRDTText:
    """
    A collaborative text document where multiple nodes can type concurrently.
    Uses a simplified operational transformation log merge strategy.
    """

    def __init__(self):
        """Initializes empty operation list."""
        self._ops: List[Dict[str, Any]] = []

    def insert(self, pos: int, char: str, node_id: str) -> dict:
        """
        Generates an insert operation.

        Args:
            pos: Insertion position index.
            char: Characters to insert.
            node_id: Unique client node identifier.

        Returns:
            The created operation dictionary.
        """
        op = {
            "op": "insert",
            "pos": pos,
            "char": char,
            "op_id": f"{node_id}:{time.time_ns()}:{uuid.uuid4().hex[:4]}",
            "ts": time.time()
        }
        self._ops.append(op)
        return op

    def delete(self, pos: int, node_id: str) -> dict:
        """
        Generates a delete operation.

        Args:
            pos: Delete start index.
            node_id: Unique client node identifier.

        Returns:
            The created operation dictionary.
        """
        op = {
            "op": "delete",
            "pos": pos,
            "op_id": f"{node_id}:{time.time_ns()}:{uuid.uuid4().hex[:4]}",
            "ts": time.time()
        }
        self._ops.append(op)
        return op

    @property
    def text(self) -> str:
        """Current text contents after applying all operations."""
        return self.apply_all(self._ops)

    def apply_op(self, op: dict) -> None:
        """Apply one operation locally."""
        if any(o["op_id"] == op["op_id"] for o in self._ops):
            return
        self._ops.append(op)
        self._ops = self.merge_ops(self._ops, [])

    @staticmethod
    def merge_ops(ops_a: list, ops_b: list) -> list:
        """
        Merge two operation histories into one consistent history.
        Deduplicates, sorts by timestamp, and transforms positions of concurrent actions.

        Args:
            ops_a: The first operation list.
            ops_b: The second operation list.

        Returns:
            A new list of merged and transformed operations.
        """
        # 1. Deduplicate by op_id
        seen = set()
        merged = []
        for op in ops_a + ops_b:
            if not isinstance(op, dict) or "op_id" not in op:
                continue
            op_id = op["op_id"]
            if op_id not in seen:
                seen.add(op_id)
                merged.append(op.copy())

        # 2. Sort by timestamp, breaking ties by op_id
        merged.sort(key=lambda x: (x.get("ts", 0.0), x.get("op_id", "")))

        # 3. Transform positions
        for i in range(len(merged)):
            op_i = merged[i]
            pos_i = op_i.get("pos", 0)
            
            # Extract node_id from op_id (format: node_id:time:uuid)
            op_id_parts = op_i.get("op_id", "").split(":")
            node_i = op_id_parts[0] if op_id_parts else ""

            for j in range(i):
                op_j = merged[j]
                op_id_parts_j = op_j.get("op_id", "").split(":")
                node_j = op_id_parts_j[0] if op_id_parts_j else ""

                # Transform position if operations were concurrent (from different nodes)
                if node_i != node_j:
                    if op_j.get("op") == "insert":
                        # If a character/string was inserted before or at the current position
                        if op_j.get("pos", 0) <= pos_i:
                            pos_i += len(op_j.get("char", ""))
                    elif op_j.get("op") == "delete":
                        # If characters were deleted before the current position
                        length = op_j.get("len", 1)
                        if op_j.get("pos", 0) < pos_i:
                            pos_i = max(0, pos_i - length)

            op_i["pos"] = pos_i

        return merged

    @staticmethod
    def apply_all(ops: list) -> str:
        """
        Apply a list of merged/transformed operations to an empty string.

        Args:
            ops: List of sorted, transformed operations.

        Returns:
            The resulting text string.
        """
        chars: List[str] = []
        for op in ops:
            if not isinstance(op, dict):
                continue
            op_type = op.get("op")
            pos = op.get("pos", 0)

            if op_type == "insert":
                char_val = op.get("char", "")
                pos = max(0, min(pos, len(chars)))
                chars[pos:pos] = list(char_val)
            elif op_type == "delete":
                length = op.get("len", 1)
                pos = max(0, min(pos, len(chars) - 1))
                if chars:
                    del chars[pos:pos + length]

        return "".join(chars)
