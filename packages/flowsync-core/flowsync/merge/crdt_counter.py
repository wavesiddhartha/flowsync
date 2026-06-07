from typing import Dict

class CRDTCounter:
    """
    A counter that multiple nodes can increment/decrement simultaneously.
    The final value is merged correctly (commutative, associative, idempotent).
    Implements a PN-Counter CRDT.
    """

    def __init__(self, node_id: str, initial: int = 0):
        """
        Initializes a CRDTCounter.

        Args:
            node_id: The unique identifier of this client node.
            initial: The initial value to start the counter at.
        """
        self.node_id = node_id
        
        # Initialize internal state dictionaries
        self._increments: Dict[str, int] = {}
        self._decrements: Dict[str, int] = {}
        
        if initial > 0:
            self._increments[self.node_id] = initial
        elif initial < 0:
            self._decrements[self.node_id] = -initial
        else:
            self._increments[self.node_id] = 0
            self._decrements[self.node_id] = 0

    def to_state(self) -> dict:
        """Returns the current state dictionary of this counter."""
        return {
            "increments": dict(self._increments),
            "decrements": dict(self._decrements)
        }

    def increment(self, amount: int = 1) -> dict:
        """
        Increment the counter by amount.

        Args:
            amount: The positive integer to add.
            
        Returns:
            The updated state dictionary.
        """
        if amount < 0:
            return self.decrement(-amount)
            
        current = self._increments.get(self.node_id, 0)
        self._increments[self.node_id] = current + amount
        return self.to_state()

    def decrement(self, amount: int = 1) -> dict:
        """
        Decrement the counter by amount.

        Args:
            amount: The positive integer to subtract.

        Returns:
            The updated state dictionary.
        """
        if amount < 0:
            return self.increment(-amount)
            
        current = self._decrements.get(self.node_id, 0)
        self._decrements[self.node_id] = current + amount
        return self.to_state()

    @property
    def value(self) -> int:
        """Current computed integer value."""
        total_inc = sum(self._increments.values())
        total_dec = sum(self._decrements.values())
        return total_inc - total_dec

    @staticmethod
    def merge(state_a: dict, state_b: dict) -> dict:
        """
        Merge two counter states from different nodes.
        For each node_id: take the MAX of their increment/decrement counts.

        Args:
            state_a: The first state dict.
            state_b: The second state dict.
            
        Returns:
            The merged state dict.
        """
        # Parse states, defaulting to empty dicts if None/invalid
        inc_a = state_a.get("increments", {}) if isinstance(state_a, dict) else {}
        dec_a = state_a.get("decrements", {}) if isinstance(state_a, dict) else {}
        
        inc_b = state_b.get("increments", {}) if isinstance(state_b, dict) else {}
        dec_b = state_b.get("decrements", {}) if isinstance(state_b, dict) else {}

        # Merge increments
        merged_inc: Dict[str, int] = {}
        all_inc_keys = set(inc_a.keys()) | set(inc_b.keys())
        for key in all_inc_keys:
            merged_inc[key] = max(inc_a.get(key, 0), inc_b.get(key, 0))

        # Merge decrements
        merged_dec: Dict[str, int] = {}
        all_dec_keys = set(dec_a.keys()) | set(dec_b.keys())
        for key in all_dec_keys:
            merged_dec[key] = max(dec_a.get(key, 0), dec_b.get(key, 0))

        return {
            "increments": merged_inc,
            "decrements": merged_dec
        }

    @staticmethod
    def from_state(state: dict, node_id: str) -> "CRDTCounter":
        """
        Reconstruct a counter from a saved state dict.

        Args:
            state: Saved state dictionary.
            node_id: The client node ID.
        """
        counter = CRDTCounter(node_id=node_id)
        if isinstance(state, dict):
            counter._increments = dict(state.get("increments", {}))
            counter._decrements = dict(state.get("decrements", {}))
        return counter
