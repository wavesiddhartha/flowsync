from typing import Any, Dict, Set

class CRDTSet:
    """
    A set of items where add/remove operations from multiple nodes always merge correctly.
    Uses a Two-Phase Set (2P-Set) strategy: once removed, an item stays removed (remove wins).
    """

    def __init__(self):
        """Initializes an empty CRDTSet."""
        self._added: Set[str] = set()
        self._removed: Set[str] = set()

    def to_state(self) -> dict:
        """Returns the current state dictionary of this set (lists for JSON compatibility)."""
        return {
            "added": list(self._added),
            "removed": list(self._removed)
        }

    def add(self, item: str) -> dict:
        """
        Adds an item to the set.

        Args:
            item: The string item to add.
            
        Returns:
            The updated state dictionary.
        """
        self._added.add(item)
        return self.to_state()

    def remove(self, item: str) -> dict:
        """
        Removes an item from the set.

        Args:
            item: The string item to remove.
            
        Returns:
            The updated state dictionary.
        """
        self._removed.add(item)
        return self.to_state()

    def contains(self, item: str) -> bool:
        """Checks if an item is currently active in the set."""
        return item in self._added and item not in self._removed

    @property
    def items(self) -> Set[str]:
        """All current items (added minus removed)."""
        return self._added - self._removed

    @staticmethod
    def merge(state_a: dict, state_b: dict) -> dict:
        """
        Merge two set states.
        added   = union of both added sets
        removed = union of both removed sets

        Args:
            state_a: The first state dict.
            state_b: The second state dict.
            
        Returns:
            The merged state dict.
        """
        # Load lists from dicts, defaulting to empty list if key not present or state is invalid
        added_a = set(state_a.get("added", [])) if isinstance(state_a, dict) else set()
        removed_a = set(state_a.get("removed", [])) if isinstance(state_a, dict) else set()

        added_b = set(state_b.get("added", [])) if isinstance(state_b, dict) else set()
        removed_b = set(state_b.get("removed", [])) if isinstance(state_b, dict) else set()

        merged_added = added_a | added_b
        merged_removed = removed_a | removed_b

        return {
            "added": list(merged_added),
            "removed": list(merged_removed)
        }

    @staticmethod
    def from_state(state: dict) -> "CRDTSet":
        """Reconstruct a set from a saved state dict."""
        crdt_set = CRDTSet()
        if isinstance(state, dict):
            crdt_set._added = set(state.get("added", []))
            crdt_set._removed = set(state.get("removed", []))
        return crdt_set
