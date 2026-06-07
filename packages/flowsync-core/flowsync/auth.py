from dataclasses import dataclass, field
import jwt
from typing import Any, List, Optional

@dataclass
class AuthResult:
    node_id: str
    roles: List[str] = field(default_factory=list)
    permissions: List[str] = field(default_factory=list)  # e.g. ["read:game:*", "write:game:*"]

    def __post_init__(self):
        # Normalize roles
        if isinstance(self.roles, str):
            self.roles = [self.roles]
        elif self.roles is None:
            self.roles = []
        else:
            self.roles = list(self.roles)
        self.roles = [str(r) for r in self.roles]

        # Normalize permissions
        if isinstance(self.permissions, str):
            self.permissions = [self.permissions]
        elif self.permissions is None:
            self.permissions = []
        else:
            self.permissions = list(self.permissions)
        self.permissions = [str(p) for p in self.permissions]

    def can_read(self, stream_name: str) -> bool:
        return self.has_permission("read", stream_name)

    def can_write(self, stream_name: str) -> bool:
        return self.has_permission("write", stream_name)

    def has_permission(self, action: str, stream_name: str) -> bool:
        # If permissions is empty, check if Roles contain permissions or allow by default if empty
        if not self.permissions:
            # If roles has "admin", allow everything
            if "admin" in self.roles:
                return True
            # Default to allowing everything if no explicit rules set to maintain compatibility
            return True

        for perm in self.permissions:
            if ":" not in perm:
                # Check for absolute wildcard *
                if perm == "*":
                    return True
                continue
                
            p_action, p_pattern = perm.split(":", 1)
            if p_action != action and p_action != "*":
                continue

            if p_pattern == "*":
                return True
            if p_pattern.endswith("*"):
                prefix = p_pattern[:-1]
                if stream_name.startswith(prefix):
                    return True
            if p_pattern == stream_name:
                return True

        return False


def jwt_auth(secret: str, algorithms: Optional[List[str]] = None):
    """
    Standard JWT validator callback.
    Decodes tokens with PyJWT and returns an AuthResult.
    """
    algs = list(algorithms) if algorithms is not None else ["HS256"]
    
    async def validator(token: str) -> Optional[AuthResult]:
        try:
            payload = jwt.decode(token, secret, algorithms=algs)
            node_id = payload.get("sub") or payload.get("node_id")
            if not node_id:
                return None
            roles = payload.get("roles", [])
            permissions = payload.get("permissions", [])
            return AuthResult(node_id=node_id, roles=roles, permissions=permissions)
        except jwt.PyJWTError:
            return None
    return validator
