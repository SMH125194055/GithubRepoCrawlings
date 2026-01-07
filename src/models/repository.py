# src/models/repository.py
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass(frozen=True)  # Immutable - cannot be changed after creation
class Repository:
    """Represents a GitHub repository - immutable data transfer object."""
    id: int
    node_id: str
    full_name: str
    owner_login: str
    name: str
    stargazer_count: int
    fetched_at: datetime

    @classmethod
    def from_graphql_response(cls, node: dict) -> 'Repository':
        """Factory method to create Repository from GitHub API response."""
        return cls(
            id=node['databaseId'],
            node_id=node['id'],
            full_name=f"{node['owner']['login']}/{node['name']}",
            owner_login=node['owner']['login'],
            name=node['name'],
            stargazer_count=node['stargazerCount'],
            fetched_at=datetime.utcnow()
        )