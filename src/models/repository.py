# src/models/repository.py
"""
Immutable data models for the GitHub Crawler.

Following the principle of immutability for data transfer objects
to prevent accidental mutations and ensure thread safety.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional


@dataclass(frozen=True)
class Repository:
    """
    Represents a GitHub repository - immutable data transfer object.
    
    All fields are read-only after creation to ensure data integrity
    as objects pass through different layers of the application.
    """
    id: int                    # GitHub's database ID (stable identifier)
    node_id: str               # GitHub's GraphQL node ID
    full_name: str             # e.g., "microsoft/vscode"
    owner_login: str           # Repository owner username
    name: str                  # Repository name
    stargazer_count: int       # Number of stars
    fetched_at: datetime       # When this data was fetched

    @classmethod
    def from_graphql_response(cls, node: dict) -> 'Repository':
        """
        Factory method to create Repository from GitHub GraphQL API response.
        
        This method acts as an anti-corruption layer, translating
        external API format to our internal domain model.
        
        Args:
            node: Dictionary containing repository data from GraphQL response
            
        Returns:
            Repository instance with validated data
            
        Raises:
            KeyError: If required fields are missing
            TypeError: If field types are invalid
        """
        # Extract and validate required fields
        database_id = node.get('databaseId')
        if database_id is None:
            raise KeyError("databaseId is required")
        
        node_id = node.get('id')
        if not node_id:
            raise KeyError("id (node_id) is required")
        
        owner = node.get('owner', {})
        owner_login = owner.get('login')
        if not owner_login:
            raise KeyError("owner.login is required")
        
        name = node.get('name')
        if not name:
            raise KeyError("name is required")
        
        stargazer_count = node.get('stargazerCount', 0)
        
        return cls(
            id=int(database_id),
            node_id=str(node_id),
            full_name=f"{owner_login}/{name}",
            owner_login=str(owner_login),
            name=str(name),
            stargazer_count=int(stargazer_count) if stargazer_count is not None else 0,
            fetched_at=datetime.now(timezone.utc)
        )
    
    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            'id': self.id,
            'node_id': self.node_id,
            'full_name': self.full_name,
            'owner_login': self.owner_login,
            'name': self.name,
            'stargazer_count': self.stargazer_count,
            'fetched_at': self.fetched_at.isoformat()
        }
