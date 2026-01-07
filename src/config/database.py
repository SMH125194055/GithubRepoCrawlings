# src/config/database.py
import os
from dataclasses import dataclass

@dataclass(frozen=True)  # Immutable configuration
class DatabaseConfig:
    host: str
    port: int
    database: str
    user: str
    password: str
    
    @classmethod
    def from_env(cls) -> 'DatabaseConfig':
        return cls(
            host=os.getenv('DB_HOST', 'localhost'),
            port=int(os.getenv('DB_PORT', '5432')),
            database=os.getenv('DB_NAME', 'github_crawler'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', 'postgres')
        )

@dataclass(frozen=True)
class GitHubConfig:
    token: str
    graphql_endpoint: str = 'https://api.github.com/graphql'
    max_repos: int = 100_000
    batch_size: int = 100  # Repos per query (max 100)
    
    @classmethod
    def from_env(cls) -> 'GitHubConfig':
        return cls(
            token=os.getenv('GITHUB_TOKEN', ''),
            max_repos=int(os.getenv('MAX_REPOS', '100000'))
        )