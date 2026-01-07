# src/repositories/repo_repository.py
import psycopg2
from psycopg2.extras import execute_values
from typing import List
from contextlib import contextmanager

from src.models.repository import Repository
from src.config.database import DatabaseConfig

class RepositoryRepository:
    """
    Repository pattern implementation for database operations.
    
    Handles all database interactions for Repository entities.
    Uses UPSERT for efficient updates (minimal rows affected).
    """
    
    def __init__(self, config: DatabaseConfig):
        self._config = config
    
    @contextmanager
    def _get_connection(self):
        """Context manager for database connections."""
        conn = psycopg2.connect(
            host=self._config.host,
            port=self._config.port,
            database=self._config.database,
            user=self._config.user,
            password=self._config.password
        )
        try:
            yield conn
        finally:
            conn.close()
    
    def upsert_batch(self, repositories: List[Repository]) -> int:
        """
        Insert or update repositories in batch.
        
        Uses PostgreSQL's ON CONFLICT for efficient upserts.
        Only updates rows where data has actually changed.
        
        Returns: Number of affected rows
        """
        if not repositories:
            return 0
        
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                # Prepare data for batch insert
                values = [
                    (
                        repo.id,
                        repo.node_id,
                        repo.full_name,
                        repo.owner_login,
                        repo.name,
                        repo.stargazer_count,
                        repo.fetched_at
                    )
                    for repo in repositories
                ]
                
                # UPSERT query - only updates if stargazer_count changed
                query = """
                    INSERT INTO repositories 
                        (id, node_id, full_name, owner_login, name, stargazer_count, updated_at)
                    VALUES %s
                    ON CONFLICT (id) DO UPDATE SET
                        stargazer_count = EXCLUDED.stargazer_count,
                        updated_at = EXCLUDED.updated_at
                    WHERE repositories.stargazer_count != EXCLUDED.stargazer_count
                """
                
                execute_values(cur, query, values)
                affected = cur.rowcount
                conn.commit()
                
                return affected
    
    def get_count(self) -> int:
        """Get total number of repositories in database."""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM repositories")
                return cur.fetchone()[0]
    
    def export_to_csv(self, filepath: str) -> None:
        """Export all repositories to CSV file."""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                with open(filepath, 'w') as f:
                    cur.copy_expert(
                        """
                        COPY (
                            SELECT id, node_id, full_name, owner_login, name, 
                                   stargazer_count, created_at, updated_at
                            FROM repositories
                            ORDER BY stargazer_count DESC
                        ) TO STDOUT WITH CSV HEADER
                        """,
                        f
                    )