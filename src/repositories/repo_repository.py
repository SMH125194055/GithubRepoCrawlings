# src/repositories/repo_repository.py
"""
Repository pattern implementation for database operations.

Provides a clean abstraction layer between the business logic
and database operations, following clean architecture principles.
"""

import psycopg2
from psycopg2.extras import execute_values
from typing import List, Optional
from contextlib import contextmanager
import logging

from src.models.repository import Repository
from src.config.database import DatabaseConfig

logger = logging.getLogger(__name__)


class RepositoryRepository:
    """
    Repository pattern implementation for Repository entities.
    
    Handles all database interactions, providing:
    - UPSERT operations for efficient updates
    - Batch operations for performance
    - Connection management
    - Export functionality
    """
    
    def __init__(self, config: DatabaseConfig):
        self._config = config
        self._connection_pool = None
    
    @contextmanager
    def _get_connection(self):
        """
        Context manager for database connections.
        
        Ensures connections are properly closed even if exceptions occur.
        """
        conn = None
        try:
            conn = psycopg2.connect(
                host=self._config.host,
                port=self._config.port,
                database=self._config.database,
                user=self._config.user,
                password=self._config.password,
                connect_timeout=10
            )
            yield conn
        except psycopg2.OperationalError as e:
            logger.error(f"Database connection failed: {e}")
            raise
        finally:
            if conn is not None:
                conn.close()
    
    def upsert_batch(self, repositories: List[Repository]) -> int:
        """
        Insert or update repositories in batch.
        
        Uses PostgreSQL's ON CONFLICT (UPSERT) for efficient operations:
        - New repos are inserted
        - Existing repos are updated only if star count changed
        - Minimal rows affected for efficiency
        
        Args:
            repositories: List of Repository objects to upsert
            
        Returns:
            Number of rows affected (inserted or updated)
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
                
                # UPSERT query using ON CONFLICT
                # Only updates if stargazer_count actually changed
                query = """
                    INSERT INTO repositories 
                        (id, node_id, full_name, owner_login, name, stargazer_count, updated_at)
                    VALUES %s
                    ON CONFLICT (id) DO UPDATE SET
                        node_id = EXCLUDED.node_id,
                        full_name = EXCLUDED.full_name,
                        owner_login = EXCLUDED.owner_login,
                        name = EXCLUDED.name,
                        stargazer_count = EXCLUDED.stargazer_count,
                        updated_at = EXCLUDED.updated_at
                    WHERE repositories.stargazer_count IS DISTINCT FROM EXCLUDED.stargazer_count
                       OR repositories.full_name IS DISTINCT FROM EXCLUDED.full_name
                """
                
                try:
                    execute_values(cur, query, values, page_size=100)
                    affected = cur.rowcount
                    conn.commit()
                    return affected
                except psycopg2.Error as e:
                    conn.rollback()
                    logger.error(f"Batch upsert failed: {e}")
                    raise
    
    def get_count(self) -> int:
        """
        Get total number of repositories in database.
        
        Returns:
            Count of repository records
        """
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM repositories")
                result = cur.fetchone()
                return result[0] if result else 0
    
    def get_stats(self) -> dict:
        """
        Get statistics about stored repositories.
        
        Returns:
            Dictionary with count, avg stars, max stars, etc.
        """
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        COUNT(*) as total_repos,
                        COALESCE(AVG(stargazer_count), 0) as avg_stars,
                        COALESCE(MAX(stargazer_count), 0) as max_stars,
                        COALESCE(MIN(stargazer_count), 0) as min_stars,
                        COALESCE(SUM(stargazer_count), 0) as total_stars
                    FROM repositories
                """)
                row = cur.fetchone()
                return {
                    'total_repos': row[0],
                    'avg_stars': float(row[1]),
                    'max_stars': row[2],
                    'min_stars': row[3],
                    'total_stars': row[4]
                }
    
    def export_to_csv(self, filepath: str) -> int:
        """
        Export all repositories to CSV file.
        
        Uses PostgreSQL's COPY command for efficient bulk export.
        
        Args:
            filepath: Path to output CSV file
            
        Returns:
            Number of rows exported
        """
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                # First get count for return value
                cur.execute("SELECT COUNT(*) FROM repositories")
                count = cur.fetchone()[0]
                
                # Export using COPY for best performance
                with open(filepath, 'w', encoding='utf-8') as f:
                    cur.copy_expert(
                        """
                        COPY (
                            SELECT 
                                id,
                                node_id,
                                full_name,
                                owner_login,
                                name,
                                stargazer_count,
                                created_at,
                                updated_at
                            FROM repositories
                            ORDER BY stargazer_count DESC
                        ) TO STDOUT WITH CSV HEADER
                        """,
                        f
                    )
                
                logger.info(f"Exported {count} repositories to {filepath}")
                return count
    
    def truncate(self) -> None:
        """
        Remove all data from repositories table.
        
        Use with caution - for testing/reset purposes only.
        """
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE repositories RESTART IDENTITY")
                conn.commit()
                logger.warning("Repositories table truncated")
