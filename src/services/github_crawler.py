# src/services/github_crawler.py
"""
Main crawler service orchestrating the GitHub crawl process.

Follows single responsibility principle - coordinates between
the API adapter and repository layer without containing
business logic specific to either.
"""

from typing import Optional, Dict, Any
from datetime import datetime, timezone
import logging

from src.adapters.github_api import GitHubGraphQLAdapter
from src.repositories.repo_repository import RepositoryRepository
from src.config.database import DatabaseConfig, GitHubConfig

logger = logging.getLogger(__name__)


class GitHubCrawlerService:
    """
    Main crawler service orchestrating the crawl process.
    
    This service:
    - Coordinates between GitHub API and database layers
    - Manages the crawl workflow
    - Provides progress reporting
    - Handles graceful shutdown
    """
    
    def __init__(
        self,
        github_config: GitHubConfig,
        db_config: DatabaseConfig
    ):
        """
        Initialize the crawler service.
        
        Args:
            github_config: Configuration for GitHub API access
            db_config: Configuration for database connection
        """
        self._github_config = github_config
        self._db_config = db_config
        self._github_adapter: Optional[GitHubGraphQLAdapter] = None
        self._repo_repository: Optional[RepositoryRepository] = None
    
    def _ensure_initialized(self) -> None:
        """Lazy initialization of adapters."""
        if self._github_adapter is None:
            self._github_adapter = GitHubGraphQLAdapter(
                token=self._github_config.token,
                endpoint=self._github_config.graphql_endpoint
            )
        if self._repo_repository is None:
            self._repo_repository = RepositoryRepository(self._db_config)
    
    def crawl_stars(self, search_query: str = "stars:>=0") -> Dict[str, Any]:
        """
        Crawl GitHub repositories and store in database.
        
        This is the main entry point for the crawl operation.
        It fetches repositories from GitHub API and stores them
        in PostgreSQL using efficient batch upserts.
        
        Args:
            search_query: GitHub search query (additional filters)
        
        Returns:
            Dictionary containing crawl statistics:
            - total_crawled: Number of unique repos fetched
            - total_upserted: Number of DB rows affected
            - duration_seconds: Total crawl time
            - repos_per_second: Crawl speed
        """
        self._ensure_initialized()
        
        start_time = datetime.now(timezone.utc)
        total_crawled = 0
        total_upserted = 0
        batch_count = 0
        
        max_repos = self._github_config.max_repos
        batch_size = self._github_config.batch_size
        
        logger.info(f"=" * 60)
        logger.info(f"Starting GitHub Repository Crawl")
        logger.info(f"Target: {max_repos:,} repositories")
        logger.info(f"Batch size: {batch_size}")
        logger.info(f"Start time: {start_time.isoformat()}")
        logger.info(f"=" * 60)
        
        try:
            # Iterate through batches from GitHub API
            for batch in self._github_adapter.search_repositories(
                query_string=search_query,
                batch_size=batch_size,
                max_repos=max_repos
            ):
                batch_count += 1
                
                # Upsert batch to database
                affected = self._repo_repository.upsert_batch(batch)
                total_crawled += len(batch)
                total_upserted += affected
                
                # Progress logging every 10 batches
                if batch_count % 10 == 0:
                    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
                    rate = total_crawled / elapsed if elapsed > 0 else 0
                    logger.info(
                        f"Progress: {total_crawled:,}/{max_repos:,} repos "
                        f"({total_crawled/max_repos*100:.1f}%) | "
                        f"Rate: {rate:.1f} repos/sec | "
                        f"DB affected: {total_upserted:,}"
                    )
                
                if total_crawled >= max_repos:
                    break
        
        except KeyboardInterrupt:
            logger.warning("Crawl interrupted by user")
        except Exception as e:
            logger.error(f"Crawl error: {e}")
            raise
        finally:
            if self._github_adapter:
                self._github_adapter.close()
                self._github_adapter = None
        
        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()
        
        stats = {
            'total_crawled': total_crawled,
            'total_upserted': total_upserted,
            'duration_seconds': duration,
            'repos_per_second': total_crawled / duration if duration > 0 else 0,
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'batch_count': batch_count
        }
        
        logger.info(f"=" * 60)
        logger.info(f"Crawl Completed!")
        logger.info(f"Total repositories crawled: {total_crawled:,}")
        logger.info(f"Database rows affected: {total_upserted:,}")
        logger.info(f"Duration: {duration:.2f} seconds ({duration/60:.2f} minutes)")
        logger.info(f"Average speed: {stats['repos_per_second']:.2f} repos/second")
        logger.info(f"=" * 60)
        
        return stats
    
    def export_data(self, filepath: str) -> int:
        """
        Export crawled data to CSV file.
        
        Args:
            filepath: Output file path
            
        Returns:
            Number of rows exported
        """
        self._ensure_initialized()
        count = self._repo_repository.export_to_csv(filepath)
        logger.info(f"Data exported to {filepath} ({count:,} rows)")
        return count
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about stored data.
        
        Returns:
            Dictionary with database statistics
        """
        self._ensure_initialized()
        return self._repo_repository.get_stats()
