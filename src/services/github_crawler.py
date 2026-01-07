# src/services/github_crawler.py
from typing import Optional
from datetime import datetime

from src.adapters.github_api import GitHubGraphQLAdapter
from src.repositories.repo_repository import RepositoryRepository
from src.config.database import DatabaseConfig, GitHubConfig

class GitHubCrawlerService:
    """
    Main crawler service orchestrating the crawl process.
    
    Follows single responsibility principle - coordinates between
    the API adapter and repository layer.
    """
    
    def __init__(
        self,
        github_config: GitHubConfig,
        db_config: DatabaseConfig
    ):
        self._github_adapter = GitHubGraphQLAdapter(
            token=github_config.token,
            endpoint=github_config.graphql_endpoint
        )
        self._repo_repository = RepositoryRepository(db_config)
        self._max_repos = github_config.max_repos
        self._batch_size = github_config.batch_size
    
    def crawl_stars(self, search_query: str = "stars:>0") -> dict:
        """
        Crawl GitHub repositories and store in database.
        
        Args:
            search_query: GitHub search query (default: all repos with stars)
        
        Returns:
            Statistics about the crawl operation
        """
        start_time = datetime.utcnow()
        total_crawled = 0
        total_upserted = 0
        
        print(f"Starting crawl at {start_time}")
        print(f"Target: {self._max_repos} repositories")
        
        try:
            # Iterate through batches from GitHub API
            for batch in self._github_adapter.search_repositories(
                query_string=search_query,
                batch_size=self._batch_size,
                max_repos=self._max_repos
            ):
                # Upsert batch to database
                affected = self._repo_repository.upsert_batch(batch)
                total_crawled += len(batch)
                total_upserted += affected
                
                print(f"Progress: {total_crawled}/{self._max_repos} "
                      f"(upserted: {total_upserted})")
                
                if total_crawled >= self._max_repos:
                    break
        
        finally:
            self._github_adapter.close()
        
        end_time = datetime.utcnow()
        duration = (end_time - start_time).total_seconds()
        
        stats = {
            'total_crawled': total_crawled,
            'total_upserted': total_upserted,
            'duration_seconds': duration,
            'repos_per_second': total_crawled / duration if duration > 0 else 0
        }
        
        print(f"\nCrawl completed!")
        print(f"Total repositories: {total_crawled}")
        print(f"Duration: {duration:.2f} seconds")
        print(f"Speed: {stats['repos_per_second']:.2f} repos/second")
        
        return stats
    
    def export_data(self, filepath: str) -> None:
        """Export crawled data to CSV file."""
        self._repo_repository.export_to_csv(filepath)
        print(f"Data exported to {filepath}")