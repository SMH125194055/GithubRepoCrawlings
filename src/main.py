# src/main.py
"""
Entry point for the GitHub Repository Crawler.

This script orchestrates the complete crawl process:
1. Load configuration from environment
2. Initialize the crawler service
3. Execute the crawl
4. Export results
"""

import os
import sys
import logging

from src.config.database import DatabaseConfig, GitHubConfig
from src.services.github_crawler import GitHubCrawlerService

# Configure logging for the entire application
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


def main() -> int:
    """
    Main entry point for the crawler.
    
    Returns:
        Exit code (0 for success, 1 for error)
    """
    logger.info("GitHub Repository Crawler starting...")
    
    # Load configuration from environment variables
    try:
        github_config = GitHubConfig.from_env()
        db_config = DatabaseConfig.from_env()
    except Exception as e:
        logger.error(f"Configuration error: {e}")
        return 1
    
    # Validate GitHub token
    if not github_config.token:
        logger.error(
            "GITHUB_TOKEN environment variable not set. "
            "Please set it to a valid GitHub personal access token or use the default GITHUB_TOKEN in Actions."
        )
        return 1
    
    logger.info(f"Configuration loaded:")
    logger.info(f"  - Target repos: {github_config.max_repos:,}")
    logger.info(f"  - Batch size: {github_config.batch_size}")
    logger.info(f"  - Database: {db_config.host}:{db_config.port}/{db_config.database}")
    
    try:
        # Create crawler service
        crawler = GitHubCrawlerService(github_config, db_config)
        
        # Execute the crawl
        # Using "stars:>=0" to get repositories with any number of stars
        # sorted by update time to get a diverse set
        stats = crawler.crawl_stars(search_query="stars:>=0")
        
        # Export results to CSV
        output_path = os.getenv('OUTPUT_PATH', 'repositories.csv')
        exported = crawler.export_data(output_path)
        
        # Print final summary
        logger.info("\n" + "=" * 60)
        logger.info("FINAL SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Repositories crawled: {stats['total_crawled']:,}")
        logger.info(f"Database rows affected: {stats['total_upserted']:,}")
        logger.info(f"Crawl duration: {stats['duration_seconds']:.2f}s")
        logger.info(f"Average speed: {stats['repos_per_second']:.2f} repos/sec")
        logger.info(f"Data exported to: {output_path} ({exported:,} rows)")
        logger.info("=" * 60)
        
        return 0
        
    except KeyboardInterrupt:
        logger.warning("Crawler interrupted by user")
        return 130
    except Exception as e:
        logger.exception(f"Crawler failed with error: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())
