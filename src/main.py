# src/main.py
import os
import sys

from src.config.database import DatabaseConfig, GitHubConfig
from src.services.github_crawler import GitHubCrawlerService

def main():
    # Load configuration from environment
    github_config = GitHubConfig.from_env()
    db_config = DatabaseConfig.from_env()
    
    if not github_config.token:
        print("Error: GITHUB_TOKEN environment variable not set")
        sys.exit(1)
    
    # Create and run crawler
    crawler = GitHubCrawlerService(github_config, db_config)
    
    # Use search query that will return many repos
    # "stars:>0" gets repos with at least 1 star
    # You can also use "is:public" for any public repo
    stats = crawler.crawl_stars(search_query="stars:>=0")
    
    # Export results
    output_path = os.getenv('OUTPUT_PATH', 'repositories.csv')
    crawler.export_data(output_path)
    
    print(f"\nFinal statistics:")
    print(f"  Repositories in DB: {stats['total_crawled']}")
    print(f"  Crawl duration: {stats['duration_seconds']:.2f}s")

if __name__ == '__main__':
    main()