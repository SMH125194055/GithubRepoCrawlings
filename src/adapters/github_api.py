# src/adapters/github_api.py
"""
Anti-corruption layer for GitHub's GraphQL API.

This adapter:
1. Translates GitHub API responses to our domain models
2. Handles rate limiting transparently
3. Provides retry mechanisms
4. Isolates external API changes from our core logic
5. Works around the 1000 result limit per search query
"""

import time
import requests
from typing import List, Generator, Optional
from dataclasses import dataclass
from datetime import datetime, timezone
from tenacity import (
    retry, 
    stop_after_attempt, 
    wait_exponential, 
    retry_if_exception_type,
    before_sleep_log
)
import logging

from src.models.repository import Repository

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RateLimitInfo:
    """Immutable rate limit information."""
    remaining: int
    reset_at: datetime
    limit: int


class GitHubAPIError(Exception):
    """Custom exception for GitHub API errors."""
    pass


class RateLimitExceeded(GitHubAPIError):
    """Raised when rate limit is exceeded."""
    pass


class GitHubGraphQLAdapter:
    """
    Anti-corruption layer for GitHub's GraphQL API.
    
    IMPORTANT: GitHub Search API has a 1000 result limit per query.
    This adapter works around it by using multiple queries with different
    star count ranges to fetch more than 1000 repositories.
    """
    
    # GraphQL query for searching repositories
    SEARCH_REPOS_QUERY = """
    query($queryString: String!, $first: Int!, $after: String) {
        search(query: $queryString, type: REPOSITORY, first: $first, after: $after) {
            pageInfo {
                hasNextPage
                endCursor
            }
            repositoryCount
            nodes {
                ... on Repository {
                    databaseId
                    id
                    name
                    owner {
                        login
                    }
                    stargazerCount
                }
            }
        }
        rateLimit {
            remaining
            resetAt
            limit
            cost
        }
    }
    """
    
    # Star ranges to partition the search (each should return <1000 repos ideally)
    # These ranges are designed to get diverse repositories
    STAR_RANGES = [
        # High star repos (fewer repos, but important)
        (100000, None),   # 100k+ stars
        (50000, 99999),   # 50k-100k
        (20000, 49999),   # 20k-50k
        (10000, 19999),   # 10k-20k
        (5000, 9999),     # 5k-10k
        (2000, 4999),     # 2k-5k
        (1000, 1999),     # 1k-2k
        (500, 999),       # 500-1k
        (200, 499),       # 200-500
        (100, 199),       # 100-200
        (50, 99),         # 50-100
        (20, 49),         # 20-50
        (10, 19),         # 10-20
        (5, 9),           # 5-10
        (2, 4),           # 2-5
        (1, 1),           # exactly 1 star
        (0, 0),           # 0 stars
    ]
    
    def __init__(self, token: str, endpoint: str = 'https://api.github.com/graphql'):
        self._token = token
        self._endpoint = endpoint
        self._session = requests.Session()
        self._session.headers.update({
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json',
            'User-Agent': 'GitHub-Crawler-Bot/1.0'
        })
        self._last_rate_limit: Optional[RateLimitInfo] = None
    
    def _parse_rate_limit(self, rate_limit_data: dict) -> RateLimitInfo:
        """Parse rate limit info from API response."""
        reset_at_str = rate_limit_data.get('resetAt', '')
        if reset_at_str:
            reset_at = datetime.fromisoformat(reset_at_str.replace('Z', '+00:00'))
        else:
            reset_at = datetime.now(timezone.utc)
        
        return RateLimitInfo(
            remaining=rate_limit_data.get('remaining', 0),
            reset_at=reset_at,
            limit=rate_limit_data.get('limit', 5000)
        )
    
    def _check_and_handle_rate_limit(self, rate_limit_info: RateLimitInfo) -> None:
        """Check and handle rate limiting proactively."""
        self._last_rate_limit = rate_limit_info
        
        # If we're running low on requests, wait for reset
        if rate_limit_info.remaining < 50:
            now = datetime.now(timezone.utc)
            wait_seconds = (rate_limit_info.reset_at - now).total_seconds()
            
            if wait_seconds > 0:
                logger.warning(
                    f"Rate limit low ({rate_limit_info.remaining} remaining). "
                    f"Waiting {wait_seconds:.0f}s until reset..."
                )
                time.sleep(wait_seconds + 2)  # Add buffer
            
        # Log rate limit status periodically
        elif rate_limit_info.remaining % 100 == 0:
            logger.info(f"Rate limit: {rate_limit_info.remaining}/{rate_limit_info.limit} remaining")
    
    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=4, max=120),
        retry=retry_if_exception_type((requests.RequestException, requests.Timeout)),
        before_sleep=before_sleep_log(logger, logging.WARNING)
    )
    def _execute_query(self, query: str, variables: dict) -> dict:
        """Execute GraphQL query with retry logic and error handling."""
        try:
            response = self._session.post(
                self._endpoint,
                json={'query': query, 'variables': variables},
                timeout=30
            )
            
            # Handle HTTP errors
            if response.status_code == 403:
                # Check if it's rate limiting
                if 'rate limit' in response.text.lower():
                    logger.warning("Rate limit exceeded via HTTP 403, waiting 60s...")
                    time.sleep(60)
                    raise RateLimitExceeded("Rate limit exceeded")
                raise GitHubAPIError(f"Forbidden: {response.text}")
            
            if response.status_code == 502 or response.status_code == 503:
                logger.warning(f"GitHub server error ({response.status_code}), retrying...")
                raise requests.RequestException(f"Server error: {response.status_code}")
            
            if response.status_code != 200:
                raise GitHubAPIError(f"API error: {response.status_code} - {response.text}")
            
            data = response.json()
            
            # Handle GraphQL errors
            if 'errors' in data:
                error_messages = [e.get('message', str(e)) for e in data['errors']]
                error_str = '; '.join(error_messages)
                
                # Check for rate limit errors in GraphQL response
                if 'rate limit' in error_str.lower():
                    logger.warning("Rate limit in GraphQL response, waiting 60s...")
                    time.sleep(60)
                    raise RateLimitExceeded(error_str)
                
                # Some errors are recoverable, log and continue
                if 'timeout' in error_str.lower() or 'loading' in error_str.lower():
                    logger.warning(f"Transient GraphQL error: {error_str}")
                    raise requests.RequestException(error_str)
                
                raise GitHubAPIError(f"GraphQL errors: {error_str}")
            
            return data.get('data', {})
            
        except requests.Timeout:
            logger.warning("Request timeout, will retry...")
            raise
    
    def _search_with_query(
        self,
        query_string: str,
        batch_size: int = 100,
        max_results: int = 1000
    ) -> Generator[List[Repository], None, None]:
        """
        Search repositories with a specific query string.
        
        Limited to max 1000 results per GitHub API limitation.
        """
        cursor = None
        total_fetched = 0
        
        while total_fetched < max_results:
            current_batch_size = min(batch_size, max_results - total_fetched)
            
            variables = {
                'queryString': query_string,
                'first': current_batch_size,
                'after': cursor
            }
            
            try:
                data = self._execute_query(self.SEARCH_REPOS_QUERY, variables)
            except (GitHubAPIError, RateLimitExceeded) as e:
                logger.error(f"Query failed: {e}")
                break
            
            # Handle rate limiting
            if 'rateLimit' in data:
                rate_limit = self._parse_rate_limit(data['rateLimit'])
                self._check_and_handle_rate_limit(rate_limit)
            
            search_data = data.get('search', {})
            nodes = search_data.get('nodes', [])
            
            if not nodes:
                break
            
            # Transform to domain objects, filtering out null nodes
            repositories = []
            for node in nodes:
                if node is not None and node.get('databaseId') is not None:
                    try:
                        repo = Repository.from_graphql_response(node)
                        repositories.append(repo)
                    except (KeyError, TypeError) as e:
                        logger.warning(f"Skipping malformed node: {e}")
                        continue
            
            if repositories:
                yield repositories
            
            total_fetched += len(repositories)
            
            # Check for more pages
            page_info = search_data.get('pageInfo', {})
            if not page_info.get('hasNextPage', False):
                break
            
            cursor = page_info.get('endCursor')
            
            # Small delay between requests to be nice to the API
            time.sleep(0.1)
    
    def search_repositories(
        self,
        query_string: str = "stars:>=0",
        batch_size: int = 100,
        max_repos: int = 100_000
    ) -> Generator[List[Repository], None, None]:
        """
        Search repositories and yield them in batches.
        
        Works around the 1000 result limit by partitioning searches
        by star count ranges. Uses cursor-based pagination within each range.
        
        Args:
            query_string: Base query (additional filters will be added)
            batch_size: Number of repos per API call (max 100)
            max_repos: Maximum total repos to fetch
        
        Yields:
            Batches of Repository objects for memory efficiency
        """
        total_fetched = 0
        seen_ids = set()  # Track unique repos to avoid duplicates
        
        logger.info(f"Starting search for {max_repos} repositories...")
        
        for min_stars, max_stars in self.STAR_RANGES:
            if total_fetched >= max_repos:
                break
            
            # Build query for this star range
            if max_stars is None:
                star_query = f"stars:>={min_stars}"
            elif min_stars == max_stars:
                star_query = f"stars:{min_stars}"
            else:
                star_query = f"stars:{min_stars}..{max_stars}"
            
            full_query = f"{star_query} sort:updated"
            logger.info(f"Searching: {full_query}")
            
            range_fetched = 0
            
            for batch in self._search_with_query(
                query_string=full_query,
                batch_size=batch_size,
                max_results=min(1000, max_repos - total_fetched)
            ):
                # Filter out duplicates
                unique_repos = []
                for repo in batch:
                    if repo.id not in seen_ids:
                        seen_ids.add(repo.id)
                        unique_repos.append(repo)
                
                if unique_repos:
                    yield unique_repos
                    total_fetched += len(unique_repos)
                    range_fetched += len(unique_repos)
                    
                    logger.info(
                        f"Progress: {total_fetched}/{max_repos} total, "
                        f"{range_fetched} from current range ({star_query})"
                    )
                
                if total_fetched >= max_repos:
                    break
        
        logger.info(f"Search complete. Total unique repositories: {total_fetched}")
    
    def get_rate_limit_status(self) -> Optional[RateLimitInfo]:
        """Get current rate limit status."""
        return self._last_rate_limit
    
    def close(self):
        """Clean up resources."""
        self._session.close()
        logger.info("GitHub API adapter closed")
