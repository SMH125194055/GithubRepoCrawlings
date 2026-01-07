# src/adapters/github_api.py
import time
import requests
from typing import List, Optional, Tuple, Generator
from dataclasses import dataclass
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from src.models.repository import Repository

@dataclass(frozen=True)
class RateLimitInfo:
    """Immutable rate limit information."""
    remaining: int
    reset_at: int
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
    
    This adapter:
    1. Translates GitHub API responses to our domain models
    2. Handles rate limiting transparently
    3. Provides retry mechanisms
    4. Isolates external API changes from our core logic
    """
    
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
        }
    }
    """
    
    def __init__(self, token: str, endpoint: str = 'https://api.github.com/graphql'):
        self._token = token
        self._endpoint = endpoint
        self._session = requests.Session()
        self._session.headers.update({
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json',
        })
    
    def _check_rate_limit(self, rate_limit_info: dict) -> None:
        """Check and handle rate limiting."""
        remaining = rate_limit_info.get('remaining', 0)
        reset_at = rate_limit_info.get('resetAt')
        
        if remaining < 10:  # Buffer of 10 requests
            if reset_at:
                # Parse ISO format and wait
                from datetime import datetime
                reset_time = datetime.fromisoformat(reset_at.replace('Z', '+00:00'))
                wait_seconds = (reset_time - datetime.now(reset_time.tzinfo)).total_seconds()
                if wait_seconds > 0:
                    print(f"Rate limit low ({remaining}). Waiting {wait_seconds:.0f}s...")
                    time.sleep(wait_seconds + 1)
    
    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        retry=retry_if_exception_type((requests.RequestException, GitHubAPIError))
    )
    def _execute_query(self, query: str, variables: dict) -> dict:
        """Execute GraphQL query with retry logic."""
        response = self._session.post(
            self._endpoint,
            json={'query': query, 'variables': variables}
        )
        
        if response.status_code == 403:
            raise RateLimitExceeded("Rate limit exceeded")
        
        if response.status_code != 200:
            raise GitHubAPIError(f"API error: {response.status_code} - {response.text}")
        
        data = response.json()
        
        if 'errors' in data:
            raise GitHubAPIError(f"GraphQL errors: {data['errors']}")
        
        return data['data']
    
    def search_repositories(
        self,
        query_string: str = "stars:>0",
        batch_size: int = 100,
        max_repos: int = 100_000
    ) -> Generator[List[Repository], None, None]:
        """
        Search repositories and yield them in batches.
        
        Uses cursor-based pagination to efficiently fetch large datasets.
        Yields batches of Repository objects for memory efficiency.
        """
        cursor = None
        total_fetched = 0
        
        while total_fetched < max_repos:
            # Adjust batch size for final batch
            current_batch_size = min(batch_size, max_repos - total_fetched)
            
            variables = {
                'queryString': query_string,
                'first': current_batch_size,
                'after': cursor
            }
            
            data = self._execute_query(self.SEARCH_REPOS_QUERY, variables)
            
            # Handle rate limiting
            if 'rateLimit' in data:
                self._check_rate_limit(data['rateLimit'])
            
            search_data = data['search']
            nodes = search_data['nodes']
            
            if not nodes:
                break
            
            # Transform to domain objects
            repositories = [
                Repository.from_graphql_response(node)
                for node in nodes
                if node is not None  # Skip null nodes
            ]
            
            yield repositories
            
            total_fetched += len(repositories)
            print(f"Fetched {total_fetched} repositories...")
            
            # Check for more pages
            page_info = search_data['pageInfo']
            if not page_info['hasNextPage']:
                break
            
            cursor = page_info['endCursor']
    
    def close(self):
        """Clean up resources."""
        self._session.close()