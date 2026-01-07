-- scripts/setup_database.sql

-- Enable UUID extension for unique IDs
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Main repositories table
CREATE TABLE repositories (
    id BIGINT PRIMARY KEY,                    -- GitHub's repo ID (stable identifier)
    node_id VARCHAR(255) UNIQUE NOT NULL,     -- GitHub's GraphQL node ID
    full_name VARCHAR(500) NOT NULL,          -- e.g., "microsoft/vscode"
    owner_login VARCHAR(255) NOT NULL,        -- Repository owner username
    name VARCHAR(255) NOT NULL,               -- Repository name
    stargazer_count INTEGER NOT NULL DEFAULT 0,
    
    -- Metadata for tracking changes
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Index for efficient lookups
    CONSTRAINT unique_full_name UNIQUE (full_name)
);

-- Index for efficient updates and queries
CREATE INDEX idx_repositories_updated_at ON repositories(updated_at);
CREATE INDEX idx_repositories_owner ON repositories(owner_login);
CREATE INDEX idx_repositories_stars ON repositories(stargazer_count DESC);

-- Crawl history for tracking runs
CREATE TABLE crawl_runs (
    id SERIAL PRIMARY KEY,
    started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE,
    repos_crawled INTEGER DEFAULT 0,
    status VARCHAR(50) DEFAULT 'running'  -- running, completed, failed
);