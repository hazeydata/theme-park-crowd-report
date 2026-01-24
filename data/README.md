# Data Directory

This directory is reserved for storing data files locally, if needed in the future.

## Current Usage

**Currently not used**: The pipeline streams data directly from S3 without downloading files locally.

**Why**: Streaming is more efficient - no need to download large files, saves disk space.

## Directory Structure

- **`raw/`**: Would store raw data files if downloaded from S3
- **`processed/`**: Would store intermediate processed files if needed

## When You Might Use This

- **Local development**: Download sample files for testing parsers
- **Offline processing**: If you need to process files without S3 access
- **Backup**: Store copies of important files locally

## Note

This directory is gitignored, so any files placed here will not be tracked in version control. This is intentional - data files can be large and should not be in the repository.
