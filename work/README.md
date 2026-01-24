# Work Directory

This directory is for intermediate working files during processing.

## Current Usage

**Currently not used**: The pipeline processes data in memory using chunked reading, so intermediate files are not needed.

## When You Might Use This

- **Large file processing**: If you need to split very large files
- **Intermediate transformations**: If you need to store partially processed data
- **Debugging**: Save intermediate results to inspect during development

## Cleanup

This directory can be safely cleaned up between runs. Files here are temporary and can be regenerated.

## Note

This directory is gitignored, so any files placed here will not be tracked in version control.
