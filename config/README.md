# Configuration

This directory contains configuration files and templates for the Theme Park Wait Time Data Pipeline.

## Current Configuration

The pipeline currently uses command-line arguments for configuration. No configuration files are required.

## Configuration Methods

### Command-Line Arguments

All configuration is done via command-line arguments when running the script:

```powershell
python src/get_tp_wait_time_data_from_s3.py --props wdw,dlr --output-base "D:\Path"
```

**Why command-line**: Simple, flexible, works well for scheduled jobs.

### Environment Variables

AWS credentials can be configured via environment variables:
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_DEFAULT_REGION`

**Why**: Standard AWS authentication method, works with AWS CLI and other tools.

### AWS Credentials File

AWS credentials can be stored in `~/.aws/credentials`:

```
[default]
aws_access_key_id = YOUR_KEY
aws_secret_access_key = YOUR_SECRET
```

**Why**: Secure way to store credentials, used by AWS CLI and boto3.

## Future Configuration Files

If needed in the future, configuration files could include:
- Default properties to process
- Output directory paths
- Chunk sizes and performance tuning
- Retry settings
- Logging configuration

## Example Configuration Template

See `config.example.json` for a template of what a configuration file might look like (if implemented in the future).
