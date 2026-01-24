"""
File identification utilities for theme park wait time data.

Ported from Julia codebase.
"""

import logging


def get_wait_time_filetype(key: str) -> str:
    """
    Determines the type of wait time file based on the filename.
    
    Ported from Julia code.
    
    Args:
        key: The S3 key or file path to analyze
        
    Returns:
        "Standby", "New Fastpass", "Old Fastpass", or "Unknown"
    """
    lower_key = key.lower()
    
    # Determine the wait time file type based on the key
    # If the key contains "wait_times" then it is a standby file
    if "wait_times" in lower_key:
        file_type = "Standby"
    elif "fastpass_times" in lower_key:
        # Check for old fastpass patterns (2012-2018, and specific 2019 patterns)
        old_patterns = [
            "_2012",
            "_2013",
            "_2014",
            "_2015",
            "_2016",
            "_2017",
            "_2018",
            "_2019_01",
            "_2019_02",
            "_201901",
            "_201902"
        ]
        if any(pat in lower_key for pat in old_patterns):
            file_type = "Old Fastpass"
        else:
            file_type = "New Fastpass"
    else:
        logging.warning(f"⚠️ Unrecognized file type: {key}")
        return "Unknown"
    
    return file_type
