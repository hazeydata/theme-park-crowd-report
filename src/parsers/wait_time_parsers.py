"""
Wait time data parsers for standby and fastpass/priority files.

Ported from Julia codebase and refactored for Python.
Handles both new and old fastpass formats, including sold-out detection.
"""

import io
import logging
import re
import time
from typing import Iterable

import pandas as pd
from botocore.exceptions import ClientError, ResponseStreamingError

# Fastpass column names (standard format)
PRIO_COLS = ["FATTID", "FDAY", "FMONTH", "FYEAR", "FHOUR", "FMIN", "FWINHR", "FWINMIN"]

# For old fastpass format: column indices [1,2,3,4,5,6,7,8,25] map to FATTID, FDAY, etc.
# Note: Index 25 is typically not used in our parsing, we use first 8 columns
OLD_FASTPASS_COL_INDICES = [1, 2, 3, 4, 5, 6, 7, 8]  # 0-indexed would be [1,2,3,4,5,6,7,8]


def parse_standby_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    """
    Parse a chunk of standby wait time data.
    
    Input columns expected:
    - entity_code (str)
    - observed_at (str)
    - submitted_posted_time (int, optional)
    - submitted_actual_time (int, optional)
    - user_id (str, optional - not used in output)
    
    Output: DataFrame with 4 columns:
    - entity_code (str, upper, stripped)
    - observed_at (str, stripped)
    - wait_time_type ("POSTED" or "ACTUAL")
    - wait_time_minutes (Int64, 0-1000)
    
    Ported from Julia logic:
    - Removes rows where BOTH submitted_actual_time AND submitted_posted_time are missing
    - Splits into separate rows for POSTED and ACTUAL times
    - Filters wait times to valid range (0-1000 minutes)
    """
    # Normalize column names to lowercase
    lower = {c: c.lower().strip() for c in chunk.columns}
    df = chunk.rename(columns=lower)

    # Julia: Remove rows where BOTH submitted_actual_time AND submitted_posted_time are missing
    # df[.!((ismissing.(df.submitted_actual_time)) .& (ismissing.(df.submitted_posted_time))), :]
    # Python equivalent: Keep rows where at least one is not missing
    if "submitted_posted_time" in df.columns and "submitted_actual_time" in df.columns:
        both_missing = df["submitted_posted_time"].isna() & df["submitted_actual_time"].isna()
        df = df[~both_missing].copy()

    out_frames = []

    # Process POSTED times
    # Julia: posted_df = copy(df[.!ismissing.(df.submitted_posted_time), :])
    if "submitted_posted_time" in df.columns:
        p = df[["entity_code", "observed_at", "submitted_posted_time"]].dropna(subset=["submitted_posted_time"])
        if not p.empty:
            p = p.rename(columns={"submitted_posted_time": "wait_time_minutes"}).copy()
            p["wait_time_minutes"] = pd.to_numeric(p["wait_time_minutes"], errors="coerce").round().astype("Int64")
            # Filter valid range (0-1000 minutes) - data quality check
            p = p[(p["wait_time_minutes"] >= 0) & (p["wait_time_minutes"] <= 1000)]
            p["wait_time_type"] = "POSTED"
            out_frames.append(p)

    # Process ACTUAL times
    # Julia: actual_df = copy(df[.!ismissing.(df.submitted_actual_time), :])
    if "submitted_actual_time" in df.columns:
        a = df[["entity_code", "observed_at", "submitted_actual_time"]].dropna(subset=["submitted_actual_time"])
        if not a.empty:
            a = a.rename(columns={"submitted_actual_time": "wait_time_minutes"}).copy()
            a["wait_time_minutes"] = pd.to_numeric(a["wait_time_minutes"], errors="coerce").round().astype("Int64")
            # Filter valid range (0-1000 minutes) - data quality check
            a = a[(a["wait_time_minutes"] >= 0) & (a["wait_time_minutes"] <= 1000)]
            a["wait_time_type"] = "ACTUAL"
            out_frames.append(a)

    # If no valid data, return empty DataFrame with correct columns
    if not out_frames:
        return pd.DataFrame(columns=["entity_code", "observed_at", "wait_time_type", "wait_time_minutes"])

    # Combine POSTED and ACTUAL rows
    # Julia: df = vcat(actual_df, posted_df)
    out = pd.concat(out_frames, ignore_index=True)
    
    # Normalize entity_code and observed_at
    # Julia: entity_code is already string, we uppercase and strip
    out["entity_code"] = out["entity_code"].astype("string").str.upper().str.strip()
    out["observed_at"] = out["observed_at"].astype("string").str.strip()
    
    # Return only the 4 core columns
    # Julia outputs: entity_id, observed_at, observed_wait_time, wait_time_type, wait_time_source
    # We output: entity_code, observed_at, wait_time_type, wait_time_minutes (4 columns)
    return out[["entity_code", "observed_at", "wait_time_type", "wait_time_minutes"]]


def _split_hhmm_or_hhmmss_to_hour_min(x: pd.Series) -> tuple[pd.Series, pd.Series]:
    """
    Helper: Split compact time format (HHMM or HHMMSS) into hour and minute.
    Handles duplicate indices by resetting temporarily.
    """
    v = pd.to_numeric(x, errors="coerce")
    # Create Series with a temporary unique index to avoid duplicate label issues
    original_index = v.index
    v_reset = v.reset_index(drop=True)
    
    h = pd.Series(pd.NA, index=v_reset.index, dtype="Int64")
    m = pd.Series(pd.NA, index=v_reset.index, dtype="Int64")
    hhmmss = v_reset >= 10000
    hhmm   = (v_reset >= 100) & (v_reset < 10000)
    hour   = v_reset < 100

    if hhmmss.any():
        vv = v_reset.where(hhmmss)
        h.loc[hhmmss] = (vv // 10000).astype("Int64")
        m.loc[hhmmss] = ((vv % 10000) // 100).astype("Int64")

    if hhmm.any():
        vv = v_reset.where(hhmm)
        h.loc[hhmm] = (vv // 100).astype("Int64")
        m.loc[hhmm] = (vv % 100).astype("Int64")

    if hour.any():
        vv = v_reset.where(hour)
        h.loc[hour] = vv.astype("Int64")

    # Restore original index
    h.index = original_index
    m.index = original_index
    
    return h, m


def _normalize_priority_compact_times(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize compact time formats in fastpass data.
    Handles FHOUR, FMIN, FWINHR, FWINMIN columns.
    """
    if "FHOUR" in df.columns:
        h_obs, m_obs = _split_hhmm_or_hhmmss_to_hour_min(df["FHOUR"])
        if "FMIN" in df.columns:
            m_obs = m_obs.fillna(pd.to_numeric(df["FMIN"], errors="coerce").astype("Int64"))
        df["FHOUR"] = h_obs.fillna(0).astype("Int64")
        df["FMIN"]  = m_obs.fillna(0).astype("Int64")

    if "FWINHR" in df.columns:
        h_ret, m_ret = _split_hhmm_or_hhmmss_to_hour_min(df["FWINHR"])
        if "FWINMIN" in df.columns:
            m_ret = m_ret.fillna(pd.to_numeric(df["FWINMIN"], errors="coerce").astype("Int64"))
        df["FWINHR"]  = h_ret.fillna(0).astype("Int64")
        df["FWINMIN"] = m_ret.fillna(0).astype("Int64")
    return df


def _priority_rows_to_minutes(df: pd.DataFrame, is_old_format: bool = False) -> pd.Series:
    """
    Calculate wait time minutes from fastpass data.
    Handles sold-out detection (FWINHR >= 8000 → 8888 minutes).
    Handles day rollover (return time before observed time).
    
    Args:
        df: DataFrame with PRIO_COLS columns
        is_old_format: If True, skip compact time normalization (old format has separate hour/minute)
    
    Returns:
        Series of wait time minutes, with 8888 for sold-out rows (FWINHR >= 8000)
    """
    # Only normalize compact times for new format
    if not is_old_format:
        df = _normalize_priority_compact_times(df.copy())

    Y = pd.to_numeric(df["FYEAR"],  errors="coerce").astype("Int64")
    M = pd.to_numeric(df["FMONTH"], errors="coerce").astype("Int64")
    D = pd.to_numeric(df["FDAY"],   errors="coerce").astype("Int64")

    h_obs = pd.to_numeric(df["FHOUR"],   errors="coerce").fillna(0).clip(0, 23).astype("Int64")
    m_obs = pd.to_numeric(df["FMIN"],    errors="coerce").fillna(0).clip(0, 59).astype("Int64")
    h_ret = pd.to_numeric(df["FWINHR"],  errors="coerce").fillna(0).astype("Int64")
    m_ret = pd.to_numeric(df["FWINMIN"], errors="coerce").fillna(0).astype("Int64")

    # Detect sold-out: FWINHR >= 8000
    sellout_mask = pd.to_numeric(df["FWINHR"], errors="coerce") >= 8000

    # Build observed and return datetimes
    # Julia: DateTime.(df[!, :FYEAR], df[!, :FMONTH], df[!, :FDAY], df[!, :FHOUR], df[!, :FMIN])
    obs = pd.to_datetime(dict(year=Y, month=M, day=D, hour=h_obs, minute=m_obs), errors="coerce")
    ret = pd.to_datetime(dict(year=Y, month=M, day=D,
                              hour=h_ret.clip(0, 23), minute=m_ret.clip(0, 59)), errors="coerce")
    
    # For sold-out: use far future date to calculate a large time difference
    ret = ret.mask(sellout_mask, pd.Timestamp(year=2099, month=12, day=31))
    
    # Handle day rollover: if return is more than 15 minutes before observed, add a day
    rollover = (ret - obs) < pd.Timedelta(minutes=-15)
    ret = ret + pd.to_timedelta(rollover.fillna(False).astype(int), unit="D")

    # Calculate minutes difference
    # Julia: (Return_at_Datetime .- Observed_at_Datetime) ./ Dates.Minute(1)
    minutes = ((ret - obs) / pd.Timedelta(minutes=1)).round().astype("Int64")
    
    # Set sold-out to 8888
    minutes = minutes.mask(sellout_mask, 8888)
    
    return minutes


def _format_priority(df: pd.DataFrame, is_old_format: bool = False) -> pd.DataFrame:
    """
    Format fastpass data into 4-column output.
    
    Args:
        df: DataFrame with PRIO_COLS columns
        is_old_format: If True, uses old format logic (no compact time normalization)
    
    Note: Sold-out rows (FWINHR >= 8000) are kept and assigned wait_time_minutes = 8888
    """
    if is_old_format:
        # Old format has separate hour/minute columns, no compact time normalization needed
        pass
    else:
        # New format: normalize compact times (HHMM, HHMMSS formats)
        df = _normalize_priority_compact_times(df.copy())

    minutes = _priority_rows_to_minutes(df, is_old_format=is_old_format)
    
    # Parse date components - ensure they're numeric
    Y = pd.to_numeric(df["FYEAR"],  errors="coerce").astype("Int64")
    M = pd.to_numeric(df["FMONTH"], errors="coerce").astype("Int64")
    D = pd.to_numeric(df["FDAY"],   errors="coerce").astype("Int64")
    h_obs = pd.to_numeric(df["FHOUR"], errors="coerce").fillna(0).clip(0, 23).astype("Int64")
    m_obs = pd.to_numeric(df["FMIN"],  errors="coerce").fillna(0).clip(0, 59).astype("Int64")

    # Build observed_at as ISO string (timezone added later)
    # Julia: DateTime.(df[!, :FYEAR], df[!, :FMONTH], df[!, :FDAY], df[!, :FHOUR], df[!, :FMIN])
    obs = pd.to_datetime(dict(year=Y, month=M, day=D, hour=h_obs, minute=m_obs), errors="coerce") \
             .dt.strftime("%Y-%m-%dT%H:%M:%S")

    out = pd.DataFrame({
        "entity_code": df["FATTID"].astype(str).str.upper().str.strip(),
        "observed_at": obs,
        "wait_time_type": "PRIORITY",
        "wait_time_minutes": minutes
    })
    out = out.dropna(subset=["entity_code", "observed_at", "wait_time_minutes"])
    return out[["entity_code", "observed_at", "wait_time_type", "wait_time_minutes"]]


def parse_fastpass_chunk(chunk: pd.DataFrame, is_new_format: bool) -> pd.DataFrame:
    """
    Parse a chunk of fastpass/priority data.
    
    Args:
        chunk: Raw DataFrame from CSV
        is_new_format: True if file has named columns, False if headerless/old format
        
    Returns:
        DataFrame with 4 columns: entity_code, observed_at, wait_time_type, wait_time_minutes
        
    Handles both new and old formats, including sold-out detection.
    
    Note: Old format follows Julia logic:
    - Julia uses select=[1,2,3,4,5,6,7,8,25] (1-indexed) with header=false, skipto=2
    - Python equivalent: columns [0,1,2,3,4,5,6,7] (0-indexed) with header=None, skiprows=1
    - Column order: [FATTID, FDAY, FMONTH, FYEAR, FHOUR, FMIN, FWINHR, FWINMIN]
    """
    if is_new_format:
        # New format: has named columns
        df = chunk.rename(columns={c: c.upper().strip() for c in chunk.columns})
        minimal = {"FATTID", "FDAY", "FMONTH", "FYEAR", "FHOUR", "FWINHR"}
        if not minimal.issubset(df.columns):
            return pd.DataFrame(columns=["entity_code", "observed_at", "wait_time_type", "wait_time_minutes"])
        keep = [c for c in PRIO_COLS if c in df.columns]
        df = df[keep].copy()
    else:
        # Old format: headerless, use first 8 columns by position
        # Julia: select=[1,2,3,4,5,6,7,8,25] (1-indexed) - we only need first 8
        # Python: columns [0,1,2,3,4,5,6,7] (0-indexed)
        # Column order matches: [FATTID, FDAY, FMONTH, FYEAR, FHOUR, FMIN, FWINHR, FWINMIN]
        if chunk.shape[1] < 8:
            # Not enough columns
            return pd.DataFrame(columns=["entity_code", "observed_at", "wait_time_type", "wait_time_minutes"])
        df = chunk.iloc[:, :8].copy()
        df.columns = PRIO_COLS
        
        # Ensure data types are correct for old format (all should be numeric except FATTID)
        # Convert to string first, then to numeric to avoid issues with mixed types
        for col in ["FDAY", "FMONTH", "FYEAR", "FHOUR", "FMIN", "FWINHR", "FWINMIN"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
    
    return _format_priority(df, is_old_format=not is_new_format)


def parse_fastpass_stream(s3, bucket: str, key: str, chunksize: int = 250_000, file_type: str = None, max_retries: int = 3) -> Iterable[pd.DataFrame]:
    """
    Stream and parse fastpass file, auto-detecting new vs old format.
    
    Args:
        s3: S3 client
        bucket: S3 bucket name
        key: S3 key
        chunksize: Chunk size for reading
        file_type: Optional file type ("New Fastpass" or "Old Fastpass"). 
                   If None, auto-detects from headers.
        max_retries: Maximum number of retry attempts for connection errors
    
    Yields DataFrames of parsed chunks.
    
    Handles connection errors with exponential backoff retry logic.
    """
    # Determine format: use file_type if provided, otherwise auto-detect
    is_new_format = False
    if file_type:
        # Use file type classifier result
        is_new_format = (file_type == "New Fastpass")
    else:
        # Fallback: probe first 8KB to detect format
        try:
            probe = s3.get_object(Bucket=bucket, Key=key, Range="bytes=0-8192")["Body"].read()
            head = pd.read_csv(io.BytesIO(probe), nrows=1)
            headered = set(c.upper().strip() for c in head.columns)
            is_new_format = set(PRIO_COLS).issubset(headered)
        except Exception:
            is_new_format = False

    # Stream the file with retry logic
    for attempt in range(max_retries):
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            stream = io.TextIOWrapper(obj["Body"], encoding="utf-8", errors="replace", newline="")
            
            if is_new_format:
                # New format: read with headers
                reader = pd.read_csv(stream, chunksize=chunksize, low_memory=False)
                for chunk in reader:
                    out = parse_fastpass_chunk(chunk, is_new_format=True)
                    if not out.empty:
                        yield out
            else:
                # Old format: headerless, skip first row (header row)
                # Julia uses: header=false, skipto=2 (skip row 1, start from row 2)
                # Python equivalent: header=None, skiprows=1
                # Julia selects columns [1,2,3,4,5,6,7,8,25] (1-indexed)
                # Python equivalent: columns [0,1,2,3,4,5,6,7] (0-indexed, we only need first 8)
                reader = pd.read_csv(stream, chunksize=chunksize, low_memory=False, header=None, skiprows=1)
                for chunk in reader:
                    out = parse_fastpass_chunk(chunk, is_new_format=False)
                    if not out.empty:
                        yield out
            
            # Success - break out of retry loop
            break
            
        except (ResponseStreamingError, ConnectionError, IOError) as e:
            if attempt < max_retries - 1:
                # Exponential backoff: wait 2^attempt seconds
                wait_time = 2 ** attempt
                logging.warning(f"Connection error reading {key} (attempt {attempt + 1}/{max_retries}): {e}. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                # Final attempt failed
                logging.error(f"Failed to read {key} after {max_retries} attempts: {e}")
                raise
        except Exception as e:
            # Non-retryable error
            logging.error(f"Error reading {key}: {e}")
            raise
