from datetime import datetime, timedelta
from typing import Tuple, Optional
import logging
import hashlib

def format_iso_datetime(dt: datetime) -> str:
    """
    Format datetime to Avianis API ISO format: YYYY-MM-DDTHH:MM:SS.sssZ
    """
    return dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')[:-3] + 'Z'

def format_date_only(dt: datetime) -> str:
    """
    Format datetime to date-only format for API queries: YYYY-MM-DD
    """
    return dt.strftime('%Y-%m-%d')

def get_utc_now() -> datetime:
    """Get current UTC datetime"""
    return datetime.utcnow()

class DateRangeManager:
    """Manages date ranges for initial and incremental loads"""
    
    def __init__(self, config):
        self.config = config
        
    def get_initial_load_dates(self) -> Tuple[str, str]:
        """
        Get date range for initial load: last 2 months + next 10 days
        Returns date-only format (YYYY-MM-DD)
        """
        now = get_utc_now()

        # Calculate start date (2 months ago)
        start_date = now - timedelta(days=self.config.INITIAL_LOAD_MONTHS_PAST * 30)

        # Calculate end date (10 days from now)
        end_date = now + timedelta(days=self.config.INITIAL_LOAD_DAYS_FUTURE)

        start_str = format_date_only(start_date)
        end_str = format_date_only(end_date)

        logging.info(f"Initial load date range: {start_str} to {end_str}")
        return start_str, end_str
    
    def get_incremental_load_dates(self) -> Tuple[str, str]:
        """
        Get date range for incremental load: last 3 days + next 10 days
        Returns date-only format (YYYY-MM-DD)
        """
        now = get_utc_now()

        # Calculate start date (3 days ago)
        start_date = now - timedelta(days=self.config.REFRESH_DAYS_PAST)

        # Calculate end date (10 days from now)
        end_date = now + timedelta(days=self.config.REFRESH_DAYS_FUTURE)

        start_str = format_date_only(start_date)
        end_str = format_date_only(end_date)

        logging.info(f"Incremental load date range: {start_str} to {end_str}")
        return start_str, end_str
    
    def get_last_activity_date(self, is_initial_load: bool = False) -> str:
        """
        Get last activity date for personnel queries
        Returns date-only format (YYYY-MM-DD)
        """
        now = get_utc_now()

        if is_initial_load:
            # For initial load, go back 2 months
            last_activity = now - timedelta(days=self.config.INITIAL_LOAD_MONTHS_PAST * 30)
        else:
            # For incremental load, go back 3 days
            last_activity = now - timedelta(days=self.config.REFRESH_DAYS_PAST)

        last_activity_str = format_date_only(last_activity)

        logging.info(f"Last activity date: {last_activity_str}")
        return last_activity_str

def safe_get(data: dict, key: str, default=None):
    """Safely get value from dict with optional default"""
    return data.get(key, default) if data else default

def clean_string(value) -> Optional[str]:
    """Clean and normalize string values"""
    if value is None:
        return None
    return str(value).strip() if str(value).strip() else None

def safe_int(value) -> Optional[int]:
    """Safely convert value to int"""
    if value is None or value == '':
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None

def safe_float(value) -> Optional[float]:
    """Safely convert value to float"""
    if value is None or value == '':
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def parse_iso_datetime(iso_string: str) -> Optional[datetime]:
    """Parse ISO datetime string to datetime object

    Supports both full datetime and date-only formats
    """
    if not iso_string:
        return None

    try:
        # Handle various ISO formats
        formats = [
            '%Y-%m-%dT%H:%M:%S.%fZ',
            '%Y-%m-%dT%H:%M:%SZ',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d'  # Date-only format
        ]

        for fmt in formats:
            try:
                return datetime.strptime(iso_string, fmt)
            except ValueError:
                continue

        logging.warning(f"Could not parse datetime: {iso_string}")
        return None
        
    except Exception as e:
        logging.error(f"Error parsing datetime {iso_string}: {e}")
        return None

def parse_flight_datetime(date_string: str) -> Optional[datetime]:
    """Parse flight data datetime format like '8/4/2025 8:41:02 PM' to UTC datetime"""
    if not date_string:
        return None
    
    try:
        # Handle flight data format: "8/4/2025 8:41:02 PM"
        dt = datetime.strptime(date_string, '%m/%d/%Y %I:%M:%S %p')
        return dt
        
    except ValueError:
        # Fallback to ISO parsing
        return parse_iso_datetime(date_string)
    except Exception as e:
        logging.error(f"Error parsing flight datetime {date_string}: {e}")
        return None

def generate_stable_id(fms_id: str, min_id: int = 100000, max_id: int = 999999) -> int:
    """Generate stable 6-digit ID using SHA256 for consistent mapping across ETL runs"""
    # Use SHA256 for better distribution and take first 4 bytes as integer
    hash_bytes = hashlib.sha256(fms_id.encode('utf-8')).digest()[:4]
    hash_int = int.from_bytes(hash_bytes, byteorder='big')
    # Map to 6-digit range (100,000 to 999,999 = 900,000 possible values)
    range_size = max_id - min_id + 1
    return (hash_int % range_size) + min_id