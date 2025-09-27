from datetime import datetime, timedelta
from typing import Tuple, Optional
import logging

def format_iso_datetime(dt: datetime) -> str:
    """
    Format datetime to Avianis API ISO format: YYYY-MM-DDTHH:MM:SS.sssZ
    """
    return dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')[:-3] + 'Z'

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
        Returns ISO format dates (YYYY-MM-DDTHH:MM:SS.sssZ)
        """
        now = get_utc_now()
        
        # Calculate start date (2 months ago)
        start_date = now - timedelta(days=self.config.INITIAL_LOAD_MONTHS_PAST * 30)
        
        # Calculate end date (10 days from now)
        end_date = now + timedelta(days=self.config.INITIAL_LOAD_DAYS_FUTURE)
        
        start_str = format_iso_datetime(start_date)
        end_str = format_iso_datetime(end_date)
        
        logging.info(f"Initial load date range: {start_str} to {end_str}")
        return start_str, end_str
    
    def get_incremental_load_dates(self) -> Tuple[str, str]:
        """
        Get date range for incremental load: last 3 days + next 10 days
        Returns ISO format dates (YYYY-MM-DDTHH:MM:SS.sssZ)
        """
        now = get_utc_now()
        
        # Calculate start date (3 days ago)
        start_date = now - timedelta(days=self.config.REFRESH_DAYS_PAST)
        
        # Calculate end date (10 days from now)
        end_date = now + timedelta(days=self.config.REFRESH_DAYS_FUTURE)
        
        start_str = format_iso_datetime(start_date)
        end_str = format_iso_datetime(end_date)
        
        logging.info(f"Incremental load date range: {start_str} to {end_str}")
        return start_str, end_str
    
    def get_last_activity_date(self, is_initial_load: bool = False) -> str:
        """
        Get last activity date for personnel queries
        Returns ISO format date (YYYY-MM-DDTHH:MM:SS.sssZ)
        """
        now = get_utc_now()
        
        if is_initial_load:
            # For initial load, go back 2 months
            last_activity = now - timedelta(days=self.config.INITIAL_LOAD_MONTHS_PAST * 30)
        else:
            # For incremental load, go back 3 days
            last_activity = now - timedelta(days=self.config.REFRESH_DAYS_PAST)
        
        last_activity_str = format_iso_datetime(last_activity)
        
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
    """Parse ISO datetime string to datetime object"""
    if not iso_string:
        return None
    
    try:
        # Handle various ISO formats
        formats = [
            '%Y-%m-%dT%H:%M:%S.%fZ',
            '%Y-%m-%dT%H:%M:%SZ',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%d %H:%M:%S'
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