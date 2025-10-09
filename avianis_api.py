import requests
import logging
import threading
import time
from typing import Dict, List, Optional
from config import Config

class AuthManager:
    """Centralized authentication manager for Avianis OAuth"""
    
    def __init__(self, config: Config):
        self.config = config
        self.access_token = None
        self.headers = {}
        self.authenticated = False
        self.auth_timestamp = 0
        self.token_expiry = 20  # 20 seconds as specified in API docs
        self._lock = threading.Lock()
        
    def authenticate(self) -> bool:
        """Authenticate using OAuth client credentials flow"""
        with self._lock:
            # Check if we have a valid token (with 5 second buffer)
            if self.authenticated and (time.time() - self.auth_timestamp) < (self.token_expiry - 5):
                return True
                
            try:
                auth_data = {
                    'grant_type': 'client_credentials',
                    'client_id': self.config.AVIANIS_CLIENT_ID,
                    'client_secret': self.config.AVIANIS_CLIENT_SECRET,
                    'client_authentication': 'header'
                }
                
                headers = {
                    'Content-Type': 'application/x-www-form-urlencoded'
                }
                
                response = requests.post(
                    f'{self.config.AVIANIS_BASE_URL}/oauth/token',
                    data=auth_data,
                    headers=headers,
                    timeout=(5, 20)
                )
                
                if response.status_code == 200:
                    token_data = response.json()
                    self.access_token = token_data.get('access_token')
                    self.token_expiry = token_data.get('expires_in', 20)
                    
                    self.headers = {
                        'Authorization': f'Bearer {self.access_token}',
                        'Content-Type': 'application/json'
                    }
                    self.authenticated = True
                    self.auth_timestamp = time.time()
                    logging.info(f"Successfully authenticated with Avianis API (expires in {self.token_expiry}s)")
                    return True
                else:
                    logging.error(f"OAuth authentication failed: {response.status_code} - {response.text}")
                    return False
                    
            except Exception as e:
                logging.error(f"OAuth authentication error: {e}")
                return False
    
    def get_headers(self) -> Dict:
        """Get authentication headers for API requests"""
        with self._lock:
            return self.headers.copy()
    
    def is_authenticated(self) -> bool:
        """Check if authentication is valid (with 5 second buffer)"""
        with self._lock:
            return self.authenticated and (time.time() - self.auth_timestamp) < (self.token_expiry - 5)

# Global authentication manager instance
_auth_manager = None
_auth_lock = threading.Lock()

def get_auth_manager() -> AuthManager:
    """Get or create the global authentication manager"""
    global _auth_manager
    with _auth_lock:
        if _auth_manager is None:
            _auth_manager = AuthManager(Config())
        return _auth_manager

class AvianisAPIClient:
    def __init__(self, auth_manager: AuthManager = None):
        self.config = Config()
        self.base_url = self.config.AVIANIS_BASE_URL.rstrip('/')
        self.api_url_v2 = f"{self.base_url}/connect/v2"
        self.api_url_v1 = f"{self.base_url}/connect/v1"
        self.session = requests.Session()
        self.auth_manager = auth_manager or get_auth_manager()
        self._lock = threading.Lock()
        
        # Optimize session for high-concurrency requests
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=20,
            pool_maxsize=50,
            max_retries=2,
            pool_block=False
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
        self.session.timeout = (5, 20)
        
    def authenticate(self) -> bool:
        """Authenticate using the centralized auth manager"""
        if self.auth_manager.authenticate():
            self.session.headers.update(self.auth_manager.get_headers())
            return True
        return False
    
    def get_data(self, endpoint: str, params: Optional[Dict] = None, timeout: tuple = (5, 20), api_version: str = 'v2') -> Optional[Dict]:
        """Generic method to fetch data from Avianis API"""
        try:
            if not self.auth_manager.is_authenticated():
                if not self.authenticate():
                    return None

            # Select API URL based on version
            api_url = self.api_url_v1 if api_version == 'v1' else self.api_url_v2
            url = f'{api_url}/{endpoint.lstrip("/")}'
            logging.info(f"Making API request to: {url} with params: {params}")
            response = self.session.get(url, params=params, timeout=timeout)

            if response.status_code == 200:
                data = response.json()
                logging.info(f"API request successful: {url} returned {len(data) if isinstance(data, list) else 'non-list'} records")
                return data
            elif response.status_code == 401:
                logging.warning("Unauthorized - attempting to re-authenticate")
                if self.authenticate():
                    response = self.session.get(url, params=params, timeout=timeout)
                    if response.status_code == 200:
                        data = response.json()
                        logging.info(f"API request successful after re-auth: {url} returned {len(data) if isinstance(data, list) else 'non-list'} records")
                        return data

            logging.error(f"API request failed: {response.status_code} - {url} - {response.text}")
            return None

        except Exception as e:
            logging.error(f"Error fetching data from {endpoint}: {e}")
            return None

    def _get_paginated_data(self, endpoint: str, params: Optional[Dict] = None, api_version: str = 'v1', page_size: int = 1000, resource_name: str = 'records', timeout: tuple = (5, 20)) -> Optional[List[Dict]]:
        """Generic method to fetch paginated data from Avianis API

        Args:
            endpoint: API endpoint to call
            params: Base parameters for the API call (will add 'Page' parameter)
            api_version: API version ('v1' or 'v2')
            page_size: Expected page size (default 1000)
            resource_name: Name of resource for logging (e.g., 'quotes', 'flight legs')
            timeout: Timeout tuple (connect_timeout, read_timeout) in seconds (default (5, 20))

        Returns:
            List of all records across all pages, or None if no data
        """
        all_data = []
        page = 1
        params = params or {}

        while True:
            params['Page'] = page

            data = self.get_data(endpoint, params, api_version=api_version, timeout=timeout)
            if not data or len(data) == 0:
                break

            all_data.extend(data)
            logging.info(f"Fetched page {page}: {len(data)} {resource_name} (total: {len(all_data)})")

            # If we got fewer records than the page size, we're done
            if len(data) < page_size:
                break

            page += 1

        return all_data if all_data else None

    def create_worker_client(self):
        """Create a new API client instance for parallel processing that shares authentication"""
        worker_client = AvianisAPIClient(self.auth_manager)
        worker_client.authenticate()
        return worker_client
    
    def get_aircraft(self) -> Optional[List[Dict]]:
        """Fetch aircraft data"""
        return self.get_data('/Aircraft')
    
    def get_aircraft_category(self) -> Optional[List[Dict]]:
        """Fetch aircraft category data"""
        return self.get_data('/AircraftCategory')
    
    def get_aircraft_model(self) -> Optional[List[Dict]]:
        """Fetch aircraft model data"""
        return self.get_data('/AircraftModel')
    
    def get_aircraft_events(self, start_date: str, end_date: str) -> Optional[List[Dict]]:
        """Fetch aircraft events in a date range"""
        params = {
            'StartDate': start_date,
            'EndDate': end_date
        }
        return self.get_data('/aircraftEvent', params)
    
    def get_trip(self, trip_id: str) -> Optional[Dict]:
        """Fetch trip details by trip ID"""
        return self.get_data(f'/trip/{trip_id}/Itinerary?includeCancelledLegs=false', api_version='v2')

    def get_quotes(self, start_date: str, end_date: str) -> Optional[List[Dict]]:
        """Fetch quotes in a date range with pagination

        Note: Uses extended timeout (60s read) as quote endpoint can be slow
        """
        params = {
            'StartDate': start_date,
            'EndDate': end_date
        }
        return self._get_paginated_data('/quote', params, api_version='v1', resource_name='quotes', timeout=(5, 60))

    def get_trips(self, start_date: str, end_date: str) -> Optional[List[Dict]]:
        """Fetch trips in a date range with pagination"""
        params = {
            'StartDate': start_date,
            'EndDate': end_date
        }
        return self._get_paginated_data('/trip', params, api_version='v1', resource_name='trips')

    def get_flight_legs(self, start_date: str, end_date: str) -> Optional[List[Dict]]:
        """Fetch flight legs in a date range with pagination"""
        params = {
            'StartDate': start_date,
            'EndDate': end_date
        }
        return self._get_paginated_data('/flightleg', params, api_version='v1', resource_name='flight legs')
    
    def get_crew_assignment(self, aircraft_id: str, start_date: str, end_date: str) -> Optional[List[Dict]]:
        """Fetch crew assignment for a specific aircraft in a date range"""
        params = {
            'start': start_date,
            'end': end_date
        }
        return self.get_data(f'/Aircraft/{aircraft_id}/CrewAssignment', params)
    
    def get_personnel(self, last_activity_date: str) -> Optional[List[Dict]]:
        """Fetch all personnel with last activity date filter"""
        params = {
            'LastActivityDate': last_activity_date
        }
        return self.get_data('/personnel', params, api_version='v1')
    
    def get_duty_categories(self) -> Optional[List[Dict]]:
        """Fetch all duty categories"""
        return self.get_data('/dutycategory', api_version='v1')
    
    def get_personnel_events(self, last_activity_date: str) -> Optional[List[Dict]]:
        """Fetch all personnel events with last activity date filter and pagination"""
        params = {
            'LastActivityDate': last_activity_date
        }
        return self._get_paginated_data('/personnelEvent', params, api_version='v1', resource_name='personnel events')
    
    def close(self):
        """Close the session"""
        self.session.close()