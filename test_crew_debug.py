#!/usr/bin/env python3

import logging
import sys
from config import Config
from avianis_api import AvianisAPIClient, get_auth_manager
from data_utils import DateRangeManager
from loaders.crew_loader import CrewLoader
from database import DatabaseManager

# Set up simple console logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

def test_crew_debug():
    try:
        logging.info("Starting crew debug test...")
        
        # Initialize components
        config = Config(operator='test')
        auth_manager = get_auth_manager()
        api_client = AvianisAPIClient(auth_manager)
        db_manager = DatabaseManager()
        crew_loader = CrewLoader(db_manager)
        date_manager = DateRangeManager(config)
        
        # Test authentication
        logging.info("Testing authentication...")
        if not api_client.authenticate():
            logging.error("Failed to authenticate with Avianis API")
            return
        
        logging.info("Authentication successful!")
        
        # Test personnel API call
        last_activity_date = date_manager.get_last_activity_date(True)
        logging.info(f"Fetching personnel with last activity date: {last_activity_date}")
        
        personnel_data = api_client.get_personnel(last_activity_date)
        if personnel_data:
            logging.info(f"Retrieved {len(personnel_data)} personnel records")
            
            # Show first record structure
            if len(personnel_data) > 0:
                first_person = personnel_data[0]
                logging.info(f"First person keys: {list(first_person.keys())}")
                logging.info(f"First person sample: {dict(list(first_person.items())[:5])}")
            
            # Test airport lookup
            logging.info("Testing airport lookup...")
            airport_mapping = crew_loader.update_crew_base_airport_ids(personnel_data)
            logging.info(f"Airport mapping result: {airport_mapping}")
            
        else:
            logging.warning("No personnel data received from API")
        
        # Test duty categories
        duty_data = api_client.get_duty_categories()
        if duty_data:
            logging.info(f"Retrieved {len(duty_data)} duty categories")
        else:
            logging.warning("No duty category data received from API")
            
    except Exception as e:
        logging.error(f"Error in crew debug test: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_crew_debug()