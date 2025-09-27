import pandas as pd
from sqlalchemy import text
from database import DatabaseManager
import logging
from typing import Optional, Dict, List
from data_utils import safe_get, clean_string, safe_int, parse_iso_datetime
from datetime import datetime
from .airport_loader import AirportLoader

class CrewLoader:
    """Handle loading crew data into MySQL database"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.airport_loader = AirportLoader(db_manager)
        from config import Config
        self.config = Config()
    
    def transform_personnel_data(self, personnel_data: List[Dict]) -> pd.DataFrame:
        """Transform raw Avianis personnel data to crew table format"""
        if not personnel_data:
            return pd.DataFrame()
        
        transformed_records = []
        
        for person in personnel_data:
            first_name = clean_string(safe_get(person, 'firstName'))
            last_name = clean_string(safe_get(person, 'lastName'))
            
            # Build full name from first and last name
            name_parts = [first_name, last_name]
            full_name = ' '.join([part for part in name_parts if part])
            
            record = {
                'code': clean_string(safe_get(person, 'employeeId')) or clean_string(safe_get(person, 'code')),
                'firstname': first_name,
                'lastname': last_name, 
                'name': full_name or clean_string(safe_get(person, 'fullName')),
                'baseairportid': None,  # Will be populated by airport lookup
                'isactive': 1 if safe_get(person, 'isActive', True) else 0,
                'issenior': 0,  # Default to non-senior, could be derived from position/seniority
                'isdomesticonly': 0,  # Default to not domestic only
                'fmsid': safe_get(person, 'id'),  # Store original Avianis ID
                'createtime': datetime.utcnow(),
                'updatedby': 'avianis_etl'
            }
            
            transformed_records.append(record)
        
        return pd.DataFrame(transformed_records)
    
    def generate_crew_code(self, first_name: str, last_name: str) -> Optional[str]:
        """Generate crew code: first 3 chars of last name + first name initial"""
        if last_name and first_name:
            last_name_part = last_name[:3].upper()
            first_name_initial = first_name[0].upper()
            return f"{last_name_part}{first_name_initial}"
        return None
    
    def is_senior_crew(self, date_of_birth: str, senior_age_threshold: int = None) -> bool:
        """Determine if crew member is senior based on age"""
        if not date_of_birth:
            return False
        
        # Use config threshold if not provided
        if senior_age_threshold is None:
            senior_age_threshold = self.config.SENIOR_CREW_AGE_THRESHOLD
        
        try:
            birth_date = parse_iso_datetime(date_of_birth)
            if not birth_date:
                return False
            
            # Calculate age
            today = datetime.utcnow()
            age = today.year - birth_date.year
            
            # Adjust for birthday not yet passed this year
            if today.month < birth_date.month or (today.month == birth_date.month and today.day < birth_date.day):
                age -= 1
            
            return age >= senior_age_threshold
            
        except Exception as e:
            logging.warning(f"Error calculating age from date of birth '{date_of_birth}': {e}")
            return False
    
    def update_crew_base_airport_ids(self, personnel_data: List[Dict]) -> Dict[str, int]:
        """Update baseairportid mapping using airport lookup for crew"""
        if not personnel_data:
            return {}
        
        try:
            # Extract unique crew base/home airport codes from personnel data
            airport_codes = []
            
            for person in personnel_data:
                homebase_airport = clean_string(safe_get(person, 'homebaseAirport'))
                
                if homebase_airport and homebase_airport not in airport_codes:
                    airport_codes.append(homebase_airport)
            
            if not airport_codes:
                logging.info("No airport codes found in personnel data")
                return {}
            
            # Use airport_loader to bulk lookup airport IDs
            airport_mapping = self.airport_loader.bulk_lookup_airport_ids(airport_codes)
            
            logging.info(f"Successfully mapped {len(airport_mapping)} out of {len(airport_codes)} crew airport codes to IDs")
            if len(airport_mapping) < len(airport_codes):
                missing_count = len(airport_codes) - len(airport_mapping)
                missing_codes = [code for code in airport_codes if code not in airport_mapping]
                logging.warning(f"Failed to find {missing_count} crew airports in database: {missing_codes}")
            
            return airport_mapping
            
        except Exception as e:
            logging.error(f"Error looking up crew base airport IDs: {e}")
            return {}
    
    def transform_duty_categories(self, duty_data: List[Dict]) -> pd.DataFrame:
        """Transform duty category data for creweventtype table"""
        if not duty_data:
            return pd.DataFrame()
        
        transformed_records = []
        
        for duty in duty_data:
            record = {
                'code': clean_string(safe_get(duty, 'code')) or clean_string(safe_get(duty, 'name')),
                'description': clean_string(safe_get(duty, 'description')) or clean_string(safe_get(duty, 'name')),
                'isavailable': True,  # Default to available
                'createtime': datetime.utcnow(),
                'updatedby': 'avianis_etl'
            }
            
            transformed_records.append(record)
        
        return pd.DataFrame(transformed_records)
    
    def reset_and_load_duty_categories(self, duty_data: List[Dict]) -> int:
        """Reset creweventtype table and load new duty category data"""
        try:
            if not duty_data:
                logging.info("No duty category data to load")
                return 0
            
            # Transform data
            duty_df = self.transform_duty_categories(duty_data)
            
            if duty_df.empty:
                logging.info("No valid duty category data after transformation")
                return 0
            
            # Reset the table
            session = self.db_manager.get_session()
            session.execute(text("DELETE FROM creweventtype"))
            session.commit()
            session.close()
            
            # Load data (let MySQL auto-increment the id field)
            duty_df.to_sql(
                'creweventtype',
                con=self.db_manager.engine,
                if_exists='append',
                index=False,
                method='multi'
            )
            
            logging.info(f"Successfully loaded {len(duty_df)} duty category records into creweventtype table")
            return len(duty_df)
            
        except Exception as e:
            logging.error(f"Error loading duty category data: {e}")
            raise
    
    def reset_and_load_crew_data(self, personnel_data: List[Dict]) -> int:
        """Reset crew table and load new crew data with airport mapping"""
        try:
            if not personnel_data:
                logging.info("No personnel data to load")
                return 0
            
            # Get airport mappings first
            airport_mapping = self.update_crew_base_airport_ids(personnel_data)
            
            # Transform data
            transformed_records = []
            
            for person in personnel_data:
                first_name = clean_string(safe_get(person, 'firstName'))
                last_name = clean_string(safe_get(person, 'lastName'))
                
                # Build full name from first and last name
                name_parts = [first_name, last_name]
                full_name = ' '.join([part for part in name_parts if part])
                
                # Generate crew code using dedicated method
                crew_code = self.generate_crew_code(
                    first_name, last_name
                )
                
                # Find base airport ID
                homebase_airport = clean_string(safe_get(person, 'homebaseAirport'))
                
                baseairportid = None
                if homebase_airport and homebase_airport in airport_mapping:
                    baseairportid = airport_mapping[homebase_airport]
                    logging.debug(f"Mapped airport {homebase_airport} to ID {baseairportid} for person {safe_get(person, 'id')}")
                
                if not baseairportid and homebase_airport:
                    logging.warning(f"No airport mapping found for person {safe_get(person, 'id')}: homebaseAirport='{homebase_airport}'")
                
                # Determine if crew member is senior based on age
                date_of_birth = safe_get(person, 'dateOfBirth')
                is_senior = self.is_senior_crew(date_of_birth)
                
                record = {
                    'code': crew_code,
                    'firstname': first_name,
                    'lastname': last_name, 
                    'name': full_name or clean_string(safe_get(person, 'fullName')),
                    'baseairportid': baseairportid,
                    'isactive': 1 if safe_get(person, 'active', True) else 0,
                    'issenior': 1 if is_senior else 0,
                    'isdomesticonly': 0,  # Default to not domestic only
                    'fmsid': safe_get(person, 'id'),  # Store original Avianis ID
                    'createtime': datetime.utcnow(),
                    'updatedby': 'avianis_etl'
                }
                
                transformed_records.append(record)
            
            if not transformed_records:
                logging.info("No valid crew data after transformation")
                return 0
            
            crew_df = pd.DataFrame(transformed_records)
            
            # Reset the crew table
            session = self.db_manager.get_session()
            session.execute(text("DELETE FROM crew"))
            session.commit()
            session.close()
            
            # Load data into database (let MySQL auto-increment the id field)
            crew_df.to_sql(
                'crew',
                con=self.db_manager.engine,
                if_exists='append',
                index=False,
                method='multi'
            )
            
            logging.info(f"Successfully loaded {len(crew_df)} crew records")
            return len(crew_df)
            
        except Exception as e:
            logging.error(f"Error loading crew data: {e}")
            raise