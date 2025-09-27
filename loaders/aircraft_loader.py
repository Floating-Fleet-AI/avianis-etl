import pandas as pd
from sqlalchemy import text
from database import DatabaseManager
import logging
import zlib
from typing import Dict, List, Optional
from data_utils import safe_get, clean_string, safe_int
from datetime import datetime
from .airport_loader import AirportLoader
from transformers.aircraft_category_transformer import AircraftCategoryTransformer

class AircraftLoader:
    """Handle loading aircraft-related data into MySQL database"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.airport_loader = AirportLoader(db_manager)
        self.category_transformer = AircraftCategoryTransformer()
        self.category_id_map = {}  # Map category names to IDs
        self.type_id_map = {}      # Map aircraft type names to IDs
    
    @staticmethod
    def generate_stable_id(identifier: str, min_id: int = 1000, max_range: int = 10000) -> int:
        """Generate stable ID using CRC32 for consistent mapping across ETL runs"""
        crc = zlib.crc32(identifier.encode('utf-8')) & 0xffffffff  # Ensure positive
        return (crc % max_range) + min_id
        
    def bulk_load_data(self, df: pd.DataFrame, table_name: str) -> int:
        """Bulk load data using pandas to_sql for better performance"""
        if df.empty:
            logging.info(f"No data to load for table {table_name}")
            return 0
        
        try:
            # Use pandas to_sql for bulk loading
            rows_loaded = df.to_sql(
                name=table_name,
                con=self.db_manager.engine,
                if_exists='append',
                index=False,
                method='multi'
            )
            
            logging.info(f"Bulk loaded {rows_loaded} records to {table_name}")
            return rows_loaded
            
        except Exception as e:
            logging.error(f"Error bulk loading data to {table_name}: {e}")
            raise
    
    def load_aircraft_categories(self, category_data: List[Dict], aircraft_data: List[Dict]) -> int:
        """Load aircraftcategory table from /AircraftCategory endpoint, sorted by capacity"""
        if not category_data:
            logging.info("No aircraft category data to load")
            return 0
        
        try:
            session = self.db_manager.get_session()
            
            # Reset the table
            session.execute(text("DELETE FROM aircraftcategory"))
            session.commit()
            session.close()
            
            # Sort categories by capacity using transformer
            sorted_categories = self.category_transformer.sort_categories_by_capacity(category_data, aircraft_data)
            
            # Transform category data with auto-increment IDs
            transformed_records = []
            for category, category_id, mean_capacity in sorted_categories:
                category_name = clean_string(safe_get(category, 'name'))
                
                # Store mapping for later use
                self.category_id_map[category_name] = category_id
                
                record = {
                    'id': category_id,
                    'name': category_name,
                    'code': category_name,  # Use name as code since no code provided
                    'fmsid': safe_get(category, 'id'),  # Store original Avianis ID
                    'createtime': datetime.utcnow()
                }
                transformed_records.append(record)
            
            if transformed_records:
                category_df = pd.DataFrame(transformed_records)
                self.bulk_load_data(category_df, 'aircraftcategory')
                logging.info(f"Successfully loaded {len(transformed_records)} aircraft categories sorted by capacity")
                return len(transformed_records)
            
            return 0
            
        except Exception as e:
            logging.error(f"Error loading aircraft categories: {e}")
            raise
    
    def load_aircraft_types(self, model_data: List[Dict]) -> int:
        """Load aircrafttype table from /AircraftModel endpoint"""
        if not model_data:
            logging.info("No aircraft model data to load")
            return 0
        
        try:
            session = self.db_manager.get_session()
            
            # Reset the table
            session.execute(text("DELETE FROM aircrafttype"))
            session.commit()
            session.close()
            
            # Transform model data
            transformed_records = []
            for model in model_data:
                type_id = self.generate_stable_id(safe_get(model, 'id'))
                type_name = clean_string(safe_get(model, 'name'))
                
                # Store mapping for later use
                self.type_id_map[type_name] = type_id
                
                record = {
                    'id': type_id,
                    'name': type_name,
                    'description': f"{clean_string(safe_get(model, 'manufacturer'))} {type_name}",
                    'code': clean_string(safe_get(model, 'code')),
                    'fmsid': safe_get(model, 'id'),  # Store original Avianis ID
                    'createtime': datetime.utcnow(),
                    # aircraftcategoryid will be set when we have the relationship data
                }
                transformed_records.append(record)
            
            if transformed_records:
                type_df = pd.DataFrame(transformed_records)
                self.bulk_load_data(type_df, 'aircrafttype')
                logging.info(f"Successfully loaded {len(transformed_records)} aircraft types")
                return len(transformed_records)
            
            return 0
            
        except Exception as e:
            logging.error(f"Error loading aircraft types: {e}")
            raise
    
    def update_base_airport_ids(self, aircraft_data: List[Dict]) -> Dict[str, int]:
        """Update baseairportid mapping using airport lookup"""
        if not aircraft_data:
            return {}
        
        try:
            # Extract unique homebase codes from aircraft data
            homebase_codes = []
            for aircraft in aircraft_data:
                if safe_get(aircraft, 'active') and safe_get(aircraft, 'managed'):
                    homebase = clean_string(safe_get(aircraft, 'homebase'))
                    if homebase and homebase not in homebase_codes:
                        homebase_codes.append(homebase)
            
            if not homebase_codes:
                logging.info("No homebase codes found in aircraft data")
                return {}
            
            # Use airport_loader to bulk lookup airport IDs
            airport_mapping = self.airport_loader.bulk_lookup_airport_ids(homebase_codes)
            
            logging.info(f"Successfully mapped {len(airport_mapping)} out of {len(homebase_codes)} homebase airport codes to IDs")
            if len(airport_mapping) < len(homebase_codes):
                missing_count = len(homebase_codes) - len(airport_mapping)
                missing_codes = [code for code in homebase_codes if code not in airport_mapping]
                logging.warning(f"Failed to find {missing_count} homebase airports in database: {missing_codes}")
            
            return airport_mapping
            
        except Exception as e:
            logging.error(f"Error looking up base airport IDs: {e}")
            return {}

    def load_aircraft(self, aircraft_data: List[Dict]) -> int:
        """Load aircraft table from /Aircraft endpoint"""
        if not aircraft_data:
            logging.info("No aircraft data to load")
            return 0
        
        try:
            # Filter for active and managed aircraft only
            filtered_aircraft = [
                aircraft for aircraft in aircraft_data 
                if safe_get(aircraft, 'active') is True and safe_get(aircraft, 'managed') is True
            ]
            
            if not filtered_aircraft:
                logging.info("No active and managed aircraft found")
                return 0
            
            # Get base airport ID mappings
            airport_mapping = self.update_base_airport_ids(aircraft_data)
            
            session = self.db_manager.get_session()
            
            # Reset the table
            session.execute(text("DELETE FROM aircraft"))
            session.commit()
            session.close()
            
            # Transform aircraft data
            transformed_records = []
            for aircraft in filtered_aircraft:
                aircraft_id = self.generate_stable_id(safe_get(aircraft, 'id'))
                aircraft_type_name = clean_string(safe_get(aircraft, 'aircraftType'))
                homebase = clean_string(safe_get(aircraft, 'homebase'))
                
                # Look up aircrafttypeid from our mapping
                aircrafttypeid = self.type_id_map.get(aircraft_type_name)
                if not aircrafttypeid:
                    logging.warning(f"Aircraft type '{aircraft_type_name}' not found in type mapping")
                
                # Look up baseairportid from airport mapping
                baseairportid = airport_mapping.get(homebase) if homebase else None
                if homebase and not baseairportid:
                    logging.warning(f"Base airport '{homebase}' not found in airport mapping")
                
                record = {
                    'id': aircraft_id,
                    'serialnumber': clean_string(safe_get(aircraft, 'serialNumber') or safe_get(aircraft, 'displayTypeCode')),
                    'tailnumber': clean_string(safe_get(aircraft, 'tailNumber')),
                    'aircrafttypeid': aircrafttypeid,
                    'maxpax': safe_int(safe_get(aircraft, 'capacity')),
                    'ownername': clean_string(safe_get(aircraft, 'vendorName')),
                    'operatorid': safe_get(aircraft, 'vendorID'), 
                    'baseairportid': baseairportid,
                    'isactive': 1 if safe_get(aircraft, 'active') else 0,
                    'fmsid': safe_get(aircraft, 'id'),  # Store original Avianis ID
                    'createtime': datetime.utcnow(),
                    # doc, isonep, istwop not available in Avianis response
                }
                transformed_records.append(record)
            
            if transformed_records:
                aircraft_df = pd.DataFrame(transformed_records)
                self.bulk_load_data(aircraft_df, 'aircraft')
                logging.info(f"Successfully loaded {len(transformed_records)} aircraft records")
                return len(transformed_records)
            
            return 0
            
        except Exception as e:
            logging.error(f"Error loading aircraft data: {e}")
            raise
    
    def update_aircraft_type_categories(self, aircraft_data: List[Dict]) -> int:
        """Update aircrafttype table with category relationships based on aircraft data"""
        if not aircraft_data:
            return 0
        
        try:
            session = self.db_manager.get_session()
            
            # Build mapping of aircraft type to category from aircraft data
            type_category_map = {}
            for aircraft in aircraft_data:
                if safe_get(aircraft, 'active') and safe_get(aircraft, 'managed'):
                    aircraft_type = clean_string(safe_get(aircraft, 'aircraftType'))
                    aircraft_category = clean_string(safe_get(aircraft, 'aircraftCategory'))
                    
                    if aircraft_type and aircraft_category:
                        type_category_map[aircraft_type] = aircraft_category
            
            # Track missing mappings
            missing_types = []
            missing_categories = []
            
            # Update aircrafttype records with category IDs
            updates_made = 0
            for type_name, category_name in type_category_map.items():
                type_id = self.type_id_map.get(type_name)
                category_id = self.category_id_map.get(category_name)
                
                if not type_id:
                    missing_types.append(type_name)
                if not category_id:
                    missing_categories.append(category_name)
                
                if type_id and category_id:
                    session.execute(
                        text("UPDATE aircrafttype SET aircraftcategoryid = :category_id WHERE id = :type_id"),
                        {'category_id': category_id, 'type_id': type_id}
                    )
                    updates_made += 1
            
            # Log issues with missing mappings
            if missing_types:
                logging.warning(f"Aircraft types not found in type_id_map: {set(missing_types)}")
            
            if missing_categories:
                logging.warning(f"Aircraft categories not found in category_id_map: {set(missing_categories)}")
            
            session.commit()
            session.close()
            
            logging.info(f"Updated {updates_made} out of {len(type_category_map)} aircraft types with category relationships")
            return updates_made
            
        except Exception as e:
            logging.error(f"Error updating aircraft type categories: {e}")
            raise
    
    def reset_and_load_all_aircraft_data(self, category_data: List[Dict], model_data: List[Dict], aircraft_data: List[Dict]) -> Dict[str, int]:
        """Load all aircraft-related data in proper order"""
        try:
            results = {}
            
            # Step 1: Load aircraft categories (sorted by capacity from aircraft data)
            results['categories'] = self.load_aircraft_categories(category_data, aircraft_data)
            
            # Step 2: Load aircraft types (models)
            results['types'] = self.load_aircraft_types(model_data)
            
            # Step 3: Update aircraft types with category relationships
            results['type_category_updates'] = self.update_aircraft_type_categories(aircraft_data)
            
            # Step 4: Load aircraft
            results['aircraft'] = self.load_aircraft(aircraft_data)
            
            logging.info(f"Aircraft data loading completed: {results}")
            return results
            
        except Exception as e:
            logging.error(f"Error loading aircraft data: {e}")
            raise