import pandas as pd
from sqlalchemy import text
from database import DatabaseManager
import logging
from typing import Optional, Dict, List
from data_utils import safe_get, clean_string, safe_int, safe_float, parse_iso_datetime
from datetime import datetime
from .airport_loader import AirportLoader
import zlib
import hashlib

class FlightScheduleLoader:
    """Handle loading flight schedule data into movement_temp, movement, and demand tables"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.airport_loader = AirportLoader(db_manager)
    
    @staticmethod
    def generate_stable_id(fms_id: str, min_id: int = 100000, max_id: int = 999999) -> int:
        """Generate stable 6-digit ID using SHA256 for consistent mapping across ETL runs"""
        # Use SHA256 for better distribution and take first 4 bytes as integer
        hash_bytes = hashlib.sha256(fms_id.encode('utf-8')).digest()[:4]
        hash_int = int.from_bytes(hash_bytes, byteorder='big')
        # Map to 6-digit range (100,000 to 999,999 = 900,000 possible values)
        range_size = max_id - min_id + 1
        return (hash_int % range_size) + min_id
    
    def get_crew_lookup_dict(self) -> Dict[str, int]:
        """Create crew lookup dictionary mapping fmsid to crew.id"""
        try:
            session = self.db_manager.get_session()
            
            # Get all crew with their fmsid
            results = session.execute(
                text("SELECT id, fmsid FROM crew WHERE fmsid IS NOT NULL")
            ).fetchall()
            
            crew_dict = {}
            for row in results:
                crew_id, fmsid = row
                if fmsid:
                    crew_dict[fmsid] = crew_id
            
            logging.info(f"Loaded {len(crew_dict)} crew fmsid mappings into dictionary")
            return crew_dict
            
        except Exception as e:
            logging.error(f"Error creating crew lookup dictionary: {e}")
            return {}
        finally:
            session.close()
    
    def get_aircraft_lookup_dict(self) -> Dict[str, int]:
        """Create aircraft lookup dictionary mapping fmsid to aircraft.id"""
        try:
            session = self.db_manager.get_session()
            
            # Get all aircraft with their fmsid
            results = session.execute(
                text("SELECT id, fmsid FROM aircraft WHERE fmsid IS NOT NULL")
            ).fetchall()
            
            aircraft_dict = {}
            for row in results:
                aircraft_id, fmsid = row
                if fmsid:
                    aircraft_dict[fmsid] = aircraft_id
            
            logging.info(f"Loaded {len(aircraft_dict)} aircraft fmsid mappings into dictionary")
            return aircraft_dict
            
        except Exception as e:
            logging.error(f"Error creating aircraft lookup dictionary: {e}")
            return {}
        finally:
            session.close()
    
    def transform_flight_data_to_movement_temp(self, flight_data: List[Dict]) -> List[Dict]:
        """Transform flight leg data to movement_temp table format"""
        if not flight_data:
            return []
        
        # Get lookup dictionaries
        airport_dict = self.airport_loader.get_airport_dictionary()
        crew_dict = self.get_crew_lookup_dict()
        aircraft_dict = self.get_aircraft_lookup_dict()
        
        transformed_records = []
        seen_ids = {}  # Track duplicates: stable_id -> list of flight info
        
        for flight in flight_data:
            try:
                # Extract crew information
                crew_list = safe_get(flight, 'crew', [])
                pic_id = None
                sic_id = None
                pic_name = None
                sic_name = None
                
                for crew_member in crew_list:
                    crew_position = safe_get(crew_member, 'crewPosition', '').lower()
                    crew_fmsid = safe_get(crew_member, 'crewID')
                    first_name = safe_get(crew_member, 'firstName', '')
                    last_name = safe_get(crew_member, 'lastName', '')
                    crew_name = f"{first_name} {last_name}".strip()
                    
                    if crew_position == 'pic':
                        pic_id = crew_dict.get(crew_fmsid) if crew_fmsid else None
                        pic_name = crew_name
                    elif crew_position == 'sic':
                        sic_id = crew_dict.get(crew_fmsid) if crew_fmsid else None
                        sic_name = crew_name
                
                # Extract airport information - only use ICAO codes
                departure_icao = safe_get(flight, 'departureICAO')
                arrival_icao = safe_get(flight, 'arrivalICAO')
                
                from_airport_id = airport_dict.get(departure_icao.upper()) if departure_icao else None
                to_airport_id = airport_dict.get(arrival_icao.upper()) if arrival_icao else None
                
                # Log airport lookups for debugging
                if departure_icao:
                    logging.debug(f"Departure airport lookup: {departure_icao} (ICAO) -> ID {from_airport_id}")
                if arrival_icao:
                    logging.debug(f"Arrival airport lookup: {arrival_icao} (ICAO) -> ID {to_airport_id}")
                
                # Extract aircraft information
                aircraft_fmsid = safe_get(flight, 'aircraftID')
                aircraft_id = aircraft_dict.get(aircraft_fmsid) if aircraft_fmsid else None
                
                # Extract times - convert to datetime objects
                scheduled_departure = parse_iso_datetime(safe_get(flight, 'scheduledDepartureDateUTC'))
                scheduled_arrival = parse_iso_datetime(safe_get(flight, 'scheduledArrivalDateUTC'))
                actual_departure = parse_iso_datetime(safe_get(flight, 'actualDepartureDateUTC'))
                actual_arrival = parse_iso_datetime(safe_get(flight, 'actualArrivalDateUTC'))
                out_blocks = parse_iso_datetime(safe_get(flight, 'outOfBlocksUTC'))
                in_blocks = parse_iso_datetime(safe_get(flight, 'inBlocksUTC'))
                
                # Calculate offtime and ontime with 6-minute padding
                offtime = None
                ontime = None
                if scheduled_departure:
                    offtime = scheduled_departure + pd.Timedelta(minutes=6)
                if scheduled_arrival:
                    ontime = scheduled_arrival - pd.Timedelta(minutes=6)
                
                fms_id = safe_get(flight, 'id')
                stable_id = self.generate_stable_id(fms_id)
                
                # Track for duplicate detection
                flight_info = {
                    'fms_id': fms_id,
                    'tripID': safe_get(flight, 'tripID'),
                    'tripNumber': safe_get(flight, 'tripNumber'),
                    'tailNumber': safe_get(flight, 'tailNumber'),
                    'departure': departure_icao,
                    'arrival': arrival_icao,
                    'scheduled_departure': scheduled_departure,
                    'scheduled_arrival': scheduled_arrival,
                    'status': safe_get(flight, 'status')
                }
                
                if stable_id in seen_ids:
                    seen_ids[stable_id].append(flight_info)
                    logging.warning(f"Duplicate stable_id {stable_id} detected!")
                    logging.warning(f"Previous flight: {seen_ids[stable_id][0]}")
                    logging.warning(f"Current flight: {flight_info}")
                else:
                    seen_ids[stable_id] = [flight_info]
                
                record = {
                    'id': stable_id,
                    'demandid': stable_id,
                    'fromairportid': from_airport_id,
                    'toairportid': to_airport_id,
                    'fromfboid': safe_get(flight, 'departureFBOHandlerID'),
                    'tofboid': safe_get(flight, 'arrivalFBOHandlerID'),
                    'aircraftid': aircraft_id,
                    'outtime': scheduled_departure,
                    'offtime': offtime,
                    'ontime': ontime,
                    'intime': scheduled_arrival,
                    'actualouttime': actual_departure,
                    'actualintime': actual_arrival,
                    'flighttime': safe_float(safe_get(flight, 'actualFlightTime')),
                    'blocktime': safe_float(safe_get(flight, 'actualBlockTime')),
                    'status': clean_string(safe_get(flight, 'status')),
                    'picid': pic_id,
                    'sicid': sic_id,
                    'fmsid': safe_get(flight, 'id'),
                    'createtime': datetime.utcnow(),
                    'fromairport': departure_icao,
                    'toairport': arrival_icao,
                    'tailnumber': clean_string(safe_get(flight, 'tailNumber')),
                    'pic': pic_name,
                    'sic': sic_name,
                    'numberpassenger': safe_int(safe_get(flight, 'passengerCount')),
                    'tripnumber': clean_string(safe_get(flight, 'tripNumber')),
                    'isposition': 1 if safe_get(flight, 'isEmpty', False) else 0,
                    'tripid': safe_get(flight, 'tripID'),
                }
                
                transformed_records.append(record)
                
            except Exception as e:
                logging.error(f"Error transforming flight record {safe_get(flight, 'id', 'unknown')}: {e}")
                continue
        
        # Log duplicate analysis
        duplicate_count = sum(1 for flights in seen_ids.values() if len(flights) > 1)
        total_duplicates = sum(len(flights) - 1 for flights in seen_ids.values() if len(flights) > 1)
        
        if duplicate_count > 0:
            logging.warning(f"Found {duplicate_count} unique stable_ids with duplicates, totaling {total_duplicates} duplicate records")
            logging.warning("Duplicate stable_ids and their flight details:")
            for stable_id, flights in seen_ids.items():
                if len(flights) > 1:
                    logging.warning(f"Stable ID {stable_id} has {len(flights)} flights:")
                    for i, flight in enumerate(flights):
                        logging.warning(f"  Flight {i+1}: {flight}")
        
        logging.info(f"Transformed {len(transformed_records)} flight records for movement_temp")
        return transformed_records
    
    def load_to_movement_temp(self, flight_data: List[Dict]) -> int:
        """Load flight data to movement_temp table"""
        try:
            if not flight_data:
                logging.info("No flight data to load")
                return 0
            
            # Transform data
            transformed_records = self.transform_flight_data_to_movement_temp(flight_data)
            
            if not transformed_records:
                logging.info("No valid flight data after transformation")
                return 0
            
            # Clear movement_temp table
            session = self.db_manager.get_session()
            session.execute(text("DELETE FROM movement_temp"))
            session.commit()
            session.close()
            
            # Convert to DataFrame and load
            df = pd.DataFrame(transformed_records)
            
            # Load data into movement_temp (let MySQL auto-increment the id field)
            df.to_sql(
                'movement_temp',
                con=self.db_manager.engine,
                if_exists='append',
                index=False,
                method='multi'
            )
            
            logging.info(f"Successfully loaded {len(df)} flight records into movement_temp table")
            return len(df)
            
        except Exception as e:
            logging.error(f"Error loading flight data to movement_temp: {e}")
            raise
    
    def port_movement_temp_to_movement(self) -> int:
        """Port data from movement_temp to movement table"""
        try:
            session = self.db_manager.get_session()
            
            # First, clear the movement table
            session.execute(text("DELETE FROM movement"))
            
            # Copy data from movement_temp to movement (excluding the auto-increment id)
            copy_query = text("""
                INSERT INTO movement (
                    id, demandid, fromairportid, toairportid, fromfboid, tofboid, aircraftid,
                    outtime, offtime, ontime, intime, actualouttime, actualofftime, 
                    actualontime, actualintime, flighttime, blocktime, status, picid, sicid,
                    fmsversion, fmsid, createtime, pic, sic, fromairport, toairport,
                    tailnumber, isowner, isaclocked, iscrewlocked, isposition, tripnumber, numberpassenger
                )
                SELECT 
                    id, demandid, fromairportid, toairportid, fromfboid, tofboid, aircraftid,
                    outtime, offtime, ontime, intime, actualouttime, actualofftime, 
                    actualontime, actualintime, flighttime, blocktime, status, picid, sicid,
                    fmsversion, fmsid, createtime, pic, sic, fromairport, toairport,
                    tailnumber, isowner, isaclocked, iscrewlocked, isposition, tripnumber, numberpassenger
                FROM movement_temp
            """)
            
            result = session.execute(copy_query)
            session.commit()
            
            rows_copied = result.rowcount
            logging.info(f"Successfully ported {rows_copied} records from movement_temp to movement")
            
            return rows_copied
            
        except Exception as e:
            logging.error(f"Error porting data from movement_temp to movement: {e}")
            session.rollback()
            raise
        finally:
            session.close()
    
    def load_qualifying_flights_to_demand(self) -> int:
        """Load flights that are not empty and have passengerCount > 2 into demand table"""
        try:
            session = self.db_manager.get_session()
            
            # First, clear the demand table
            session.execute(text("DELETE FROM demand"))
            
            # Insert qualifying flights into demand table using movement_temp data
            # Criteria: isEmpty=false (isposition=0) and passengerCount > 2
            demand_query = text("""
                INSERT INTO demand (
                    id, legnumber, tripnumber, requestaircrafttypeid, requestaircraftcategoryid,
                    fromairportid, toairportid, fromfboid, tofboid, aircraftid, outtime, intime,
                    primarypaxid, numberpassenger, flighttime, blocktime, status, flexbefore, flexafter,
                    isowner, iswholesale, isofffleet, fmsversion, fmsid, createtime
                )
                SELECT 
                    demandid as id, 
                    1 as legnumber, 
                    tripnumber, 
                    NULL as requestaircrafttypeid, 
                    NULL as requestaircraftcategoryid, 
                    fromairportid, 
                    toairportid, 
                    fromfboid, 
                    tofboid,
                    aircraftid, 
                    outtime, 
                    intime, 
                    NULL as primarypaxid, 
                    numberpassenger, 
                    flighttime, 
                    blocktime, 
                    status, 
                    0 as flexbefore, 
                    0 as flexafter, 
                    isowner, 
                    0 as iswholesale,
                    0 as isofffleet, 
                    fmsversion, 
                    tripid as fmsid, 
                    createtime
                FROM movement_temp 
                WHERE isposition = 0 AND numberpassenger > 2
            """)
            
            result = session.execute(demand_query)
            session.commit()
            
            rows_inserted = result.rowcount
            logging.info(f"Successfully loaded {rows_inserted} qualifying flights into demand table")
            
            return rows_inserted
            
        except Exception as e:
            logging.error(f"Error loading qualifying flights to demand: {e}")
            session.rollback()
            raise
        finally:
            session.close()
    
    def process_flight_schedules(self, flight_data: List[Dict]) -> Dict[str, int]:
        """Complete workflow: load to movement_temp, port to movement, and load qualifying flights to demand"""
        results = {}
        
        try:
            # Step 1: Load flight data into movement_temp table
            logging.info("Step 1: Loading flight data into movement_temp table")
            temp_count = self.load_to_movement_temp(flight_data)
            results['movement_temp_loaded'] = temp_count
            
            # Step 2: Port data from movement_temp to movement table
            logging.info("Step 2: Porting data from movement_temp to movement table")
            movement_count = self.port_movement_temp_to_movement()
            results['movement_loaded'] = movement_count
            
            # Step 3: Load qualifying flights into demand table
            logging.info("Step 3: Loading qualifying flights into demand table")
            demand_count = self.load_qualifying_flights_to_demand()
            results['demand_loaded'] = demand_count
            
            logging.info(f"Flight schedule processing complete: {temp_count} temp, {movement_count} movement, {demand_count} demand records")
            
            return results
            
        except Exception as e:
            logging.error(f"Error in flight schedule processing workflow: {e}")
            raise