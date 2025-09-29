import pandas as pd
from sqlalchemy import text
from database import DatabaseManager
import logging
from typing import Optional, Dict, List
from data_utils import safe_get, clean_string, safe_int, safe_float, parse_iso_datetime, generate_stable_id
from datetime import datetime
from .airport_loader import AirportLoader
from transformers.flight_transformer import FlightTransformer

class FlightLoader:
    """Handle loading flight schedule data into movement_temp, movement, and demand tables"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.airport_loader = AirportLoader(db_manager)
        self.flight_transformer = FlightTransformer(db_manager)
    
    
    def transform_flight_data_to_movement_temp(self, flight_data: List[Dict]) -> List[Dict]:
        """Transform flight leg data to movement_temp table format using FlightTransformer"""
        return self.flight_transformer.transform_flight_to_movement_temp(flight_data)
    
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
                    id, 
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
                WHERE isposition = 0 AND numberpassenger > 0
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