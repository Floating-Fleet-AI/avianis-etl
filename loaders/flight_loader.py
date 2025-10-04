import pandas as pd
from sqlalchemy import text
from database import DatabaseManager
import logging
import concurrent.futures
from typing import Optional, Dict, List
from data_utils import safe_get, clean_string, safe_int, safe_float, parse_iso_datetime, generate_stable_id
from datetime import datetime
from .airport_loader import AirportLoader
from transformers.flight_transformer import FlightTransformer
from loaders.crew_assignment_loader import CrewAssignmentLoader
from lookup_service import LookupService

class FlightLoader:
    """Handle loading flight schedule data into movement_temp, movement, and demand tables"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.airport_loader = AirportLoader(db_manager)
        self.flight_transformer = FlightTransformer(db_manager)
        self.crew_assignment_loader = CrewAssignmentLoader(db_manager)
        self.lookup_service = LookupService(db_manager)
    
    
    def load_to_movement_temp(self, movement_records: List[Dict]) -> int:
        """Load movement records to movement_temp table"""
        try:
            if not movement_records:
                logging.info("No movement records to load")
                return 0
            
            # Clear movement_temp table
            session = self.db_manager.get_session()
            session.execute(text("DELETE FROM movement_temp"))
            session.commit()
            session.close()
            
            # Convert to DataFrame and load
            df = pd.DataFrame(movement_records)
            
            # Load data into movement_temp (let MySQL auto-increment the id field)
            df.to_sql(
                'movement_temp',
                con=self.db_manager.engine,
                if_exists='append',
                index=False,
                method='multi'
            )
            
            logging.info(f"Successfully loaded {len(df)} movement records into movement_temp table")
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
            # Criteria: isEmpty=false (isposition=0) and passengerCount > 0
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
    
    def load_crew_assignments(self, crew_assignment_records: List[Dict]) -> int:
        """Load crew assignment records to crewassignment_temp table"""
        try:
            if not crew_assignment_records:
                logging.info("No crew assignment records to load")
                return 0
            
            # Load crew assignments into crewassignment_temp table
            loaded_count = self.crew_assignment_loader.reset_and_load_crew_assignments_temp(crew_assignment_records)
            logging.info(f"Successfully loaded {loaded_count} crew assignment records")
            return loaded_count
                
        except Exception as e:
            logging.error(f"Error processing crew assignments: {e}")
            raise
    
    def get_flight_date_range(self, flight_data: List[Dict]) -> tuple:
        """Extract date range from flight data for crew assignment processing"""
        if not flight_data:
            return None, None
        
        from data_utils import parse_iso_datetime
        dates = []
        
        for flight in flight_data:
            scheduled_departure = parse_iso_datetime(safe_get(flight, 'scheduledDepartureDateUTC'))
            if scheduled_departure:
                dates.append(scheduled_departure.date())
        
        if dates:
            min_date = min(dates)
            max_date = max(dates)
            logging.info(f"Flight data date range: {min_date} to {max_date}")
            return min_date, max_date
        
        return None, None
    
    def process_flight_schedules(self, flight_data: List[Dict]) -> Dict[str, int]:
        """Complete workflow: transform once, then load to movement_temp, port to movement, load qualifying flights to demand, and process crew assignments"""
        results = {}
        
        try:
            # Step 0: Pre-compute all lookups and transform data once
            logging.info("Step 0: Pre-computing lookups and transforming flight data")
            lookup_sets = self.flight_transformer.collect_lookup_sets(flight_data)
            lookups = self.lookup_service.get_bulk_lookups(
                crew_names=lookup_sets['crew_names'],
                aircraft_tail_numbers=lookup_sets['tail_numbers'],
                airport_codes=lookup_sets['airport_codes']
            )
            
            # Single transformation producing both movement and crew assignment records
            transformed_data = self.flight_transformer.transform_flight_data(flight_data, lookups)
            movement_records = transformed_data['movements']
            crew_assignment_records = transformed_data['crew_assignments']
            
            # Step 1: Load movement_temp and crew_assignments in parallel
            logging.info("Step 1: Loading movement_temp and crew assignments in parallel")
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                # Submit both loading operations to run in parallel
                movement_future = executor.submit(self.load_to_movement_temp, movement_records)
                crew_assignment_future = executor.submit(self.load_crew_assignments, crew_assignment_records)
                
                # Wait for both to complete
                temp_count = movement_future.result()
                crew_assignment_count = crew_assignment_future.result()
            
            results['movement_temp_loaded'] = temp_count
            results['crew_assignments_loaded'] = crew_assignment_count
            
            # Step 2: Port data from movement_temp to movement table
            logging.info("Step 2: Porting data from movement_temp to movement table")
            movement_count = self.port_movement_temp_to_movement()
            results['movement_loaded'] = movement_count
            
            # Step 3: Load qualifying flights into demand table
            logging.info("Step 3: Loading qualifying flights into demand table")
            demand_count = self.load_qualifying_flights_to_demand()
            results['demand_loaded'] = demand_count
            
            # Step 4: Transfer crew assignments from temp to target table (create shifts)
            logging.info("Step 4: Transferring crew assignments from temp to target table")
            date_range = self.get_flight_date_range(flight_data)
            if date_range[0] and date_range[1]:
                crew_shifts_count = self.crew_assignment_loader.transfer_temp_to_target(date_range)
                results['crew_shifts_loaded'] = crew_shifts_count
            else:
                logging.warning("Could not determine date range for crew assignment transfer")
                results['crew_shifts_loaded'] = 0
            
            logging.info(f"Flight schedule processing complete: {temp_count} temp, {movement_count} movement, {demand_count} demand, {crew_assignment_count} crew assignment records, {results.get('crew_shifts_loaded', 0)} crew shifts (single transform + parallel loading + shift aggregation)")
            
            return results
            
        except Exception as e:
            logging.error(f"Error in flight schedule processing workflow: {e}")
            raise