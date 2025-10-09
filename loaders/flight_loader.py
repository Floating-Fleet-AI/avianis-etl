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

    def __init__(self, db_manager: DatabaseManager, api_client=None):
        self.db_manager = db_manager
        self.airport_loader = AirportLoader(db_manager)
        self.flight_transformer = FlightTransformer(db_manager)
        self.crew_assignment_loader = CrewAssignmentLoader(db_manager)
        self.lookup_service = LookupService(db_manager)
        self.api_client = api_client

    def _clear_table(self, session, table_name: str, is_initial: bool, start_date: str = None, end_date: str = None, date_column: str = 'outtime'):
        """Clear table records based on load type

        Args:
            session: Database session
            table_name: Name of table to clear
            is_initial: If True, truncate entire table; if False, delete only date range
            start_date: Start date for incremental load (ISO format)
            end_date: End date for incremental load (ISO format)
            date_column: Column name to use for date filtering
        """
        if is_initial:
            # Truncate entire table for initial load
            truncate_query = text(f"TRUNCATE TABLE {table_name}")
            session.execute(truncate_query)
            logging.info(f"Truncated {table_name} table (initial load)")
        else:
            # Delete only records in the exact datetime range used for API query
            # This ensures we only delete records that will be replaced
            parsed_start_date = parse_iso_datetime(start_date)
            parsed_end_date = parse_iso_datetime(end_date)

            # Format as MySQL datetime: YYYY-MM-DD HH:MM:SS
            mysql_start = parsed_start_date.strftime('%Y-%m-%d %H:%M:%S')
            mysql_end = parsed_end_date.strftime('%Y-%m-%d %H:%M:%S')

            delete_query = text(f"""
                DELETE FROM {table_name}
                WHERE {date_column} >= :start_date AND {date_column} <= :end_date
            """)
            delete_result = session.execute(delete_query, {
                'start_date': mysql_start,
                'end_date': mysql_end
            })
            deleted_count = delete_result.rowcount
            logging.info(f"Deleted {deleted_count} existing records from {table_name} in datetime range {mysql_start} to {mysql_end}")
        session.commit()
    
    
    def load_to_movement_temp(self, movement_records: List[Dict]) -> int:
        """Load movement records to movement_temp table

        Note: Always clears entire movement_temp table since it's a staging table
        """
        try:
            if not movement_records:
                logging.info("No movement records to load")
                return 0

            # Clear movement_temp table (always truncate staging table)
            session = self.db_manager.get_session()
            session.execute(text("TRUNCATE TABLE movement_temp"))
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
    
    def port_movement_temp_to_movement(self, is_initial: bool, start_date: str = None, end_date: str = None) -> int:
        """Port data from movement_temp to movement table"""
        try:
            session = self.db_manager.get_session()

            # Clear the movement table based on load type
            self._clear_table(session, 'movement', is_initial, start_date, end_date, 'outtime')

            # Upsert data from movement_temp to movement
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
                FROM movement_temp AS new
                ON DUPLICATE KEY UPDATE
                    demandid = new.demandid,
                    fromairportid = new.fromairportid,
                    toairportid = new.toairportid,
                    fromfboid = new.fromfboid,
                    tofboid = new.tofboid,
                    aircraftid = new.aircraftid,
                    outtime = new.outtime,
                    offtime = new.offtime,
                    ontime = new.ontime,
                    intime = new.intime,
                    actualouttime = new.actualouttime,
                    actualofftime = new.actualofftime,
                    actualontime = new.actualontime,
                    actualintime = new.actualintime,
                    flighttime = new.flighttime,
                    blocktime = new.blocktime,
                    status = new.status,
                    picid = new.picid,
                    sicid = new.sicid,
                    fmsversion = new.fmsversion,
                    fmsid = new.fmsid,
                    createtime = new.createtime,
                    pic = new.pic,
                    sic = new.sic,
                    fromairport = new.fromairport,
                    toairport = new.toairport,
                    tailnumber = new.tailnumber,
                    isowner = new.isowner,
                    isaclocked = new.isaclocked,
                    iscrewlocked = new.iscrewlocked,
                    isposition = new.isposition,
                    tripnumber = new.tripnumber,
                    numberpassenger = new.numberpassenger
            """)

            result = session.execute(copy_query)
            session.commit()

            rows_affected = result.rowcount
            logging.info(f"Successfully ported records from movement_temp to movement: {rows_affected} rows affected (inserted or updated)")

            return rows_affected

        except Exception as e:
            logging.error(f"Error porting data from movement_temp to movement: {e}")
            session.rollback()
            raise
        finally:
            session.close()
    
    def load_qualifying_flights_to_demand(self, is_initial: bool, start_date: str = None, end_date: str = None) -> int:
        """Load flights that are not empty into demand table"""
        try:
            session = self.db_manager.get_session()

            # Clear the demand table based on load type
            self._clear_table(session, 'demand', is_initial, start_date, end_date, 'outtime')

            # Upsert qualifying flights into demand table from movement_temp
            # Criteria: isEmpty=false (isposition=0)
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
                FROM movement_temp AS new
                WHERE isposition = 0
                ON DUPLICATE KEY UPDATE
                    legnumber = VALUES(legnumber),
                    tripnumber = new.tripnumber,
                    requestaircrafttypeid = VALUES(requestaircrafttypeid),
                    requestaircraftcategoryid = VALUES(requestaircraftcategoryid),
                    fromairportid = new.fromairportid,
                    toairportid = new.toairportid,
                    fromfboid = new.fromfboid,
                    tofboid = new.tofboid,
                    aircraftid = new.aircraftid,
                    outtime = new.outtime,
                    intime = new.intime,
                    primarypaxid = VALUES(primarypaxid),
                    numberpassenger = new.numberpassenger,
                    flighttime = new.flighttime,
                    blocktime = new.blocktime,
                    status = new.status,
                    flexbefore = VALUES(flexbefore),
                    flexafter = VALUES(flexafter),
                    isowner = new.isowner,
                    iswholesale = VALUES(iswholesale),
                    isofffleet = VALUES(isofffleet),
                    fmsversion = new.fmsversion,
                    fmsid = new.tripid,
                    createtime = new.createtime
            """)

            result = session.execute(demand_query)
            session.commit()

            rows_affected = result.rowcount
            logging.info(f"Successfully loaded qualifying flights into demand table: {rows_affected} rows affected (inserted or updated)")

            return rows_affected

        except Exception as e:
            logging.error(f"Error loading qualifying flights to demand: {e}")
            session.rollback()
            raise
        finally:
            session.close()

    def _build_aircraft_lookups(self, session) -> Dict:
        """Build lookup dictionaries for aircraft types and categories"""
        # Lookup by FMSID (from API)
        type_query = text("SELECT id, fmsid FROM aircrafttype WHERE fmsid IS NOT NULL")
        type_result = session.execute(type_query)
        type_by_fmsid = {row[1]: row[0] for row in type_result.fetchall()}

        category_query = text("SELECT id, fmsid FROM aircraftcategory WHERE fmsid IS NOT NULL")
        category_result = session.execute(category_query)
        category_by_fmsid = {row[1]: row[0] for row in category_result.fetchall()}

        # Lookup by name (for fallback)
        type_by_name_query = text("SELECT id, name, aircraftcategoryid FROM aircrafttype WHERE name IS NOT NULL")
        type_by_name_result = session.execute(type_by_name_query)
        type_by_name = {row[1]: {'id': row[0], 'categoryid': row[2]} for row in type_by_name_result.fetchall()}

        return {
            'type_by_fmsid': type_by_fmsid,
            'category_by_fmsid': category_by_fmsid,
            'type_by_name': type_by_name
        }

    def _extract_aircraft_info_from_leg(self, leg: Dict, lookups: Dict) -> tuple:
        """Extract aircraft type and category IDs from a flight leg

        Returns:
            tuple: (type_id, category_id, used_fallback)
        """
        type_id = None
        category_id = None
        used_fallback = False

        # Primary: Try flightLegDemandRequest
        demand_request = leg.get('flightLegDemandRequest')
        if demand_request and isinstance(demand_request, dict):
            aircraft_model_id = demand_request.get('aircraftModelID')
            aircraft_category_id = demand_request.get('aircraftCategoryID')

            type_id = lookups['type_by_fmsid'].get(aircraft_model_id)
            category_id = lookups['category_by_fmsid'].get(aircraft_category_id)

        # Fallback: Use aircraft.aircraftType name lookup
        if not type_id and not category_id:
            aircraft = leg.get('aircraft', {})
            if aircraft:
                aircraft_type_name = aircraft.get('aircraftType')
                if aircraft_type_name:
                    type_info = lookups['type_by_name'].get(aircraft_type_name)
                    if type_info:
                        type_id = type_info['id']
                        category_id = type_info['categoryid']
                        used_fallback = True

        return type_id, category_id, used_fallback

    def _process_trip_legs(self, tripid: str, legs: List[Dict], lookups: Dict) -> List[Dict]:
        """Process all legs for a trip and extract aircraft info

        Returns:
            List of updates: [{'demandid': ..., 'type_id': ..., 'category_id': ...}, ...]
        """
        updates = []

        trip_data = self.api_client.get_trip(tripid)
        if not trip_data:
            logging.warning(f"No trip data returned for tripid {tripid}")
            return updates

        flight_legs = trip_data if isinstance(trip_data, list) else []
        if not flight_legs:
            logging.warning(f"No flight legs in trip {tripid}")
            return updates

        # Create leg lookup by ID for faster matching
        legs_by_id = {leg.get('id'): leg for leg in flight_legs}

        for leg_info in legs:
            fmsid = leg_info['fmsid']
            demandid = leg_info['demandid']

            matching_leg = legs_by_id.get(fmsid)
            if not matching_leg:
                logging.warning(f"Could not find flight leg {fmsid} in trip {tripid}")
                continue

            type_id, category_id, _ = self._extract_aircraft_info_from_leg(matching_leg, lookups)

            if type_id or category_id:
                updates.append({
                    'demandid': demandid,
                    'type_id': type_id,
                    'category_id': category_id
                })

        return updates

    def populate_demand_aircraft_requests(self) -> int:
        """Populate requestAircraftTypeId and requestAircraftCategoryId in demand table

        This method:
        1. Queries demand records from movement_temp
        2. Fetches trip details from API
        3. Extracts aircraft type/category info (with fallback to aircraft.aircraftType)
        4. Updates demand table with the extracted info
        """
        if not self.api_client:
            logging.warning("API client not provided, skipping demand aircraft request population")
            return 0

        session = None
        try:
            session = self.db_manager.get_session()

            # Step 1: Query demand records
            query = text("""
                SELECT DISTINCT demandid, fmsid, tripid
                FROM movement_temp
                WHERE demandid IS NOT NULL AND tripid IS NOT NULL
            """)
            result = session.execute(query)
            records = result.fetchall()

            if not records:
                logging.info("No demand records found in movement_temp")
                return 0

            logging.info(f"Found {len(records)} demand records to process")

            # Step 2: Build lookups
            lookups = self._build_aircraft_lookups(session)

            # Step 3: Group records by trip
            trips_map = {}
            for demandid, fmsid, tripid in records:
                if tripid not in trips_map:
                    trips_map[tripid] = []
                trips_map[tripid].append({'demandid': demandid, 'fmsid': fmsid})

            logging.info(f"Processing {len(trips_map)} unique trips")

            # Step 4: Process trips and collect updates
            all_updates = []
            for tripid, legs in trips_map.items():
                try:
                    updates = self._process_trip_legs(tripid, legs, lookups)
                    all_updates.extend(updates)
                except Exception as e:
                    logging.error(f"Error processing trip {tripid}: {e}")
                    continue

            if not all_updates:
                logging.info("No aircraft request updates to apply")
                return 0

            # Step 5: Apply updates to demand table
            update_count = 0
            for update in all_updates:
                try:
                    update_query = text("""
                        UPDATE demand
                        SET requestaircrafttypeid = :type_id,
                            requestaircraftcategoryid = :category_id
                        WHERE id = :demandid
                    """)
                    session.execute(update_query, update)
                    update_count += 1
                except Exception as e:
                    logging.error(f"Error updating demand {update['demandid']}: {e}")
                    continue

            session.commit()
            logging.info(f"Successfully updated {update_count} demand records with aircraft request info")

            return update_count

        except Exception as e:
            logging.error(f"Error populating demand aircraft requests: {e}")
            if session:
                session.rollback()
            raise
        finally:
            if session:
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
    
    def process_flight_schedules(self, flight_data: List[Dict], is_initial: bool, start_date: str, end_date: str) -> Dict[str, int]:
        """Complete workflow: transform once, then load to movement_temp, port to movement, load qualifying flights to demand, and process crew assignments

        Args:
            flight_data: List of flight leg dictionaries from API
            is_initial: If True, truncate tables; if False, delete only date range
            start_date: Start date for the load (ISO format)
            end_date: End date for the load (ISO format)
        """
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
            movement_count = self.port_movement_temp_to_movement(is_initial, start_date, end_date)
            results['movement_loaded'] = movement_count

            # Step 3: Load qualifying flights into demand table
            logging.info("Step 3: Loading qualifying flights into demand table")
            demand_count = self.load_qualifying_flights_to_demand(is_initial, start_date, end_date)
            results['demand_loaded'] = demand_count

            # Step 3.5: Populate aircraft request info in demand table
            logging.info("Step 3.5: Populating aircraft request info in demand table")
            aircraft_request_count = self.populate_demand_aircraft_requests()
            results['demand_aircraft_requests_populated'] = aircraft_request_count

            # Step 4: Transfer crew assignments from temp to target table (create shifts)
            logging.info("Step 4: Transferring crew assignments from temp to target table")
            date_range = self.get_flight_date_range(flight_data)
            if date_range[0] and date_range[1]:
                crew_shifts_count = self.crew_assignment_loader.transfer_temp_to_target(date_range)
                results['crew_shifts_loaded'] = crew_shifts_count
            else:
                logging.warning("Could not determine date range for crew assignment transfer")
                results['crew_shifts_loaded'] = 0

            logging.info(f"Flight schedule processing complete: {temp_count} temp, {movement_count} movement, {demand_count} demand ({aircraft_request_count} with aircraft requests), {crew_assignment_count} crew assignment records, {results.get('crew_shifts_loaded', 0)} crew shifts (single transform + parallel loading + shift aggregation)")

            return results

        except Exception as e:
            logging.error(f"Error in flight schedule processing workflow: {e}")
            raise