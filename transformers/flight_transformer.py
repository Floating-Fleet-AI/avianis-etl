import logging
import pandas as pd
from typing import Dict, List, Set, Optional
from datetime import datetime
from sqlalchemy import text
from data_utils import safe_get, clean_string, safe_int, safe_float, parse_iso_datetime, parse_flight_datetime, generate_stable_id


class FlightTransformer:
    """Transform flight data to match movement_temp schema"""
    
    def __init__(self, db_manager):
        self.db_manager = db_manager
        
    def get_bulk_crew_lookup(self, crew_names: Set[str]) -> Dict[str, int]:
        """Get crew ID lookups for a set of crew names"""
        if not crew_names:
            return {}
            
        try:
            session = self.db_manager.get_session()
            
            # Build query to find crew by full name
            placeholders = ','.join([':name' + str(i) for i in range(len(crew_names))])
            query = text(f"""
                SELECT id, CONCAT(firstname, ' ', lastname) as full_name 
                FROM crew 
                WHERE CONCAT(firstname, ' ', lastname) IN ({placeholders})
            """)
            
            # Create parameter dict
            params = {f'name{i}': name for i, name in enumerate(crew_names)}
            results = session.execute(query, params).fetchall()
            
            crew_lookup = {}
            for row in results:
                crew_id, full_name = row
                crew_lookup[full_name] = crew_id
            
            logging.info(f"Found {len(crew_lookup)} crew members from {len(crew_names)} requested")
            return crew_lookup
            
        except Exception as e:
            logging.error(f"Error in bulk crew lookup: {e}")
            return {}
        finally:
            session.close()
    
    def get_bulk_airport_lookup(self, airport_codes: Set[str]) -> Dict[str, int]:
        """Get airport ID lookups for a set of ICAO codes"""
        if not airport_codes:
            return {}
            
        try:
            session = self.db_manager.get_session()
            
            # Build query to find airports by ICAO code
            placeholders = ','.join([':icao' + str(i) for i in range(len(airport_codes))])
            query = text(f"""
                SELECT id, icaocode 
                FROM airport 
                WHERE icaocode IN ({placeholders})
            """)
            
            # Create parameter dict
            params = {f'icao{i}': code for i, code in enumerate(airport_codes)}
            results = session.execute(query, params).fetchall()
            
            airport_lookup = {}
            for row in results:
                airport_id, icao = row
                airport_lookup[icao] = airport_id
            
            logging.info(f"Found {len(airport_lookup)} airports from {len(airport_codes)} requested")
            return airport_lookup
            
        except Exception as e:
            logging.error(f"Error in bulk airport lookup: {e}")
            return {}
        finally:
            session.close()
    
    def get_bulk_aircraft_lookup(self, tail_numbers: Set[str]) -> Dict[str, int]:
        """Get aircraft ID lookups for a set of tail numbers"""
        if not tail_numbers:
            return {}
            
        try:
            session = self.db_manager.get_session()
            
            # Build query to find aircraft by tail number
            placeholders = ','.join([':tail' + str(i) for i in range(len(tail_numbers))])
            query = text(f"""
                SELECT id, tailnumber 
                FROM aircraft 
                WHERE tailnumber IN ({placeholders})
            """)
            
            # Create parameter dict
            params = {f'tail{i}': tail for i, tail in enumerate(tail_numbers)}
            results = session.execute(query, params).fetchall()
            
            aircraft_lookup = {}
            for row in results:
                aircraft_id, tailnumber = row
                aircraft_lookup[tailnumber] = aircraft_id
            
            logging.info(f"Found {len(aircraft_lookup)} aircraft from {len(tail_numbers)} requested")
            return aircraft_lookup
            
        except Exception as e:
            logging.error(f"Error in bulk aircraft lookup: {e}")
            return {}
        finally:
            session.close()
    
    def extract_crew_info(self, flight: Dict) -> Dict:
        """Extract PIC and SIC information from flight crew"""
        crew_list = safe_get(flight, 'crew', [])
        pic_name = None
        sic_name = None
        
        for crew_member in crew_list:
            crew_position = safe_get(crew_member, 'crewPosition', '').lower()
            first_name = safe_get(crew_member, 'firstName', '')
            last_name = safe_get(crew_member, 'lastName', '')
            crew_name = f"{first_name} {last_name}".strip()
            
            if crew_position == 'pic':
                pic_name = crew_name
            elif crew_position == 'sic':
                sic_name = crew_name
        
        return {
            'pic_name': pic_name,
            'sic_name': sic_name
        }
    
    def collect_lookup_sets(self, flight_data: List[Dict]) -> Dict[str, Set[str]]:
        """Collect all unique values needed for bulk lookups"""
        crew_names = set()
        airport_codes = set()
        tail_numbers = set()
        
        for flight in flight_data:
            # Collect crew names
            crew_info = self.extract_crew_info(flight)
            if crew_info['pic_name']:
                crew_names.add(crew_info['pic_name'])
            if crew_info['sic_name']:
                crew_names.add(crew_info['sic_name'])
            
            # Collect airport ICAO codes
            departure_icao = safe_get(flight, 'departureICAO')
            arrival_icao = safe_get(flight, 'arrivalICAO')
            if departure_icao:
                airport_codes.add(departure_icao.upper())
            if arrival_icao:
                airport_codes.add(arrival_icao.upper())
            
            # Collect tail numbers
            tail_number = safe_get(flight, 'tailNumber')
            if tail_number:
                tail_numbers.add(tail_number)
        
        return {
            'crew_names': crew_names,
            'airport_codes': airport_codes,
            'tail_numbers': tail_numbers
        }
    
    def transform_flight_to_movement_temp(self, flight_data: List[Dict]) -> List[Dict]:
        """Transform flight data to movement_temp table format"""
        if not flight_data:
            return []
        
        # Collect all lookup values and perform bulk lookups
        lookup_sets = self.collect_lookup_sets(flight_data)
        crew_lookup = self.get_bulk_crew_lookup(lookup_sets['crew_names'])
        airport_lookup = self.get_bulk_airport_lookup(lookup_sets['airport_codes'])
        aircraft_lookup = self.get_bulk_aircraft_lookup(lookup_sets['tail_numbers'])
        
        transformed_records = []
        
        # Track unmatched items for logging
        unmatched_crew = set()
        unmatched_airports = set()
        unmatched_aircraft = set()
        skipped_flights = []  # Track flights skipped due to unmatched aircraft
        
        for flight in flight_data:
            try:
                # Extract crew information
                crew_info = self.extract_crew_info(flight)
                pic_name = crew_info['pic_name']
                sic_name = crew_info['sic_name']
                
                # Lookup crew IDs and track unmatched
                pic_id = crew_lookup.get(pic_name) if pic_name else None
                sic_id = crew_lookup.get(sic_name) if sic_name else None
                
                if pic_name and not pic_id:
                    unmatched_crew.add(pic_name)
                if sic_name and not sic_id:
                    unmatched_crew.add(sic_name)
                
                # Extract airport information - only use ICAO codes
                departure_icao = safe_get(flight, 'departureICAO')
                arrival_icao = safe_get(flight, 'arrivalICAO')
                
                from_airport_id = airport_lookup.get(departure_icao.upper()) if departure_icao else None
                to_airport_id = airport_lookup.get(arrival_icao.upper()) if arrival_icao else None
                
                # Track unmatched airports
                if departure_icao and not from_airport_id:
                    unmatched_airports.add(departure_icao.upper())
                if arrival_icao and not to_airport_id:
                    unmatched_airports.add(arrival_icao.upper())
                
                # Extract aircraft information using tail number
                tail_number = safe_get(flight, 'tailNumber')
                aircraft_id = aircraft_lookup.get(tail_number) if tail_number else None
                
                # Track unmatched aircraft and skip flight if aircraft not found
                if tail_number and not aircraft_id:
                    unmatched_aircraft.add(tail_number)
                    # Skip this flight and log details
                    skipped_flight = {
                        'fms_id': safe_get(flight, 'id'),
                        'trip_number': safe_get(flight, 'tripNumber'),
                        'tail_number': tail_number,
                        'route': f"{departure_icao}->{arrival_icao}" if departure_icao and arrival_icao else "Unknown route",
                        'scheduled_departure': safe_get(flight, 'scheduledDepartureDateUTC'),
                        'status': safe_get(flight, 'status')
                    }
                    skipped_flights.append(skipped_flight)
                    continue  # Skip processing this flight
                
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
                
                # Generate stable IDs
                fms_id = safe_get(flight, 'id')
                trip_id = safe_get(flight, 'tripID')
                stable_id = generate_stable_id(fms_id)
                demand_id = generate_stable_id(trip_id) if trip_id else stable_id
                
                # Parse create date from flight data
                create_date_str = safe_get(flight, 'createDate')
                create_time = parse_flight_datetime(create_date_str) if create_date_str else datetime.utcnow()
                
                # Transform fields according to schema mapping
                record = {
                    'id': stable_id,
                    'demandid': demand_id,
                    'fromairportid': from_airport_id,
                    'toairportid': to_airport_id,
                    'fromfboid': safe_int(safe_get(flight, 'departureFBOHandlerID')),
                    'tofboid': safe_int(safe_get(flight, 'arrivalFBOHandlerID')),
                    'aircraftid': aircraft_id,
                    'outtime': scheduled_departure,
                    'offtime': offtime,
                    'ontime': ontime,
                    'intime': scheduled_arrival,
                    'actualouttime': out_blocks,
                    'actualofftime': actual_departure,
                    'actualontime': actual_arrival,
                    'actualintime': in_blocks,
                    'flighttime': safe_float(safe_get(flight, 'actualFlightTime')),
                    'blocktime': safe_float(safe_get(flight, 'actualBlockTime')),
                    'status': clean_string(safe_get(flight, 'status')),
                    'picid': pic_id,
                    'sicid': sic_id,
                    'fmsversion': None,  # Not available in source data
                    'fmsid': fms_id,
                    'createtime': create_time,
                    'fromairport': departure_icao,
                    'toairport': arrival_icao,
                    'tailnumber': clean_string(tail_number),
                    'pic': pic_name,
                    'sic': sic_name,
                    'numberpassenger': safe_int(safe_get(flight, 'passengerCount')),
                    'tripnumber': clean_string(safe_get(flight, 'tripNumber')),
                    'isposition': 1 if safe_get(flight, 'isEmpty', False) else 0,
                    'isowner': 1 if safe_get(flight, 'tripRegulatoryType', '') == 'Part 91' else 0,
                    'tripid': trip_id,
                    'tripnumber': safe_int(safe_get(flight, 'tripNumber')),
                }
                
                transformed_records.append(record)
                
            except Exception as e:
                logging.error(f"Error transforming flight record {safe_get(flight, 'id', 'unknown')}: {e}")
                continue
        
        # Log unmatched items
        if unmatched_crew:
            logging.warning(f"Found {len(unmatched_crew)} unmatched crew members:")
            for crew_name in sorted(unmatched_crew):
                logging.warning(f"  - Crew not found: {crew_name}")
        
        if unmatched_airports:
            logging.warning(f"Found {len(unmatched_airports)} unmatched airports:")
            for airport_code in sorted(unmatched_airports):
                logging.warning(f"  - Airport not found: {airport_code}")
        
        if unmatched_aircraft:
            logging.warning(f"Found {len(unmatched_aircraft)} unmatched aircraft:")
            for tail_number in sorted(unmatched_aircraft):
                logging.warning(f"  - Aircraft not found: {tail_number}")
        
        # Log detailed information about skipped flights
        if skipped_flights:
            logging.warning(f"SKIPPED {len(skipped_flights)} flights due to unmatched aircraft:")
            logging.warning("=" * 80)
            for flight in skipped_flights:
                logging.warning(f"Flight ID: {flight['fms_id']}")
                logging.warning(f"  Trip: {flight['trip_number']}")
                logging.warning(f"  Aircraft: {flight['tail_number']} (NOT FOUND)")
                logging.warning(f"  Route: {flight['route']}")
                logging.warning(f"  Departure: {flight['scheduled_departure']}")
                logging.warning(f"  Status: {flight['status']}")
                logging.warning("-" * 40)
            
            # Summary by aircraft
            aircraft_count = {}
            for flight in skipped_flights:
                tail = flight['tail_number']
                aircraft_count[tail] = aircraft_count.get(tail, 0) + 1
            
            logging.warning("SUMMARY - Flights skipped by aircraft:")
            for tail, count in sorted(aircraft_count.items()):
                logging.warning(f"  {tail}: {count} flights skipped")
        
        if not unmatched_crew and not unmatched_airports and not unmatched_aircraft:
            logging.info("All crew, airports, and aircraft were successfully matched")
        
        logging.info(f"Transformed {len(transformed_records)} flight records for movement_temp")
        if skipped_flights:
            logging.info(f"Skipped {len(skipped_flights)} flights due to unmatched aircraft")
        
        return transformed_records