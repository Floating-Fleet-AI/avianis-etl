import logging
import pandas as pd
from typing import Dict, List, Set, Optional
from datetime import datetime
from data_utils import safe_get, clean_string, safe_int, safe_float, parse_iso_datetime, parse_flight_datetime, generate_stable_id
from lookup_service import LookupService


class FlightTransformer:
    """Transform flight data to match movement_temp schema"""
    
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.lookup_service = LookupService(db_manager)
        
    
    def extract_shared_flight_data(self, flight: Dict) -> Dict:
        """Extract all shared data from a flight record that's needed by both movement and crew assignment processing"""
        # Basic flight identifiers
        fms_id = safe_get(flight, 'id')
        trip_id = safe_get(flight, 'tripID')
        stable_id = generate_stable_id(fms_id)
        tail_number = safe_get(flight, 'tailNumber')
        
        # Extract all flight times once
        scheduled_departure = parse_iso_datetime(safe_get(flight, 'scheduledDepartureDateUTC'))
        scheduled_arrival = parse_iso_datetime(safe_get(flight, 'scheduledArrivalDateUTC'))
        actual_departure = parse_iso_datetime(safe_get(flight, 'actualDepartureDateUTC'))
        actual_arrival = parse_iso_datetime(safe_get(flight, 'actualArrivalDateUTC'))
        out_blocks = parse_iso_datetime(safe_get(flight, 'outOfBlocksUTC'))
        in_blocks = parse_iso_datetime(safe_get(flight, 'inBlocksUTC'))
        
        # Parse create date once
        create_date_str = safe_get(flight, 'createDate')
        create_time = None
        if create_date_str:
            from data_utils import parse_flight_datetime
            create_time = parse_flight_datetime(create_date_str)
        if not create_time:
            create_time = datetime.utcnow()
        
        # Extract crew information once
        crew_list = safe_get(flight, 'crew', [])
        pic_name = None
        sic_name = None
        crew_members = []
        
        for crew_member in crew_list:
            crew_position = safe_get(crew_member, 'crewPosition', '').lower()
            first_name = safe_get(crew_member, 'firstName', '')
            last_name = safe_get(crew_member, 'lastName', '')
            crew_name = f"{first_name} {last_name}".strip()
            
            if crew_position == 'pic':
                pic_name = crew_name
            elif crew_position == 'sic':
                sic_name = crew_name
            
            crew_members.append({
                'name': crew_name,
                'position': crew_position,
                'position_id': 1 if crew_position == 'pic' else 2 if crew_position == 'sic' else None
            })
        
        return {
            # IDs and basic info
            'fms_id': fms_id,
            'trip_id': trip_id,
            'stable_id': stable_id,
            'tail_number': tail_number,
            
            # Times
            'scheduled_departure': scheduled_departure,
            'scheduled_arrival': scheduled_arrival,
            'actual_departure': actual_departure,
            'actual_arrival': actual_arrival,
            'out_blocks': out_blocks,
            'in_blocks': in_blocks,
            'create_time': create_time,
            
            # Crew information
            'pic_name': pic_name,
            'sic_name': sic_name,
            'crew_members': crew_members,
            'crew_list': crew_list,
            
            # Raw flight data for other fields
            'raw_flight': flight
        }
    
    def calculate_oooi_times(self, shared_data: Dict) -> Dict:
        """
        Calculate OOOI times with 6-minute padding

        Returns dict with:
        - outtime: scheduled_departure
        - offtime: outtime + 6 minutes
        - ontime: intime - 6 minutes
        - intime: scheduled_arrival
        - flight_time: ontime - offtime (in minutes)
        - block_time: intime - outtime (in minutes)
        """
        
        outtime = shared_data['scheduled_departure']
        intime = shared_data['scheduled_arrival']

        # Calculate offtime and ontime with 6-minute padding
        offtime = None
        ontime = None
        if outtime:
            offtime = outtime + pd.Timedelta(minutes=6)
        if intime:
            ontime = intime - pd.Timedelta(minutes=6)

        # Calculate flight_time = ontime - offtime (in minutes)
        flight_time = None
        if ontime and offtime:
            flight_time = (ontime - offtime).total_seconds() / 60

        # Calculate block_time = intime - outtime (in minutes)
        block_time = None
        if intime and outtime:
            block_time = (intime - outtime).total_seconds() / 60

        return {
            'outtime': outtime,
            'offtime': offtime,
            'ontime': ontime,
            'intime': intime,
            'flight_time': flight_time,
            'block_time': block_time
        }

    def build_movement_record(self, shared_data: Dict, lookups: Dict[str, Dict[str, int]]) -> Dict:
        """Build a movement record from shared flight data and lookups"""
        flight = shared_data['raw_flight']
        crew_lookup = lookups.get('crew', {})
        airport_lookup = lookups.get('airports', {})
        aircraft_lookup = lookups.get('aircraft', {})
        
        # Get airport lookups
        departure_icao = safe_get(flight, 'departureICAO')
        arrival_icao = safe_get(flight, 'arrivalICAO')
        from_airport_id = airport_lookup.get(departure_icao.upper()) if departure_icao else None
        to_airport_id = airport_lookup.get(arrival_icao.upper()) if arrival_icao else None
        
        # Get aircraft ID
        aircraft_id = aircraft_lookup.get(shared_data['tail_number']) if shared_data['tail_number'] else None
        
        # Get crew IDs
        pic_id = crew_lookup.get(shared_data['pic_name']) if shared_data['pic_name'] else None
        sic_id = crew_lookup.get(shared_data['sic_name']) if shared_data['sic_name'] else None

        oooi_times = self.calculate_oooi_times(shared_data)

        id = shared_data['stable_id']
        passengerCount = safe_get(flight, 'passengerCount', 0)
        isEmpty = flight.get('isEmpty')
        
        return {
            'id': id,
            'demandid': id if isEmpty is False else None,
            'fromairportid': from_airport_id,
            'toairportid': to_airport_id,
            'fromfboid': safe_int(safe_get(flight, 'departureFBOHandlerID')),
            'tofboid': safe_int(safe_get(flight, 'arrivalFBOHandlerID')),
            'aircraftid': aircraft_id,
            'outtime': oooi_times['outtime'],
            'offtime': oooi_times['offtime'],
            'ontime': oooi_times['ontime'],
            'intime': oooi_times['intime'],
            'actualouttime': shared_data['out_blocks'],
            'actualofftime': shared_data['actual_departure'],
            'actualontime': shared_data['actual_arrival'],
            'actualintime': shared_data['in_blocks'],
            'flighttime': oooi_times['flight_time'],
            'blocktime': oooi_times['block_time'],
            'status': clean_string(safe_get(flight, 'status')),
            'picid': pic_id,
            'sicid': sic_id,
            'fmsversion': None,
            'fmsid': shared_data['fms_id'],
            'createtime': shared_data['create_time'],
            'fromairport': departure_icao,
            'toairport': arrival_icao,
            'tailnumber': clean_string(shared_data['tail_number']),
            'pic': shared_data['pic_name'],
            'sic': shared_data['sic_name'],
            'numberpassenger': passengerCount,
            'tripnumber': clean_string(safe_get(flight, 'tripNumber')),
            'isposition': 1 if isEmpty is True else 0,
            'isowner': 1 if safe_get(flight, 'tripRegulatoryType', '') == 'Part 91' else 0,
            'tripid': shared_data['trip_id'],
            'tripnumber': safe_int(safe_get(flight, 'tripNumber')),
        }
    
    def build_crew_assignment_records(self, shared_data: Dict, aircraft_id: int, crew_lookup: Dict[str, int]) -> List[Dict]:
        """Build crew assignment records from shared flight data"""
        if not shared_data['crew_members'] or not aircraft_id:
            return []
        
        assignments = []
        for crew_member in shared_data['crew_members']:
            if crew_member['position_id'] is None:
                logging.warning(f"Unknown crew position: {crew_member['position']} for crew member {crew_member['name']}")
                continue
            
            # Get crew ID from lookup
            crew_id = crew_lookup.get(crew_member['name'])
            if not crew_id:
                # Skip this assignment if crew not found (will be logged in main loop)
                continue
            
            assignment = {
                'aircraftid': aircraft_id,
                'crewid': crew_id,
                'positionid': crew_member['position_id'],
                'starttime': shared_data['scheduled_departure'],
                'endtime': shared_data['scheduled_arrival'],
                'actualstarttime': shared_data['actual_departure'],
                'actualendtime': shared_data['actual_arrival'],
                'fmsversion': None,
                'fmsid': shared_data['fms_id'],
                'createtime': shared_data['create_time'],
                'tailnumber': shared_data['tail_number'],
                'crewname': crew_member['name'],
                'pic': shared_data['pic_name'],
                'sic': shared_data['sic_name']
            }
            
            assignments.append(assignment)
        
        return assignments
    
    def collect_lookup_sets(self, flight_data: List[Dict]) -> Dict[str, Set[str]]:
        """Collect all unique values needed for bulk lookups"""
        crew_names = set()
        airport_codes = set()
        tail_numbers = set()
        
        for flight in flight_data:
            # Collect crew names using shared data extraction
            shared_data = self.extract_shared_flight_data(flight)
            if shared_data['pic_name']:
                crew_names.add(shared_data['pic_name'])
            if shared_data['sic_name']:
                crew_names.add(shared_data['sic_name'])
            
            # Collect airport ICAO codes
            departure_icao = safe_get(flight, 'departureICAO')
            arrival_icao = safe_get(flight, 'arrivalICAO')
            if departure_icao:
                airport_codes.add(departure_icao.upper())
            if arrival_icao:
                airport_codes.add(arrival_icao.upper())
            
            # Collect tail numbers
            if shared_data['tail_number']:
                tail_numbers.add(shared_data['tail_number'])
        
        return {
            'crew_names': crew_names,
            'airport_codes': airport_codes,
            'tail_numbers': tail_numbers
        }
    
    def transform_flight_data(self, flight_data: List[Dict], lookups: Dict[str, Dict[str, int]]) -> Dict[str, List[Dict]]:
        """Transform flight data to both movement_temp and crew assignment records"""
        if not flight_data:
            return {'movements': [], 'crew_assignments': []}
        
        # Use provided lookups
        crew_lookup = lookups.get('crew', {})
        airport_lookup = lookups.get('airports', {})
        aircraft_lookup = lookups.get('aircraft', {})
        
        movement_records = []
        crew_assignment_records = []
        
        # Track unmatched items for logging
        unmatched_crew = set()
        unmatched_airports = set()
        unmatched_aircraft = set()
        skipped_flights = []  # Track flights skipped due to unmatched aircraft
        
        for flight in flight_data:
            try:
                # Extract all shared data once
                shared_data = self.extract_shared_flight_data(flight)

                # Track unmatched crew
                for crew_member in shared_data['crew_members']:
                    if crew_member['name'] and not crew_lookup.get(crew_member['name']):
                        unmatched_crew.add(crew_member['name'])
                
                # Track unmatched airports
                departure_icao = safe_get(flight, 'departureICAO')
                arrival_icao = safe_get(flight, 'arrivalICAO')
                if departure_icao and not airport_lookup.get(departure_icao.upper()):
                    unmatched_airports.add(departure_icao.upper())
                if arrival_icao and not airport_lookup.get(arrival_icao.upper()):
                    unmatched_airports.add(arrival_icao.upper())
                
                # Check aircraft and skip flight if aircraft not found
                aircraft_id = aircraft_lookup.get(shared_data['tail_number']) if shared_data['tail_number'] else None
                if shared_data['tail_number'] and not aircraft_id:
                    unmatched_aircraft.add(shared_data['tail_number'])
                    skipped_flight = {
                        'fms_id': shared_data['fms_id'],
                        'trip_number': safe_get(flight, 'tripNumber'),
                        'tail_number': shared_data['tail_number'],
                        'route': f"{departure_icao}->{arrival_icao}" if departure_icao and arrival_icao else "Unknown route",
                        'scheduled_departure': safe_get(flight, 'scheduledDepartureDateUTC'),
                        'status': safe_get(flight, 'status')
                    }
                    skipped_flights.append(skipped_flight)
                    continue  # Skip processing this flight
                
                # Build movement record using shared data
                movement_record = self.build_movement_record(shared_data, lookups)
                movement_records.append(movement_record)
                
                # Build crew assignment records using shared data
                crew_assignments = self.build_crew_assignment_records(shared_data, aircraft_id, crew_lookup)
                crew_assignment_records.extend(crew_assignments)
                
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
        
        logging.info(f"Transformed {len(movement_records)} flight records for movement_temp")
        logging.info(f"Generated {len(crew_assignment_records)} crew assignment records")
        if skipped_flights:
            logging.info(f"Skipped {len(skipped_flights)} flights due to unmatched aircraft")
        
        return {
            'movements': movement_records,
            'crew_assignments': crew_assignment_records
        }