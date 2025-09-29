import logging
from typing import Dict, List, Set
from sqlalchemy import text
from database import DatabaseManager

class LookupService:
    """Centralized service for all entity ID lookups"""
    
    def __init__(self, db_manager: DatabaseManager):
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
    
    def get_bulk_lookups(self, crew_names: Set[str] = None, 
                        aircraft_tail_numbers: Set[str] = None,
                        airport_codes: Set[str] = None) -> Dict[str, Dict[str, int]]:
        """Perform multiple bulk lookups in one call for efficiency"""
        results = {}
        
        if crew_names:
            results['crew'] = self.get_bulk_crew_lookup(crew_names)
        
        if aircraft_tail_numbers:
            results['aircraft'] = self.get_bulk_aircraft_lookup(aircraft_tail_numbers)
        
        if airport_codes:
            results['airports'] = self.get_bulk_airport_lookup(airport_codes)
        
        return results