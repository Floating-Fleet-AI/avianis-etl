import pandas as pd
from sqlalchemy import text
from database import DatabaseManager
import logging
from typing import Optional, Dict, List
from data_utils import safe_get, clean_string

class AirportLoader:
    """Handle loading airport-related data into MySQL database"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
    
    def lookup_airport_id(self, airport_code: str) -> Optional[int]:
        """Helper method to lookup airport ID from airport code"""
        if not airport_code or len(airport_code) < 3:
            return None
        
        try:
            session = self.db_manager.get_session()
            result = session.execute(
                text("SELECT id FROM airport WHERE iatacode = :iata OR icaocode = :icao LIMIT 1"),
                {"iata": airport_code[:3], "icao": airport_code}
            ).fetchone()
            
            if result:
                logging.debug(f"Airport found: {airport_code} -> ID {result[0]}")
                return result[0]
            else:
                logging.warning(f"Airport not found in database: '{airport_code}'")
                return None
                
        except Exception as e:
            logging.error(f"Error looking up airport ID for {airport_code}: {e}")
            return None
        finally:
            session.close()
    
    def get_airport_dictionary(self) -> Dict[str, int]:
        """Get a dictionary mapping airport codes (ICAO/IATA) to their IDs for fast lookup"""
        try:
            session = self.db_manager.get_session()
            
            # Get all airports with their codes
            results = session.execute(
                text("SELECT id, icaocode, iatacode FROM airport WHERE icaocode IS NOT NULL OR iatacode IS NOT NULL")
            ).fetchall()
            
            airport_dict = {}
            for row in results:
                airport_id, icao, iata = row
                
                # Map both ICAO and IATA codes to the same airport ID
                if icao:
                    airport_dict[icao.upper()] = airport_id
                if iata:
                    airport_dict[iata.upper()] = airport_id
            
            logging.info(f"Loaded {len(airport_dict)} airport code mappings into dictionary")
            return airport_dict
            
        except Exception as e:
            logging.error(f"Error creating airport dictionary: {e}")
            return {}
        finally:
            session.close()
    
    def bulk_lookup_airport_ids(self, airport_codes: List[str]) -> Dict[str, int]:
        """Bulk lookup airport IDs using optimized dictionary approach"""
        if not airport_codes:
            return {}
        
        # Get the airport dictionary once
        airport_dict = self.get_airport_dictionary()
        
        # Map airport codes to IDs using dictionary
        airport_mapping = {}
        missing_airports = []
        
        for airport_code in airport_codes:
            if airport_code:
                # Try uppercase lookup
                code_upper = airport_code.upper()
                if code_upper in airport_dict:
                    airport_mapping[airport_code] = airport_dict[code_upper]
                    logging.debug(f"Airport found in dictionary: {airport_code} -> ID {airport_dict[code_upper]}")
                else:
                    missing_airports.append(airport_code)
                    logging.warning(f"Airport not found in dictionary: '{airport_code}'")
        
        # For now, we'll just log missing airports
        # In the future, we could implement API calls to fetch missing airport data
        if missing_airports:
            logging.warning(f"Missing airports that need to be added to database: {missing_airports}")
            # Create a missing airport report
            self.log_missing_airports(missing_airports)
        
        logging.info(f"Successfully mapped {len(airport_mapping)} out of {len(airport_codes)} airport codes to IDs")
        return airport_mapping
    
    def log_missing_airports(self, missing_airports: List[str]):
        """Log missing airports to a file for manual review"""
        try:
            import os
            from datetime import datetime
            
            # Create logs directory if it doesn't exist
            os.makedirs("logs/missing_data", exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"logs/missing_data/missing_airports_{timestamp}.log"
            
            with open(filename, 'w') as f:
                f.write(f"Missing airports detected at {datetime.now()}\n")
                f.write("=" * 50 + "\n")
                for airport in missing_airports:
                    f.write(f"{airport}\n")
            
            logging.info(f"Missing airports logged to {filename}")
            
        except Exception as e:
            logging.error(f"Error logging missing airports: {e}")
    
    def create_missing_airport_stub(self, airport_code: str) -> Optional[int]:
        """Create a stub airport record for missing airports"""
        try:
            session = self.db_manager.get_session()
            
            # Check if airport already exists
            existing = session.execute(
                text("SELECT id FROM airport WHERE icaocode = :icao OR iatacode = :iata LIMIT 1"),
                {"icao": airport_code, "iata": airport_code}
            ).fetchone()
            
            if existing:
                return existing[0]
            
            # Create a new airport ID (using a simple max + 1 approach)
            max_id_result = session.execute(text("SELECT MAX(id) FROM airport")).fetchone()
            new_id = (max_id_result[0] or 0) + 1
            
            # Determine if it's likely ICAO (4 chars) or IATA (3 chars)
            if len(airport_code) == 4:
                icao_code = airport_code.upper()
                iata_code = None
            else:
                icao_code = None
                iata_code = airport_code.upper()
            
            # Insert stub airport record
            session.execute(
                text("""
                    INSERT INTO airport (id, icaocode, iatacode, name, country, city, createtime) 
                    VALUES (:id, :icao, :iata, :name, :country, :city, :createtime)
                """),
                {
                    "id": new_id,
                    "icao": icao_code,
                    "iata": iata_code,
                    "name": f"Unknown Airport ({airport_code})",
                    "country": "Unknown",
                    "city": "Unknown",
                    "createtime": pd.to_datetime("now")
                }
            )
            
            session.commit()
            logging.info(f"Created stub airport record: {airport_code} -> ID {new_id}")
            return new_id
            
        except Exception as e:
            logging.error(f"Error creating stub airport for {airport_code}: {e}")
            session.rollback()
            return None
        finally:
            session.close()