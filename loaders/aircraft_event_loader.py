import pandas as pd
from sqlalchemy import text
from database import DatabaseManager
import logging
from typing import Dict, List
from data_utils import safe_get, clean_string, parse_iso_datetime


class AircraftEventLoader:
    """Handle loading aircraft offline events into aircraftevent table using SQL JOINs"""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self._session = None

    def _filter_and_transform_raw(self, event_data: List[Dict]) -> List[Dict]:
        """Transform offline events to raw fields (no lookups)

        Sets type='mx' for maintenanceType=1, empty string for others
        """
        records = []

        for event in event_data:
            try:
                is_maintenance = safe_get(event, 'maintenanceType') == 1
                
                record = {
                    # ID fields (will be populated by JOIN)
                    'aircraftid': None,
                    'airportid': None,
                    # Data fields
                    'type': 'mx' if is_maintenance else '',
                    'description': clean_string(safe_get(event, 'name')),
                    'starttime': parse_iso_datetime(safe_get(event, 'outOfServiceDateTimeUTC')),
                    'endtime': parse_iso_datetime(safe_get(event, 'projectedReturnToServiceDateTimeUTC')),
                    'actualstarttime': None,
                    'actualendtime': parse_iso_datetime(safe_get(event, 'actualReturnToServiceDateTimeUTC')),
                    'fmsversion': None,
                    'fmsid': safe_get(event, 'id'),
                    'createtime': None,  # Will use DB default
                    # Lookup fields
                    'tailnumber': safe_get(event, 'aircraft'),
                    'airport': safe_get(event, 'airport')
                }
                records.append(record)

            except Exception as e:
                logging.error(f"Error transforming event {safe_get(event, 'id', 'unknown')}: {e}")
                continue

        return records

    def _clear_table(self, table_name: str, is_initial: bool, start_date: str = None, end_date: str = None, date_column: str = 'starttime'):
        """Clear table records based on load type"""
        if is_initial:
            truncate_query = text(f"TRUNCATE TABLE {table_name}")
            self._session.execute(truncate_query)
            logging.info(f"Truncated {table_name} table (initial load)")
        else:
            delete_query = text(f"""
                DELETE FROM {table_name}
                WHERE DATE({date_column}) BETWEEN DATE(:start_date) AND DATE(:end_date)
            """)
            delete_result = self._session.execute(delete_query, {
                'start_date': start_date,
                'end_date': end_date
            })
            deleted_count = delete_result.rowcount
            logging.info(f"Deleted {deleted_count} existing records from {table_name} in date range {start_date} to {end_date}")
        self._session.commit()

    def _load_to_temp(self, raw_records: List[Dict]) -> int:
        """Load raw records into aircraftevent_temp table"""
        self._session.execute(text("TRUNCATE TABLE aircraftevent_temp"))
        self._session.commit()

        df = pd.DataFrame(raw_records)
        df.to_sql(
            'aircraftevent_temp',
            con=self.db_manager.engine,
            if_exists='append',
            index=False,
            method='multi'
        )
        loaded_count = len(df)
        logging.info(f"Loaded {loaded_count} records into aircraftevent_temp")
        return loaded_count

    def _log_unmatched_records(self) -> Dict[str, int]:
        """Log unmatched aircraft and airports, return counts for summary"""
        counts = {'unmatched_aircraft': 0, 'unmatched_airports': 0, 'skipped_events': 0}

        # Find unmatched aircraft
        unmatched_aircraft_query = text("""
            SELECT DISTINCT t.tailnumber
            FROM aircraftevent_temp t
            LEFT JOIN aircraft a ON t.tailnumber = a.tailnumber
            WHERE t.tailnumber IS NOT NULL AND a.id IS NULL
        """)
        unmatched_aircraft = self._session.execute(unmatched_aircraft_query).fetchall()

        if unmatched_aircraft:
            counts['unmatched_aircraft'] = len(unmatched_aircraft)
            logging.warning(f"Found {len(unmatched_aircraft)} unmatched aircraft tail numbers:")
            for row in unmatched_aircraft:
                logging.warning(f"  - Aircraft not found: {row[0]}")

        # Find unmatched airports
        unmatched_airports_query = text("""
            SELECT DISTINCT t.airport
            FROM aircraftevent_temp t
            LEFT JOIN airport ap ON UPPER(t.airport) = ap.icaocode
            WHERE t.airport IS NOT NULL AND ap.id IS NULL
        """)
        unmatched_airports = self._session.execute(unmatched_airports_query).fetchall()

        if unmatched_airports:
            counts['unmatched_airports'] = len(unmatched_airports)
            logging.warning(f"Found {len(unmatched_airports)} unmatched airport codes:")
            for row in unmatched_airports:
                logging.warning(f"  - Airport not found: {row[0]}")

        # Find and log events that will be skipped (unmatched aircraft)
        skipped_events_query = text("""
            SELECT t.fmsid, t.description, t.tailnumber, t.airport, t.starttime
            FROM aircraftevent_temp t
            LEFT JOIN aircraft a ON t.tailnumber = a.tailnumber
            WHERE t.tailnumber IS NOT NULL AND a.id IS NULL
            ORDER BY t.starttime
        """)
        skipped_events = self._session.execute(skipped_events_query).fetchall()

        if skipped_events:
            counts['skipped_events'] = len(skipped_events)
            logging.warning(f"SKIPPED {len(skipped_events)} events due to unmatched aircraft:")
            logging.warning("=" * 80)
            for row in skipped_events:
                fms_id, description, tailnumber, airport, starttime = row
                logging.warning(f"Event ID: {fms_id}")
                logging.warning(f"  Description: {description}")
                logging.warning(f"  Aircraft: {tailnumber} (NOT FOUND)")
                logging.warning(f"  Airport: {airport}")
                logging.warning(f"  Start Time: {starttime}")
                logging.warning("-" * 40)

        # Log success if everything matched
        if not unmatched_aircraft and not unmatched_airports:
            logging.info("All aircraft and airports were successfully matched")

        return counts

    def _update_temp_with_joins(self):
        """Update aircraftevent_temp with aircraft and airport IDs via JOINs"""
        update_query = text("""
            UPDATE aircraftevent_temp t
            LEFT JOIN aircraft a ON t.tailnumber = a.tailnumber
            LEFT JOIN airport ap ON UPPER(t.airport) = ap.icaocode
            SET t.aircraftid = a.id,
                t.airportid = ap.id
        """)
        self._session.execute(update_query)
        self._session.commit()
        logging.info("Updated aircraftevent_temp with aircraft and airport IDs via JOIN")

    def _transfer_temp_to_target(self) -> int:
        """Transfer records from temp to target table (skip records with missing aircraft)"""
        insert_query = text("""
            INSERT INTO aircraftevent (
                aircraftid, airportid, type, description, starttime, endtime,
                actualstarttime, actualendtime, fmsversion, fmsid, tailnumber, airport
            )
            SELECT
                aircraftid,
                airportid,
                type,
                description,
                starttime,
                endtime,
                actualstarttime,
                actualendtime,
                fmsversion,
                fmsid,
                tailnumber,
                airport
            FROM aircraftevent_temp
            WHERE aircraftid IS NOT NULL
        """)

        result = self._session.execute(insert_query)
        self._session.commit()
        return result.rowcount

    def _cleanup_temp_table(self):
        """Garbage collect - truncate temp table"""
        self._session.execute(text("TRUNCATE TABLE aircraftevent_temp"))
        self._session.commit()
        logging.info("Garbage collected aircraftevent_temp table")

    def load_aircraft_events(self, event_data: List[Dict], is_initial: bool, start_date: str = None, end_date: str = None) -> Dict[str, int]:
        """Load aircraft offline events using SQL JOINs for better performance

        Workflow:
        1. Transform to raw fields (all offline events, type='mx' for maintenanceType=1)
        2. Load into aircraftevent_temp
        3. Log unmatched records
        4. Clear target table (by date range or truncate)
        5. Update temp with JOINed IDs
        6. Transfer temp to target (skip missing aircraft)
        7. Garbage collect temp table

        Args:
            event_data: List of offline event dictionaries from API
            is_initial: If True, truncate table; if False, delete only date range
            start_date: Start date for the load (YYYY-MM-DD format)
            end_date: End date for the load (YYYY-MM-DD format)

        Returns:
            Dictionary with count of loaded records
        """
        results = {}
        self._session = None

        try:
            if not event_data:
                logging.info("No aircraft event data to load")
                return results

            # Step 1: Transform to raw fields (no lookups)
            logging.info(f"Transforming {len(event_data)} offline events")
            raw_records = self._filter_and_transform_raw(event_data)

            if not raw_records:
                logging.info("No events to load after transformation")
                return results

            logging.info(f"Transformed {len(raw_records)} offline events")

            self._session = self.db_manager.get_session()

            # Step 2: Load raw data into temp table
            self._load_to_temp(raw_records)

            # Step 3: Log unmatched records (before updating IDs)
            unmatched_counts = self._log_unmatched_records()

            # Step 4: Clear target table
            self._clear_table('aircraftevent', is_initial, start_date, end_date, 'starttime')

            # Step 5: Update temp table with JOINed IDs
            self._update_temp_with_joins()

            # Step 6: Transfer from temp to target
            loaded_count = self._transfer_temp_to_target()
            results['events_loaded'] = loaded_count

            # Log final summary
            logging.info(f"Successfully loaded {loaded_count} aircraft event records into aircraftevent table")
            if unmatched_counts['skipped_events'] > 0:
                logging.info(f"Skipped {unmatched_counts['skipped_events']} events due to unmatched aircraft")

            # Step 7: Garbage collect temp table
            self._cleanup_temp_table()

            return results

        except Exception as e:
            logging.error(f"Error loading aircraft events: {e}")
            if self._session:
                self._session.rollback()
                # Clean up temp table even on error
                try:
                    self._cleanup_temp_table()
                    logging.info("Garbage collected aircraftevent_temp table after error")
                except:
                    pass  # Don't fail on cleanup
            raise
        finally:
            if self._session:
                self._session.close()
