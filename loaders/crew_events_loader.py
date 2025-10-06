import pandas as pd
from sqlalchemy import text
from database import DatabaseManager
import logging
from typing import Optional, Dict, List
from data_utils import safe_get, clean_string, parse_iso_datetime
from datetime import datetime

class CrewEventsLoader:
    """Handle loading crew events (crew unavailability) into MySQL database"""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.unavailable_event_types = None

    def load_unavailable_event_types(self):
        """Load crew event types from database where isavailable = 0"""
        if self.unavailable_event_types is not None:
            return  # Already loaded

        session = self.db_manager.get_session()
        try:
            query = text("""
                SELECT code
                FROM creweventtype
                WHERE isavailable = 0
            """)
            result = session.execute(query)
            self.unavailable_event_types = {row[0] for row in result.fetchall()}
            logging.info(f"Loaded {len(self.unavailable_event_types)} unavailable event types: {self.unavailable_event_types}")
        except Exception as e:
            logging.error(f"Error loading crew event types: {e}")
            self.unavailable_event_types = set()
        finally:
            session.close()

    def should_mark_unavailable(self, event: Dict) -> bool:
        """Determine if event should mark crew as unavailable"""
        event_type = safe_get(event, 'eventType', '') or ''
        duty_category = safe_get(event, 'dutyEventCategory', '') or ''

        event_type = event_type.strip()
        duty_category = duty_category.strip()

        # Mark unavailable if:
        # 1. eventType == hardDayOff
        if event_type == 'hardDayOff':
            return True

        # 2. dutyEventCategory matches a code in creweventtype where isavailable = 0
        if duty_category and duty_category in self.unavailable_event_types:
            return True

        return False

    def transform_crew_events(self, events_data: List[Dict]) -> pd.DataFrame:
        """Transform raw Avianis crew events to crewunavaildate format"""
        if not events_data:
            return pd.DataFrame()

        transformed_records = []
        filtered_count = 0

        for event in events_data:
            # Only process events that should mark crew unavailable
            if not self.should_mark_unavailable(event):
                event_type = safe_get(event, 'eventType', '')
                duty_category = safe_get(event, 'dutyEventCategory', '')
                logging.debug(f"Filtered out event {safe_get(event, 'id')} - eventType: '{event_type}', dutyEventCategory: '{duty_category}'")
                filtered_count += 1
                continue

            event_id = safe_get(event, 'id')
            personnel_id = safe_get(event, 'personnelID')

            if not event_id:
                logging.warning(f"Skipping event - no event ID")
                continue

            if not personnel_id:
                logging.warning(f"Skipping event {event_id} - no personnelID")
                continue

            start_time_utc = parse_iso_datetime(safe_get(event, 'startDateTimeUTC'))
            end_time_utc = parse_iso_datetime(safe_get(event, 'endDateTimeUTC'))

            if not start_time_utc or not end_time_utc:
                logging.warning(f"Skipping event {event_id} - invalid date/time")
                continue

            last_updated = parse_iso_datetime(safe_get(event, 'lastUpdatedDate'))
            create_time = last_updated if last_updated else datetime.utcnow()

            record = {
                'fmsid': event_id,
                'personnel_fmsid': personnel_id,
                'starttime': start_time_utc,
                'endtime': end_time_utc,
                'crewname': clean_string(safe_get(event, 'personnelName')),
                'category': clean_string(safe_get(event, 'dutyEventCategory')),
                'createtime': create_time,
                'crewid': None
            }

            transformed_records.append(record)

        logging.info(f"Filtered out {filtered_count} events (not hardDayOff or Training)")
        logging.info(f"Kept {len(transformed_records)} unavailability events")

        return pd.DataFrame(transformed_records)

    def lookup_crew_ids(self, df: pd.DataFrame) -> pd.DataFrame:
        """Lookup crew IDs from crew table using personnelID"""
        if df.empty:
            return df

        session = self.db_manager.get_session()
        try:
            # Get unique personnel_fmsids
            unique_personnel_ids = df['personnel_fmsid'].unique().tolist()

            # Build query to lookup crew ids using crew.fmsid
            placeholders = ','.join([f"'{pid}'" for pid in unique_personnel_ids])
            query = text(f"""
                SELECT id as crewid, fmsid
                FROM crew
                WHERE fmsid IN ({placeholders})
            """)

            result = session.execute(query)
            crew_lookup = {row[1]: row[0] for row in result.fetchall()}

            # Map crew ids using personnel_fmsid
            df['crewid'] = df['personnel_fmsid'].map(crew_lookup)

            # Log unmapped crews
            unmapped = df[df['crewid'].isna()]
            if not unmapped.empty:
                logging.warning(f"Could not find crew IDs for {len(unmapped)} events:")
                for idx, row in unmapped.iterrows():
                    logging.warning(f"  - personnel_fmsid: {row['personnel_fmsid']}, name: {row['crewname']}")

            return df

        except Exception as e:
            logging.error(f"Error looking up crew IDs: {e}")
            raise
        finally:
            session.close()

    def load_crew_unavailability(self, events_data: List[Dict], last_activity_date: str = None) -> int:
        """Load crew unavailability data from crew events"""
        try:
            # Load unavailable event types from database
            self.load_unavailable_event_types()

            # Transform events
            logging.info(f"Transforming {len(events_data)} crew events...")
            df = self.transform_crew_events(events_data)

            if df.empty:
                logging.info("No unavailability events to load")
                return 0

            logging.info(f"Found {len(df)} unavailability events")

            logging.info("Looking up crew IDs...")
            df = self.lookup_crew_ids(df)

            df = df[df['crewid'].notna()]

            if df.empty:
                logging.warning("No events could be matched to crew members")
                return 0

            logging.info(f"Matched {len(df)} events to crew members")

            # Drop the personnel_fmsid column before inserting
            df = df.drop(columns=['personnel_fmsid'])

            # Delete existing records from last_activity_date onwards based on starttime
            session = self.db_manager.get_session()
            try:
                if last_activity_date:
                    delete_query = text("""
                        DELETE FROM crewunavaildate
                        WHERE starttime >= :last_activity_date
                    """)
                    result = session.execute(delete_query, {
                        'last_activity_date': parse_iso_datetime(last_activity_date)
                    })
                    deleted_count = result.rowcount
                    logging.info(f"Deleted {deleted_count} existing unavailability records with starttime >= {last_activity_date}")
                session.commit()

                df.to_sql(
                    'crewunavaildate',
                    con=self.db_manager.engine,
                    if_exists='append',
                    index=False,
                    method='multi',
                    chunksize=1000
                )

                logging.info(f"Successfully loaded {len(df)} crew unavailability records")
                return len(df)

            except Exception as e:
                session.rollback()
                logging.error(f"Error loading crew unavailability data: {e}")
                raise
            finally:
                session.close()

        except Exception as e:
            logging.error(f"Failed to load crew unavailability: {e}")
            raise

    def calculate_crew_availability(self, start_time: datetime, end_time: datetime, cutoff_hour: int = 12, is_initial: bool = False) -> int:
        """
        Calculate crew availability based on unavailability data.

        Args:
            start_time: Start date in format 'YYYY-MM-DD'
            end_time: End date in format 'YYYY-MM-DD'
            cutoff_hour: Hour threshold for end date exclusion (default: 12 noon)
            is_initial: If True, truncate the entire table. If False, delete only records in date range.

        Returns:
            Number of availability records inserted
        """
        logging.info(f"Calculating crew availability from {start_time} to {end_time}")

        session = self.db_manager.get_session()
        try:
            # Delete existing availability records
            if is_initial:
                # Truncate entire table for initial load
                truncate_query = text("TRUNCATE TABLE crewavaildate")
                session.execute(truncate_query)
                logging.info(f"Truncated crewavaildate table (initial load)")
            else:
                # Delete only records in the date range for incremental load
                delete_query = text("""
                    DELETE FROM crewavaildate
                    WHERE availdate BETWEEN DATE(:start_time) AND DATE(:end_time)
                """)
                delete_result = session.execute(delete_query, {
                    'start_time': start_time,
                    'end_time': end_time
                })
                deleted_count = delete_result.rowcount
                logging.info(f"Deleted {deleted_count} existing availability records in date range {start_time} to {end_time}")
            session.commit()

            query = text(f"""
                INSERT IGNORE INTO crewavaildate(crewid, availdate, crewname, createtime)
                WITH RECURSIVE numbers AS (
                    SELECT 0 AS day_id
                    UNION ALL
                    SELECT day_id + 1 FROM numbers WHERE day_id < DATEDIFF(:end_time, :start_time)
                )
                SELECT c.crewid, c.availdate, c.crewname, NOW()
                FROM
                (
                    SELECT
                        c.id AS crewid,
                        c.name AS crewname,
                        DATE(DATE_ADD(:start_time, INTERVAL n.day_id DAY)) AS availdate
                    FROM crew c
                    INNER JOIN numbers n
                    WHERE c.isactive = 1
                ) c
                LEFT JOIN crewunavaildate cu
                    ON c.crewid = cu.crewid
                    AND c.availdate BETWEEN DATE(cu.starttime) AND DATE_ADD(DATE(cu.endtime), INTERVAL -1 DAY)
                LEFT JOIN crewunavaildate cu2
                    ON c.crewid = cu2.crewid
                    AND c.availdate = DATE(cu2.endtime)
                    AND HOUR(cu2.endtime) > :cutoff_hour
                WHERE cu.crewid IS NULL
                AND cu2.crewid IS NULL
                ORDER BY 1, 2
            """)

            result = session.execute(query, {
                'start_time': start_time,
                'end_time': end_time,
                'cutoff_hour': cutoff_hour
            })

            inserted_count = result.rowcount
            session.commit()

            logging.info(f"Successfully calculated and inserted {inserted_count} crew availability records")
            return inserted_count

        except Exception as e:
            session.rollback()
            logging.error(f"Error calculating crew availability: {e}")
            raise
        finally:
            session.close()
