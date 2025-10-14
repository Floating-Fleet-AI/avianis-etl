import pandas as pd
from sqlalchemy import text
from database import DatabaseManager
import logging
from typing import List, Dict

class CrewAssignmentLoader:
    """Handle loading crew assignment data into MySQL database"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
    
    def reset_and_load_crew_assignments_temp(self, crew_assignment_data: List[Dict]) -> int:
        """Reset crewassignment_temp table and load new crew assignment data"""
        try:
            if not crew_assignment_data:
                logging.info("No crew assignment data to load")
                return 0
            
            crew_assignment_df = pd.DataFrame(crew_assignment_data)
            
            if crew_assignment_df.empty:
                logging.info("No valid crew assignment data after transformation")
                return 0
            
            # Reset the table
            session = self.db_manager.get_session()
            session.execute(text("DELETE FROM crewassignment_temp"))
            session.commit()
            session.close()
            
            # Load data (let MySQL auto-increment the id field)
            crew_assignment_df.to_sql(
                'crewassignment_temp',
                con=self.db_manager.engine,
                if_exists='append',
                index=False,
                method='multi'
            )
            
            logging.info(f"Successfully loaded {len(crew_assignment_df)} crew assignment records into crewassignment_temp table")
            return len(crew_assignment_df)
            
        except Exception as e:
            logging.error(f"Error loading crew assignment data: {e}")
            raise
    
    def transfer_temp_to_target(self, query_date_range: tuple) -> int:
        """Transfer crew assignments from temp table to target table with shift aggregation and dutydate calculation"""
        try:
            from sqlalchemy import text
            session = self.db_manager.get_session()
            
            min_date, max_date = query_date_range
            logging.info(f"Transferring crew assignments from temp to target table for date range: {min_date} to {max_date}")
            
            # Delete existing crew assignments in the date range from target table
            # Use dutydate for deletion since that's what we're grouping by
            delete_query = text("""
                DELETE FROM crewassignment 
                WHERE dutydate BETWEEN :min_date AND :max_date
            """)
            
            delete_result = session.execute(delete_query, {
                'min_date': min_date,
                'max_date': max_date
            })
            
            deleted_count = delete_result.rowcount
            logging.info(f"Deleted {deleted_count} existing crew assignments from target table")
            
            # Insert aggregated crew assignments with dutydate calculation
            # Constants for crew timing (matching fl3xx-etl implementation)
            crew_pre_flt_min = 60   # 60 minutes before start
            crew_post_flt_min = 30  # 30 minutes after end
            cutoff_hour = 9         # 9:00 AM cutoff
            
            insert_query = text("""
                INSERT INTO crewassignment (
                    crewid, dutydate, aircraftid, positionid, 
                    starttime, endtime, actualstarttime, actualendtime,
                    fmsversion, fmsid, createtime, tailnumber, crewname
                )
                SELECT 
                    crewid,
                    dutydate,
                    aircraftid,
                    positionid,
                    MIN(DATE_SUB(starttime, INTERVAL :crew_pre_flt_min MINUTE)) as starttime,
                    MAX(DATE_ADD(endtime, INTERVAL :crew_post_flt_min MINUTE)) as endtime,
                    MIN(actualstarttime) as actualstarttime,
                    MAX(actualendtime) as actualendtime,
                    MAX(fmsversion) as fmsversion,
                    GROUP_CONCAT(DISTINCT fmsid ORDER BY fmsid) as fmsid,
                    NOW() as createtime,
                    GROUP_CONCAT(DISTINCT tailnumber ORDER BY tailnumber) as tailnumber,
                    MAX(crewname) as crewname
                FROM (
                    SELECT 
                        crewid,
                        aircraftid,
                        positionid,
                        starttime,
                        endtime,
                        actualstarttime,
                        actualendtime,
                        fmsversion,
                        fmsid,
                        tailnumber,
                        crewname,
                        CASE 
                            WHEN HOUR(starttime) >= :cutoff_hour THEN DATE(starttime)
                            ELSE DATE_SUB(DATE(starttime), INTERVAL 1 DAY)
                        END as dutydate
                    FROM crewassignment_temp
                    WHERE crewid IS NOT NULL
                    AND starttime IS NOT NULL
                    AND endtime IS NOT NULL
                ) temp_with_dutydate
                GROUP BY crewid, aircraftid, dutydate, positionid
                ORDER BY dutydate, crewid, positionid
            """)
            
            insert_result = session.execute(insert_query, {
                'crew_pre_flt_min': crew_pre_flt_min,
                'crew_post_flt_min': crew_post_flt_min,
                'cutoff_hour': cutoff_hour
            })
            
            inserted_count = insert_result.rowcount
            
            session.commit()

            logging.info(f"Transferred {inserted_count} aggregated crew assignments (shifts) from temp to target table")

            # Garbage collect - truncate temp table after successful transfer
            session.execute(text("TRUNCATE TABLE crewassignment_temp"))
            session.commit()
            logging.info("Garbage collected crewassignment_temp table")

            return inserted_count

        except Exception as e:
            session.rollback()
            logging.error(f"Error transferring crew assignments from temp to target: {e}")
            # Clean up temp table even on error
            try:
                session.execute(text("TRUNCATE TABLE crewassignment_temp"))
                session.commit()
                logging.info("Garbage collected crewassignment_temp table after error")
            except:
                pass  # Don't fail on cleanup
            raise
        finally:
            session.close()