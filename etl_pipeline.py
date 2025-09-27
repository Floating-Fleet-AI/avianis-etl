#!/usr/bin/env python3

import logging
import sys
from datetime import datetime, timedelta
from typing import Optional, List, Dict
import argparse
import pandas as pd

from config import Config
from avianis_api import AvianisAPIClient, get_auth_manager
from data_utils import DateRangeManager
from loaders.aircraft_loader import AircraftLoader
from loaders.crew_loader import CrewLoader
from loaders.flight_schedule_loader import FlightScheduleLoader
from database import DatabaseManager

class AvianisETL:
    """Main ETL pipeline for Avianis data"""
    
    def __init__(self, operator: str = None):
        self.operator = operator
        self.config = Config(operator=operator)
        self.auth_manager = get_auth_manager()
        self.api_client = AvianisAPIClient(self.auth_manager)
        self.db_manager = DatabaseManager()
        self.aircraft_loader = AircraftLoader(self.db_manager)
        self.crew_loader = CrewLoader(self.db_manager)
        self.flight_schedule_loader = FlightScheduleLoader(self.db_manager)
        self.date_manager = DateRangeManager(self.config)
        
        # Log operator information
        if self.operator:
            logging.info(f"ETL pipeline initialized for operator: {self.operator}")
        
        # Setup logging with operator-specific log folder
        if self.operator:
            log_dir = f"logs/{self.operator}"
        else:
            log_dir = "logs"
        
        import os
        os.makedirs(log_dir, exist_ok=True)
        
        # Configure logging
        log_level = self.config.LOG_LEVEL
        
        # Force reset logging configuration
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
        
        logging.basicConfig(
            level=getattr(logging, log_level),
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f'{log_dir}/etl_pipeline.log'),
                logging.StreamHandler(sys.stdout)
            ],
            force=True
        )
        
        # Also set the root logger level explicitly
        logging.getLogger().setLevel(getattr(logging, log_level))
    
    def check_table_exists(self, table_name: str) -> bool:
        """Check if a table exists and has data"""
        try:
            session = self.db_manager.get_session()
            result = session.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = result.fetchone()[0]
            session.close()
            return count > 0
        except Exception:
            return False
    
    def is_initial_load(self) -> bool:
        """Determine if this is an initial load based on table data"""
        # Check if key tables have data
        tables_to_check = ['aircraft', 'crew']
        
        for table in tables_to_check:
            if self.check_table_exists(table):
                logging.info(f"Table {table} has data, running incremental load")
                return False
        
        logging.info("Tables appear empty, running initial load")
        return True
    
    def load_aircraft_data(self):
        """Load aircraft and related reference data"""
        logging.info("Starting aircraft data load...")
        
        try:
            # Authenticate first
            if not self.api_client.authenticate():
                raise Exception("Failed to authenticate with Avianis API")
            
            # Load all aircraft-related data from API
            logging.info("Fetching aircraft categories...")
            category_data = self.api_client.get_aircraft_category()
            if not category_data:
                logging.warning("No aircraft category data received from API")
                category_data = []
            
            logging.info("Fetching aircraft models...")
            model_data = self.api_client.get_aircraft_model()
            if not model_data:
                logging.warning("No aircraft model data received from API")
                model_data = []
            
            logging.info("Fetching aircraft...")
            aircraft_data = self.api_client.get_aircraft()
            if not aircraft_data:
                logging.warning("No aircraft data received from API")
                aircraft_data = []
            
            # Load all data using the aircraft loader
            results = self.aircraft_loader.reset_and_load_all_aircraft_data(
                category_data, model_data, aircraft_data
            )
            
            logging.info(f"Aircraft data loading completed: Categories={results.get('categories', 0)}, "
                        f"Types={results.get('types', 0)}, Aircraft={results.get('aircraft', 0)}")
            
        except Exception as e:
            logging.error(f"Error loading aircraft data: {e}")
            raise
    
    def load_crew_data(self):
        """Load crew and personnel data"""
        logging.info("Starting crew data load...")
        
        try:
            # Authenticate first
            if not self.api_client.authenticate():
                raise Exception("Failed to authenticate with Avianis API")
            
            # Determine date range
            is_initial = self.is_initial_load()
            last_activity_date = self.date_manager.get_last_activity_date(is_initial)
            
            # Load duty categories first
            duty_data = self.api_client.get_duty_categories()
            if duty_data:
                self.crew_loader.reset_and_load_duty_categories(duty_data)
                logging.info(f"Loaded {len(duty_data)} duty category records")
            
            # Load personnel data
            personnel_data = self.api_client.get_personnel(last_activity_date)
            if personnel_data:
                self.crew_loader.reset_and_load_crew_data(personnel_data)
                logging.info(f"Loaded {len(personnel_data)} crew records")
            else:
                logging.warning("No personnel data received from API")
            
        except Exception as e:
            logging.error(f"Error loading crew data: {e}")
            raise
    
    def load_flight_data(self):
        """Load flight legs and process them into movement and demand tables"""
        logging.info("Starting flight data load...")
        
        try:
            # Authenticate first
            if not self.api_client.authenticate():
                raise Exception("Failed to authenticate with Avianis API")
            
            # Determine date range
            is_initial = self.is_initial_load()
            if is_initial:
                start_date, end_date = self.date_manager.get_initial_load_dates()
            else:
                start_date, end_date = self.date_manager.get_incremental_load_dates()
            
            # Load flight legs
            flight_data = self.api_client.get_flight_legs(start_date, end_date)
            if flight_data:
                logging.info(f"Retrieved {len(flight_data)} flight leg records")
                
                # Process flight schedules through the complete workflow
                results = self.flight_schedule_loader.process_flight_schedules(flight_data)
                logging.info(f"Flight schedule processing completed: "
                           f"movement_temp={results.get('movement_temp_loaded', 0)}, "
                           f"movement={results.get('movement_loaded', 0)}, "
                           f"demand={results.get('demand_loaded', 0)}")
            else:
                logging.warning("No flight leg data received from API")
            
            # Load aircraft events
            event_data = self.api_client.get_aircraft_events(start_date, end_date)
            if event_data:
                logging.info(f"Retrieved {len(event_data)} aircraft event records")
                # TODO: Implement aircraft event loader
            
        except Exception as e:
            logging.error(f"Error loading flight data: {e}")
            raise
    
    def load_crew_assignments(self):
        """Load crew assignments for aircraft"""
        logging.info("Starting crew assignments load...")
        
        try:
            # Authenticate first
            if not self.api_client.authenticate():
                raise Exception("Failed to authenticate with Avianis API")
            
            # Get aircraft list first
            aircraft_data = self.api_client.get_aircraft()
            if not aircraft_data:
                logging.warning("No aircraft data available for crew assignments")
                return
            
            # Determine date range
            is_initial = self.is_initial_load()
            if is_initial:
                start_date, end_date = self.date_manager.get_initial_load_dates()
            else:
                start_date, end_date = self.date_manager.get_incremental_load_dates()
            
            # Load crew assignments for each aircraft
            total_assignments = 0
            for aircraft in aircraft_data:
                aircraft_id = aircraft.get('id')
                if aircraft_id:
                    assignment_data = self.api_client.get_crew_assignment(
                        aircraft_id, start_date, end_date
                    )
                    if assignment_data:
                        total_assignments += len(assignment_data)
                        # TODO: Implement crew assignment loader
            
            logging.info(f"Retrieved {total_assignments} crew assignment records")
            
        except Exception as e:
            logging.error(f"Error loading crew assignments: {e}")
            raise
    
    def load_personnel_events(self):
        """Load personnel events"""
        logging.info("Starting personnel events load...")
        
        try:
            # Authenticate first
            if not self.api_client.authenticate():
                raise Exception("Failed to authenticate with Avianis API")
            
            # Determine date range
            is_initial = self.is_initial_load()
            last_activity_date = self.date_manager.get_last_activity_date(is_initial)
            
            # Load personnel events
            event_data = self.api_client.get_personnel_events(last_activity_date)
            if event_data:
                logging.info(f"Retrieved {len(event_data)} personnel event records")
                # TODO: Implement personnel event loader
            else:
                logging.warning("No personnel event data received from API")
            
        except Exception as e:
            logging.error(f"Error loading personnel events: {e}")
            raise
    
    def run_setup(self):
        """Run setup operation (aircraft and crew data)"""
        logging.info("Running setup operation...")
        
        try:
            self.load_aircraft_data()
            self.load_crew_data()
            logging.info("Setup operation completed successfully")
            
        except Exception as e:
            logging.error(f"Setup operation failed: {e}")
            raise
    
    def run_full_etl(self):
        """Run complete ETL pipeline"""
        logging.info("Running full ETL pipeline...")
        
        try:
            self.load_aircraft_data()
            self.load_crew_data()
            self.load_flight_data()
            self.load_crew_assignments()
            self.load_personnel_events()
            
            logging.info("Full ETL pipeline completed successfully")
            
        except Exception as e:
            logging.error(f"Full ETL pipeline failed: {e}")
            raise
    
    def close(self):
        """Clean up resources"""
        self.api_client.close()
        self.db_manager.close_connection()

def main():
    parser = argparse.ArgumentParser(description='Avianis ETL Pipeline')
    parser.add_argument('--operator', default='test', help='Operator environment')
    parser.add_argument('--setup', action='store_true', help='Run setup (aircraft and crew data)')
    parser.add_argument('--aircraft-only', action='store_true', help='Load aircraft data only')
    parser.add_argument('--crew-only', action='store_true', help='Load crew data only')
    parser.add_argument('--flight-data-only', action='store_true', help='Load flight data only')
    parser.add_argument('--crew-assignments-only', action='store_true', help='Load crew assignments only')
    parser.add_argument('--personnel-events-only', action='store_true', help='Load personnel events only')
    
    args = parser.parse_args()
    
    etl = AvianisETL(operator=args.operator)
    
    try:
        if args.setup:
            etl.run_setup()
        elif args.aircraft_only:
            etl.load_aircraft_data()
        elif args.crew_only:
            etl.load_crew_data()
        elif args.flight_data_only:
            etl.load_flight_data()
        elif args.crew_assignments_only:
            etl.load_crew_assignments()
        elif args.personnel_events_only:
            etl.load_personnel_events()
        else:
            etl.run_full_etl()
            
    except Exception as e:
        logging.error(f"ETL pipeline failed: {e}")
        sys.exit(1)
    finally:
        etl.close()

if __name__ == "__main__":
    main()