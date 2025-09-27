import os
from dotenv import load_dotenv

class Config:
    def __init__(self, operator: str = None):
        # Load operator-specific .env file if operator is provided
        if operator:
            env_file = f'.env.{operator}'
            if os.path.exists(env_file):
                load_dotenv(env_file, override=True)
            else:
                # Fall back to default .env if operator-specific file doesn't exist
                load_dotenv()
        else:
            # Load default .env file
            load_dotenv()
    
    # Avianis API Configuration
    @property
    def AVIANIS_BASE_URL(self):
        return os.getenv('AVIANIS_BASE_URL', 'https://api.avianis.io')
    
    @property
    def AVIANIS_CLIENT_ID(self):
        return os.getenv('AVIANIS_CLIENT_ID')
    
    @property
    def AVIANIS_CLIENT_SECRET(self):
        return os.getenv('AVIANIS_CLIENT_SECRET')
    
    # MySQL Database Configuration
    @property
    def MYSQL_HOST(self):
        return os.getenv('MYSQL_HOST', 'localhost')
    
    @property
    def MYSQL_PORT(self):
        return int(os.getenv('MYSQL_PORT', 3306))
    
    @property
    def MYSQL_USER(self):
        return os.getenv('MYSQL_USER')
    
    @property
    def MYSQL_PASSWORD(self):
        return os.getenv('MYSQL_PASSWORD')
    
    @property
    def MYSQL_DATABASE(self):
        return os.getenv('MYSQL_DATABASE', 'avianis_data')
    
    # ETL Configuration
    @property
    def BATCH_SIZE(self):
        return int(os.getenv('BATCH_SIZE', 1000))
    
    @property
    def LOG_LEVEL(self):
        return os.getenv('LOG_LEVEL', 'INFO')
    
    # Data refresh period configuration
    @property
    def REFRESH_DAYS_PAST(self):
        return int(os.getenv('REFRESH_DAYS_PAST', 3))
    
    @property
    def REFRESH_DAYS_FUTURE(self):
        return int(os.getenv('REFRESH_DAYS_FUTURE', 10))
    
    # Initial load configuration
    @property
    def INITIAL_LOAD_MONTHS_PAST(self):
        return int(os.getenv('INITIAL_LOAD_MONTHS_PAST', 2))
    
    @property
    def INITIAL_LOAD_DAYS_FUTURE(self):
        return int(os.getenv('INITIAL_LOAD_DAYS_FUTURE', 10))
    
    @property
    def mysql_connection_string(self):
        return f"mysql+pymysql://{self.MYSQL_USER}:{self.MYSQL_PASSWORD}@{self.MYSQL_HOST}:{self.MYSQL_PORT}/{self.MYSQL_DATABASE}"
    
    # Datadog Configuration
    @property
    def DATADOG_API_KEY(self):
        return os.getenv('DATADOG_API_KEY')
    
    @property
    def DATADOG_APP_KEY(self):
        return os.getenv('DATADOG_APP_KEY')
    
    @property
    def DATADOG_AGENT_HOST(self):
        return os.getenv('DATADOG_AGENT_HOST', 'localhost')
    
    @property
    def DATADOG_AGENT_PORT(self):
        return int(os.getenv('DATADOG_AGENT_PORT', 8125))
    
    @property
    def DD_ENV(self):
        return os.getenv('DD_ENV', 'development')
    
    # Crew Configuration
    @property
    def SENIOR_CREW_AGE_THRESHOLD(self):
        return int(os.getenv('SENIOR_CREW_AGE_THRESHOLD', 65))