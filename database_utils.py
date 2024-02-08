import yaml 
from sqlalchemy import create_engine

class DatabaseConnector: 
    def read_db_creds(self, file_path= 'db_creds.yaml'):
        with open(file_path, 'r') as file:
             credentials = yaml.safe_load(file)
        return credentials 
    

    def init_db_engine(self):
        credentials = self.read_db_creds()
        db_uri = f"postgresql://{credentials['RDS_USER']}:{credentials['RDS_PASSWORD']}@{credentials['RDS_HOST']}:{credentials['RDS_PORT']}/{credentials['RDS_DATABASE']}"
        engine = create_engine(db_uri)
        return engine
      
    
    def list_db_tables(self):
        engine = self.init_db_engine()
        with engine.connect() as connection:
            tables = connection.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
            return [table[0] for table in tables]