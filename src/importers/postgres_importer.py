import psycopg2
import logging
from typing import List, Dict
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from urllib.parse import quote_plus


# Import into postgres using supplied ddl statements
class PostgresImporter:
    def __init__(self, config: Dict[str, str]):
        self.config = config
        self.engine = self._create_engine()    
        
    #experimental to check if database exists
    def _create_engine(self):
        password = quote_plus(self.config['password'])
        url = f"postgresql://{self.config['user']}:{password}@{self.config['host']}:{self.config['port']}/{self.config['database']}"
        return create_engine(url)
    
    #creates database if not exists  
    def create_database_if_not_exists(self):
        if not database_exists(self.engine.url):
            create_database(self.engine.url)
            logging.info(f"Database '{self.config['database']}' created successfully.")
        else:
            logging.info(f"Database '{self.config['database']}' already exists.")
        

    def import_ddl(self, ddl_statements: List[str]):
                   
        self.create_database_if_not_exists()
            
        try:
            conn = psycopg2.connect(**self.config)
            cur = conn.cursor()
            
            execution_count = 0
            done_executing = 0
            total_statements = len(ddl_statements)
            for ddl in ddl_statements:
                logging.debug(f"No. of DDL Statements Left to Be Executed: {total_statements - done_executing}")
                try:
                    execution_count+=1
                    cur.execute(ddl)
                    conn.commit()
                    done_executing+=1
                    logging.debug(f"Executed : {ddl}")
                    
                    
                except psycopg2.Error as e:
                    
                    if execution_count > (2*total_statements)+ 1:
                        logging.error(f"Error executing DDL of ddl_statements: {e}")
                        logging.error(f"DDL with issue: {ddl}")
                        conn.rollback()
                        raise
                    
                    else:
                        conn.rollback()
                        ddl_statements.append(ddl)
                        logging.warning(f"Execution Failed: {e}")    
                        logging.info(" RETRYING...")                
                        continue
                    
            cur.close()
            conn.close()
            logging.info("Import Succesfull to PostgresImporter")
        
        except psycopg2.Error as e:
            logging.error(f"Error importing in Function PostgresImporter: {e}")
            raise