import snowflake.connector
from snowflake.connector.errors import ProgrammingError
import logging

class SnowflakeImporter:
    
    def __init__(self, config):
        
        self.account = config['account']
        self.user = config['user']
        self.password = config['password']
        self.warehouse = config['warehouse']
        self.database = config['database']
        self.schema = config['schema']
        self.connection = None
        # self.cur = None
        
    def connect(self):
        try:
            self.connection = snowflake.connector.connect(
                account=self.account,
                user=self.user,
                password=self.password,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema
            )
            # self.cur = self.connection.cursor()
            logging.info("Connected to Snowflake successfully.")
        except Exception as e:
            logging.error(f"Error connecting to Snowflake: {e}")
            raise
        
    def create_database_if_not_exists(self):
        
        if not self.connection:
            logging.warning("Not Connected To Snowflake, Connect first")
            return
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
            cursor.execute(f"USE DATABASE {self.database}")
            logging.info(f"Database '{self.database}' created or already exists and is now in use.")
        except Exception as e:
            print(f"Error creating database: {e}")
        finally:
            cursor.close()
            

    def import_ddl(self, ddl_statements):
        
        try:
        
            self.connect()
            self.create_database_if_not_exists()
            cursor = self.connection.cursor()
            execution_count = 0
            done_executing = 0
            total_statements = len(ddl_statements)
            
            for ddl in ddl_statements: 
                if not self.connection:
                    logging.warning("Not Connected To Snowflake, Connect first")
                    return
                
                logging.debug(f"No. of DDL Statements Left to Be Executed: {total_statements - done_executing}")
                
                
                try:
                    execution_count+=1
                    cursor.execute(ddl)
                    done_executing+=1
                    logging.debug(f"Executed DDL Succesfully: {ddl}")
                    
                    
                except ProgrammingError as e:
                    
                    if execution_count > (2*total_statements) + 1:
                        logging.error(f"Error Executing DDL: {e}")
                        logging.error(f"DDL with Issue: {ddl}")
                        self.connection.rollback()
                        raise
                    
                    else:
                        self.connection.rollback()
                        ddl_statements.append(ddl)
                        logging.warning(f"Execution Failed: {e}")
                        logging.info("RETRYING....")
                        continue
                    
            cursor.close()
            self.connection.close()
            logging.info("Import Succesfull to Snowflake!")
        
        except ProgrammingError as e:
            logging.error(f"Error Importing in Function Snowflake Importer: {e}")
            raise    
        
            
        