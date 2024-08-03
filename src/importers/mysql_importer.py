import mysql.connector
import logging
from typing import List, Dict

#import into mysql using supplied ddl statements
class MySQLImporter():    
    def __init__(self, config):
        self.config = config
        
        
    def import_ddl(self, ddl_statements: List[str]):      
        try:
            conn = mysql.connector.connect(**self.config)
            cur = conn.cursor()
            
            execution_count = 0
            total_statements = len(ddl_statements)
            for ddl in ddl_statements:
                logging.debug(f"No. of DDL Statements Left to Be Executed: {len(ddl_statements)}")
                try:
                    execution_count+=1
                    # logging.debug(f"Now Executing: {ddl}")
                    cur.execute(ddl)
                    conn.commit()
                    logging.debug(f"Executed : {ddl}")
                    
                except mysql.connector.Error as e:
                    
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
            logging.info("Import to MySQL Succesfull!")
                
        except mysql.connector.Error as e:
            logging.error(f"Error importing in Function MySQLImporter: {e}")
            raise