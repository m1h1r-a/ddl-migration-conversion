import mysql.connector
import logging

class MySQLImporter():
    
    def __init__(self, config):
        self.config = config
        
    def import_ddl(self, ddl_statements):
        
        try:
            conn = mysql.connector.connect(**self.config)
            cur = conn.cursor()
            
            for ddl in ddl_statements:
                try:
                    cur.execute(ddl)
                    conn.commit()
                    logging.info(f"Executed : {ddl}")
                    
                except mysql.connector.Error as e:
                    logging.error(f"Error executing DDL of ddl_statements: {e}")
                    logging.error(f"DDL with issue: {ddl}")
                    conn.rollback()
                    raise
            
                cur.close()
                conn.close()
                logging.info("Import to MySQL Succesful!")
                
        except mysql.connector.Error as e:
            logging.error(f"Error importing in Function MySQLImporter: {e}")
            raise