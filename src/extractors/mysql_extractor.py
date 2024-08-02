import mysql.connector
import logging
from typing import List, Dict


class MySQLExtractor:
    def __init__(self, config):
        self.config = config

    def extract_ddl(self) -> List[str]:
        try:
            conn = mysql.connector.connect(**self.config)
            cur = conn.cursor()
            
            cur.execute("SHOW TABLES")
            tables = cur.fetchall()
            
            ddl_statements = []
            for table in tables:
                table_name = table[0]
                cur.execute(f"SHOW CREATE TABLE {table_name}")
                ddl = cur.fetchone()[1]
                ddl_statements.append(ddl)
            
            cur.close()
            conn.close()
            
            logging.info(f"Successfully extracted {len(ddl_statements)} DDL statements from MySQL")
            return ddl_statements
        
        except mysql.connector.Error as e:
            logging.error(f"Error extracting DDL from MySQL: {e}")
            raise