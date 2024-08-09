import re
from typing import List, Dict
import logging

#from mysql to postgres converter
class ToPostgresConverter:
    
    @staticmethod
    def to_postgres(mysql_ddl: str):
        postgres_ddl = mysql_ddl
        
        
        logging.debug(f"Before Converting: {postgres_ddl}")

        #convert ddl into postgres syntax
        #main conversion mapper from mysql to opstgres
        mapper = {
            r'create or replace' : 'CREATE',
            r'NUMBER\(\d+(?:,\d+)?\) NOT NULL autoincrement start 1 increment 1 noorder' : 'SERIAL',
            'INT': 'INTEGER',
            'DATETIME': 'TIMESTAMP',
            'LONGTEXT': 'TEXT',
            'DOUBLE':'FLOAT',
        } 
        
        
        postgres_ddl = postgres_ddl.replace('`', '"')
        
        for mysqlType, postgresType in mapper.items():
            postgres_ddl = re.sub(r'\b' + mysqlType + r'\b', postgresType, postgres_ddl, flags=re.IGNORECASE)
        
        #removes mysql specific syntax
        postgres_ddl = re.sub(r'(\w+\.\w+\.(\w+)(\([^)]+\)))',r'\2\3', postgres_ddl, flags=re.IGNORECASE)
        postgres_ddl = re.sub(r'VARCHAR\(16777216\)','VARCHAR(255)',postgres_ddl,flags=re.IGNORECASE)
        postgres_ddl = re.sub(r'NUMBER\((\d+),(\d+)\)',r'NUMERIC(\1,\2)',postgres_ddl,flags=re.IGNORECASE)
        postgres_ddl = re.sub(r'NUMERIC\(38,0\)', 'INTEGER', postgres_ddl,flags=re.IGNORECASE)
        postgres_ddl = re.sub(r'TINYINT\s*\(\s*1\s*\)', 'BOOLEAN', postgres_ddl, flags=re.IGNORECASE)
        postgres_ddl = re.sub(r'CREATE TABLE','CREATE TABLE IF NOT EXISTS' ,postgres_ddl, flags=re.IGNORECASE)
        postgres_ddl = re.sub(r'AUTO_INCREMENT', 'SERIAL', postgres_ddl, flags=re.IGNORECASE)
        postgres_ddl = re.sub(r'ENGINE\s*=\s*\w+', '', postgres_ddl, flags=re.IGNORECASE)
        postgres_ddl = re.sub(r'DEFAULT CHARSET\s*=\s*\w+', '', postgres_ddl, flags=re.IGNORECASE)
        postgres_ddl = re.sub(r'COLLATE\s*=\s*\w+', '', postgres_ddl, flags=re.IGNORECASE)
        postgres_ddl = re.sub(r'INTEGER NOT NULL SERIAL', 'SERIAL', postgres_ddl, flags=re.IGNORECASE)
        
        

        # foreign key support    
        postgres_ddl = re.sub(r'KEY\s+"[\w]+"?\s+\("[\w]+"\),\n\s+', '',postgres_ddl,flags=re.IGNORECASE)
        postgres_ddl = re.sub(r'\n\s*CONSTRAINT\s+"[^"]+"\s+', '',postgres_ddl,flags=re.IGNORECASE)
      
        
        
        # logging.debug(f"After Converting: {postgres_ddl}")
        
        
        return postgres_ddl
