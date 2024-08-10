import re
import logging

#Convert DDL to mysql ddl
class ToMySQLConverter:
    
    @staticmethod
    def to_mysql(ddl):
        mysql_ddl = ddl
        
        mapper = {
            r'create or replace' : 'CREATE',
            r'CREATE TABLE' : 'CREATE TABLE IF NOT EXISTS',
            r'\bserial\b': 'INT AUTO_INCREMENT',
            r'DEFAULT nextval\(\'"\w+"\'::regclass\)': 'AUTO_INCREMENT',
            r'\btimestamp(\s+with\s+time\s+zone)?\b': 'DATETIME',
            r'\bcharacter\s+varying\b': 'VARCHAR',
            r'\btext\b': 'TEXT',
            r'\binteger\b' : 'INT',
            r'\bfloat\b' : 'DOUBLE',
            r' numeric' : ' DECIMAL(10,2)',
            r'::VARCHAR' : '',
            r'character' : 'CHAR',
            r'::bpchar' : '',
            r'::numeric' : '',
            r' CONSTRAINT \w+' : '',
            r'PRIMARY\s+KEY\s+\("([^"]+)"\)' : r'PRIMARY KEY (\1)',
            r'FOREIGN\s+KEY\s+\("([^"]+)"\)\s+REFERENCES\s+([^"]+)\("([^"]+)"\)' : r'FOREIGN KEY (\1) REFERENCES \2(\3)',
            r'ON UPDATE CASCADE ON DELETE CASCADE' : '',
            r'NUMBER\(38\,0\)':'INT',
            r'autoincrement start 1 increment 1 noorder':'AUTO_INCREMENT',
            r'NUMBER\((\d+),(\d+)\)' : r'DECIMAL(\1,\2)',
            r'VARCHAR\(16777216\)':'TEXT',
            r'DEFAULT nextval\(\'\w+\'::regclass\)':'AUTO_INCREMENT',
        }
        
        for prev_ddl,mysql_map in mapper.items():
            mysql_ddl = re.sub(prev_ddl, mysql_map, mysql_ddl, flags= re.IGNORECASE)
            
        
        #other fixes:
        mysql_ddl = re.sub(r'(\w+\.\w+\.(\w+)(\([^)]+\)))',r'\2\3', mysql_ddl, flags=re.IGNORECASE)
        mysql_ddl = re.sub(r'CONSTRAINT\s+(\w+)\s+PRIMARY KEY\s+\("(\w+)"\)', r'PRIMARY KEY (\2)', mysql_ddl, flags=re.IGNORECASE)
        mysql_ddl = re.sub(r'\);$', ') ENGINE=InnoDB;', mysql_ddl, flags=re.IGNORECASE)
        mysql_ddl = re.sub(r'WITH\s*\([^)]*\)', '', mysql_ddl, flags=re.IGNORECASE)
        mysql_ddl = re.sub(r'public.', '',mysql_ddl, flags=re.IGNORECASE)
        mysql_ddl = re.sub(r'(\w+\.\w+\.(\w+)(\([^)]+\)))',r'\2\3', mysql_ddl, flags=re.IGNORECASE)

        
        
        # logging.debug(f"After Converting: {mysql_ddl}")
        
        return mysql_ddl