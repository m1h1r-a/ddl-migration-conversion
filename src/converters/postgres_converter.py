import re

class PostgresToMySQL():
    
    def convert_to_mysql(postgres_ddl):
        mysql_ddl = postgres_ddl
        
        mapper = {
            r'\bserial\b': 'INT AUTO_INCREMENT',
            r'\btimestamp(\s+with\s+time\s+zone)?\b': 'DATETIME',
            r'\bcharacter\s+varying\b': 'VARCHAR',
            r'\btext\b': 'LONGTEXT',
            r'\binteger\b' : 'INT',
            r'\bfloat\b' : 'DOUBLE'
            
        }
        
        for postgres_map,mysql_map in mapper.items():
            mysql_ddl = re.sub(postgres_map, mysql_map, mysql_ddl, flags= re.IGNORECASE)
            
        # print(mysql_ddl)
        
        #remove extras frmo postgres
        mysql_ddl = re.sub(r'WITH\s*\([^)]*\)', '', mysql_ddl)
        
        return mysql_ddl
        
        