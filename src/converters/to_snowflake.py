import re
import logging

# Convert DDL to Snowflake DDL
class ToSnowflakeConverter:
    @staticmethod
    def to_snowflake(ddl):
        snowflake_ddl = ddl

        logging.debug(f"Before Converting: {ddl}")

        # Mapper for PostgreSQL to Snowflake conversions
        mapper = {
            r'`': '',
            r'CREATE OR REPLACE': 'CREATE OR REPLACE',
            r'CREATE TABLE': 'CREATE TABLE IF NOT EXISTS',
            r'\bSERIAL\b': 'IDENTITY(1,1)',
            r'DEFAULT nextval\(\'"\w+"\'::regclass\)': 'IDENTITY(1,1)',
            r'\bTIMESTAMP(\s+WITH\s+TIME\s+ZONE)?\b': 'TIMESTAMP_NTZ',
            r'\bCHARACTER\s+VARYING\b': 'VARCHAR',
            r'\bTEXT\b': 'TEXT',
            r'\bINTEGER\b': 'INT',
            r'\bFLOAT\b': 'FLOAT',
            r' NUMERIC': ' DECIMAL(10,2)',
            r'::VARCHAR': '',
            r'CHARACTER': 'CHAR',
            r'::BPCHAR': '',
            r'::NUMERIC': '',
            r' CONSTRAINT \w+': '',
            r'PRIMARY\s+KEY\s+\("([^"]+)"\)': r'PRIMARY KEY (\1)',
            r'FOREIGN\s+KEY\s+\("([^"]+)"\)\s+REFERENCES\s+([^"]+)\("([^"]+)"\)': r'FOREIGN KEY (\1) REFERENCES \2(\3)',
            r'ON UPDATE CASCADE ON DELETE CASCADE': '',
            r'CREATE TABLE IF NOT EXISTS': 'CREATE TABLE IF NOT EXISTS',
            r'INT AUTO_INCREMENT': 'INT IDENTITY(1,1)',
            r'DATETIME': 'TIMESTAMP_NTZ',
            r'DOUBLE': 'FLOAT',
            r'DECIMAL\(10,2\)': 'DECIMAL(10,2)',
            r'CHAR': 'CHAR',
            r'PRIMARY KEY \((\w+)\)': r'PRIMARY KEY (\1)',
            r'FOREIGN KEY \((\w+)\) REFERENCES (\w+)\((\w+)\)': r'FOREIGN KEY (\1) REFERENCES \2(\3)',
        }

        # Apply the mapper
        for prev_ddl, snow_map in mapper.items():
            snowflake_ddl = re.sub(prev_ddl, snow_map, snowflake_ddl, flags=re.IGNORECASE)
        
        # Additional fixes
        snowflake_ddl = re.sub(r'\bON UPDATE CASCADE ON DELETE CASCADE\B','',snowflake_ddl,flags=re.IGNORECASE)
        snowflake_ddl = re.sub(r'AUTO_INCREMENT','IDENTITY(1,1)',snowflake_ddl,flags=re.IGNORECASE)
        snowflake_ddl = re.sub(r'CONSTRAINT\s+(\w+)\s+PRIMARY KEY\s+\("(\w+)"\)', r'PRIMARY KEY (\2)', snowflake_ddl, flags=re.IGNORECASE)
        snowflake_ddl = re.sub(r'\);$', ');', snowflake_ddl, flags=re.IGNORECASE)
        snowflake_ddl = re.sub(r'WITH\s*\([^)]*\)', '', snowflake_ddl, flags=re.IGNORECASE)
        snowflake_ddl = re.sub(r'PUBLIC\.', '', snowflake_ddl, flags=re.IGNORECASE)
        snowflake_ddl = re.sub(r'ENGINE\s*=\s*\w+', '', snowflake_ddl, flags=re.IGNORECASE)
        snowflake_ddl = re.sub(r'DEFAULT CHARSET\s*=\s*\w+', '', snowflake_ddl, flags=re.IGNORECASE)
        snowflake_ddl = re.sub(r'COLLATE\s*=\s*\w+', '', snowflake_ddl, flags=re.IGNORECASE)
        snowflake_ddl = re.sub(r'tinyint\(1\)','BOOLEAN',snowflake_ddl)
        snowflake_ddl = re.sub(r'KEY\s+\w+\s+\(\w+\)\,\n','',snowflake_ddl,flags=re.IGNORECASE)
        snowflake_ddl = re.sub(r'\s*ON DELETE CASCADE ON UPDATE CASCADE','',snowflake_ddl, flags=re.IGNORECASE)


        logging.debug(f"After Converting: {snowflake_ddl}")
        return snowflake_ddl
