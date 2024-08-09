import logging
from typing import List, Dict
from configparser import ConfigParser
import argparse

#Mysql to Postgres
from extractors.mysql_extractor import MySQLExtractor
from converters.to_postgres import ToPostgresConverter
from importers.postgres_importer import PostgresImporter

#Postgres to Mysql
from extractors.postgres_extractor import PostgresExtractor
from converters.to_mysql import ToMySQLConverter
from importers.mysql_importer import MySQLImporter

#snowflake
from extractors.snowflake_extractor import SnowflakeExtractor
from converters.to_snowflake import ToSnowflakeConverter
from importers.snowflake_importer import SnowflakeImporter  


# logging time error and messages
#debug->info->warning->error->critical
logging.basicConfig(filename='ddl_migration.log', filemode='w', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', datefmt="%d-%m-%Y %H:%M:%S")
logging.getLogger('snowflake.connector').setLevel(logging.WARNING)


class MySQLToPostgresMigrator:
    def __init__(self, source_config: Dict[str, str], destination_config: Dict[str, str]):
        self.exporter = MySQLExtractor(source_config)
        self.converter = ToPostgresConverter()
        self.importer = PostgresImporter(destination_config)

    def migrate(self):
        try:
            # extractorr
            mysql_ddl = self.exporter.extract_ddl()
            
            # converters
            postgres_ddl = [self.converter.to_postgres(ddl) for ddl in mysql_ddl]
            
            # importers
            self.importer.import_ddl(postgres_ddl)
            
            
        except Exception as e:
            logging.error(f"DDL migration failed in main.py migrate function: {e}")
            raise
        
class PostgresToMySQLMigrator:
    def __init__(self, source_config: Dict[str, str], destination_config: Dict[str, str]):
        self.exporter = PostgresExtractor(source_config)
        self.converter = ToMySQLConverter()
        self.importer = MySQLImporter(destination_config)
        
        
    def migrate(self):
        
        try:
            
            #extractor
            postgres_ddl = self.exporter.extract_ddl()
            
            #converter
            mysql_ddl = [self.converter.to_mysql(ddl) for ddl in postgres_ddl]

            #importer
            self.importer.import_ddl(mysql_ddl)
            
        except Exception as e:
            logging.error(f"DDL migration failed in main.py migrate function: {e}")
            raise
        

class SnowflakeToPostgresMigrator:
    def __init__(self, source_config: Dict[str, str], destination_config: Dict[str, str]):
        self.exporter = SnowflakeExtractor(source_config)
        self.converter = ToPostgresConverter()
        self.importer = PostgresImporter(destination_config)

    def migrate(self):
        try:
            # extractorr
            mysql_ddl = self.exporter.extract_ddl()
            
            # converters
            postgres_ddl = [self.converter.to_postgres(ddl) for ddl in mysql_ddl]
            
            # importers
            self.importer.import_ddl(postgres_ddl)
            
            
        except Exception as e:
            logging.error(f"DDL migration failed in main.py migrate function: {e}")
            raise
        
class MySQLToSnowflakeMigrator:
    def __init__(self, source_config, destination_config):
        self.exporter = MySQLExtractor(source_config)
        self.converter = ToSnowflakeConverter()
        self.importer = SnowflakeImporter(destination_config)
        
    def migrate(self):
        try:
            mysql_ddl = self.exporter.extract_ddl()
            snowflake_ddl = [self.converter.to_snowflake(ddl) for ddl in mysql_ddl]
            self.importer.import_ddl(snowflake_ddl)
        except Exception as e:
            logging.error(f"DDL migration failed in MySQLToSnowflakeMigrator: {e}")
            raise
        
        
class PostgresToSnowflake:
    def __init__(self, source_config, destination_config):
        self.exporter = PostgresExtractor(source_config)
        self.converter = ToSnowflakeConverter()
        self.importer = SnowflakeImporter(destination_config)
        
    def migrate(self):
        try:
            postgres_ddl = self.exporter.extract_ddl()
            snowflake_ddl = [self.converter.to_snowflake(ddl) for ddl in postgres_ddl]
            self.importer.import_ddl(snowflake_ddl)
        except Exception as e:
            logging.error(f"DDL migration failed in PostgresToSnowflake: {e}")
            raise

class SnowflakeToMySQL:
    def __init__(self, source_config, destination_config):
        self.exporter = SnowflakeExtractor(source_config) 
        self.converter = ToMySQLConverter()
        self.importer = MySQLImporter(destination_config)
        
    def migrate(self):
        try:
            snowflake_ddl = self.exporter.extract_ddl()
            mysql_ddl = [self.converter.to_mysql(ddl) for ddl in snowflake_ddl]
            self.importer.import_ddl(mysql_ddl)
        except Exception as e:
            logging.error(f"DDL migration failed in SnowflakeToMySQL: {e}")
            raise
        

if __name__ == "__main__":
    
    
    #parser to select source and destination
    parser = argparse.ArgumentParser(description='Enter Source with -s or --source & Destination with -d or --destination for DDL migration-conversion')
    parser.add_argument('-s', '--source', type=str, nargs='+', help="List of source elements")
    parser.add_argument('-d', '--destination', type=str, nargs='+', help="List of destination elements")
    args = parser.parse_args()
    
    source_list = args.source if args.source else []
    destination_list = args.destination if args.destination else []
    
    #will change later with inclusion of more clients
    source = source_list[0]
    destination = destination_list[0]
    
    # parser to take input from configuration file
    config = ConfigParser()
    try:
        config.read("config/db.ini")
    except Exception as e:
        print(f"Unable To Read from Configuration File: {e}")
        logging.error("Unable To read Config File")
        raise
    
    
    try:
        #default configs to cloud based, if that dosent work got to except and switch to inhouse clients
        source_config = {
            "host":config[source]["host"],
            "user":config[source]["user"],
            "password":config[source]["password"],
            "database":config[source]["database"],
            "port":config[source]["port"],
            # "account":config[source]["account"],
            # "warehouse":config[source]["warehouse"],
            # "schema":config[source]["schema"]
        }
        
        destination_config = {
            "host":config[destination]["host"],
            "user":config[destination]["user"],
            "password":config[destination]["password"],
            "database":config[destination]["database"],
            "port":config[destination]["port"],
            "account":config[destination]["account"],
            "warehouse":config[destination]["warehouse"],
            "schema":config[destination]["schema"]
        }
    
    except Exception as e:
        #put in house clients here with try block
                
        print(f"Invalid Configuration Selected: {e}")
        logging.error("Invalid Configuration/Configuration not Present in db.ini")
        raise
        
    
    try:
        # migrator = MySQLToPostgresMigrator(source_config, destination_config) 
        # migrator = PostgresToMySQLMigrator(source_config, destination_config)
        # migrator = SnowflakeToPostgresMigrator(source_config,destination_config)
        # migrator = MySQLToSnowflakeMigrator(source_config, destination_config)
        migrator = PostgresToSnowflake(source_config, destination_config)
        # migrator = SnowflakeToMySQL(source_config, destination_config)
        
        
        migrator.migrate()
        logging.info("Migration completed successfully.")
        print("Migration completed successfully. Check the log file for details.")
    except Exception as e:
        print(f"Migration failed: {e}")
        print("Check the log file for more details.")
        