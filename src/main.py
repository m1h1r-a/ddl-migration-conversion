import logging
from typing import List, Dict
from configparser import ConfigParser
import argparse

from extractors.mysql_extractor import MySQLExtractor
from converters.mysql_converter import MySQLtoPostgres
from importers.postgres_importer import PostgresImporter

from extractors.postgres_extractor import PostgresExtractor
from converters.postgres_converter import PostgresToMySQL
from importers.mysql_importer import MySQLImporter



# logging time error and messages
#debug->info->warning->error->critical
logging.basicConfig(filename='ddl_migration.log', filemode='w', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', datefmt="%d-%m-%Y %H:%M:%S")



class MySQLToPostgresMigrator:
    def __init__(self, source_config: Dict[str, str], destination_config: Dict[str, str]):
        self.exporter = MySQLExtractor(source_config)
        self.converter = MySQLtoPostgres()
        self.importer = PostgresImporter(destination_config)

    def migrate(self):
        try:
            # extractorr
            mysql_ddl = self.exporter.extract_ddl()
            
            # converters
            postgres_ddl = [self.converter.mysql_to_postgres(ddl) for ddl in mysql_ddl]
            
            # importers
            self.importer.import_ddl(postgres_ddl)
            
            
        except Exception as e:
            logging.error(f"DDL migration failed in main.py migrate function: {e}")
            raise
        
class PostgresToMySQLMigrator:
    def __init__(self, source_config: Dict[str, str], destination_config: Dict[str, str]):
        self.exporter = PostgresExtractor(source_config)
        self.converter = PostgresToMySQL()
        self.importer = MySQLImporter(destination_config)
        
        
    def migrate(self):
        
        try:
            
            #extractor
            postgres_ddl = self.exporter.extract_ddl()
            
            #converter
            mysql_ddl = [self.converter.convert_to_mysql(ddl) for ddl in postgres_ddl]

            #importer
            self.importer.import_ddl(mysql_ddl)
            
        except Exception as e:
            logging.error(f"DDL migration failed in main.py migrate function: {e}")
            raise


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description='Enter Source & Destination')
    parser.add_argument('source', type=str, help='Enter Source')
    parser.add_argument('destination', type=str, help='Enter Destination')
    args = parser.parse_args()
    
    source = args.source
    destination = args.destination
    
    
    config = ConfigParser()
    try:
        config.read("config/db.ini")
    except Exception as e:
        print(f"Unable To Read from Configuration File: {e}")
        logging.error("Unable To read Config File")
        raise
    
    try:
        source_config = {
            "host":config[source]["host"],
            "user":config[source]["user"],
            "password":config[source]["password"],
            "database":config[source]["database"]
        }
        
        destination_config = {
            "host":config[destination]["host"],
            "user":config[destination]["user"],
            "password":config[destination]["password"],
            "database":config[destination]["database"]
        }
        
    except Exception as e:
        print(f"Invalid Configuration Selected: {e}")
        logging.error("Invalid Configuration/Configuration not Present in db.ini")
        raise
        
    
    try:
        # migrator = MySQLToPostgresMigrator(source_config, destination_config) 
        migrator = PostgresToMySQLMigrator(source_config, destination_config)
        
        migrator.migrate()
        logging.info("Migration completed successfully.")
        print("Migration completed successfully. Check the log file for details.")
    except Exception as e:
        print(f"Migration failed: {e}")
        print("Check the log file for more details.")
        