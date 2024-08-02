import logging
from typing import List, Dict
from extractors.mysql_extractor import MySQLExtractor
from converters.mysql_converter import DDLConverter
from importers.postgres_importer import PostgresImporter
from configparser import ConfigParser
import argparse




# logging time error and messages
#debug->info->warning->error->critical
logging.basicConfig(filename='ddl_migration.log', filemode='w', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt="%d-%m-%Y %H:%M:%S")



class DDLMigrator:
    def __init__(self, source_config: Dict[str, str], destination_config: Dict[str, str]):
        self.exporter = MySQLExtractor(source_config)
        self.converter = DDLConverter()
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
            logging.error(f"DDL migration failed: {e}")
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
        migrator = DDLMigrator(source_config, destination_config)
        migrator.migrate()
        logging.info("Migration completed successfully.")
        print("Migration completed successfully. Check the log file for details.")
    except Exception as e:
        print(f"Migration failed: {e}")
        print("Check the log file for more details.")
        