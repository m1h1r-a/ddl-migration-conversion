import logging
from typing import List, Dict
from configparser import ConfigParser
import argparse

#Mysql 
from extractors.mysql_extractor import MySQLExtractor
from converters.to_mysql import ToMySQLConverter
from importers.mysql_importer import MySQLImporter

#Postgres
from extractors.postgres_extractor import PostgresExtractor
from converters.to_postgres import ToPostgresConverter
from importers.postgres_importer import PostgresImporter

#snowflake
from extractors.snowflake_extractor import SnowflakeExtractor
from converters.to_snowflake import ToSnowflakeConverter
from importers.snowflake_importer import SnowflakeImporter  


# logging time error and messages
#debug->info->warning->error->critical
logging.basicConfig(filename='ddl_migration.log', filemode='w', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', datefmt="%d-%m-%Y %H:%M:%S")
logging.getLogger('snowflake.connector').setLevel(logging.WARNING)


#tempfiles was here
        
class Exporter:
        
    def mysql(self, source_config):
        mysqlExtractor = MySQLExtractor(source_config)
        ddl = mysqlExtractor.extract_ddl()
        return ddl
    
    def postgres(self, source_config):
        postgresExtractor = PostgresExtractor(source_config)
        ddl = postgresExtractor.extract_ddl()
        return ddl
    
    def snowflake(self, source_config):
        snowflakeExtractor = SnowflakeExtractor(source_config)
        ddl = snowflakeExtractor.extract_ddl()
        return ddl
    

if __name__ == "__main__":
    
    #parser to select source and destination
    parser = argparse.ArgumentParser(description='Enter Source with -s or --source & Destination with -d or --destination for DDL migration-conversion')
    parser.add_argument('-s', '--source', type=str, nargs='+', help="List of source elements")
    parser.add_argument('-d', '--destination', type=str, nargs='+', help="List of destination elements")
    args = parser.parse_args()
    
    source_list = args.source if args.source else []
    destination_list = args.destination if args.destination else []
    
    combined_ddl = []
    
    # parser to take input from configuration file
    config = ConfigParser()
    try:
        config.read("config/db.ini")
    except Exception as e:
        print(f"Unable To Read from Configuration File: {e}")
        logging.error("Unable To read Config File")
        raise
    
    
    # main migrator try
    try:
        
        #source config try
        try:
            
            for source in source_list:
                if source == 'mysql' or source == 'postgres':
                    
                    source_config = {
                        "host":config[source]["host"],
                        "user":config[source]["user"],
                        "password":config[source]["password"],
                        "database":config[source]["database"],
                        "port":config[source]["port"]
                    }
                elif source == 'snowflake':
                    source_config = {
                        "host":config[source]["host"],
                        "user":config[source]["user"],
                        "password":config[source]["password"],
                        "database":config[source]["database"],
                        "port":config[source]["port"],
                        "account":config[source]["account"],
                        "warehouse":config[source]["warehouse"],
                        "schema":config[source]["schema"]
                    }
                else:
                    logging.error("Wrong Inputs Provided")
                    raise
                
                extractor = Exporter()    
                method = getattr(extractor,source)
                combined_ddl.extend(method(source_config))
                
            logging.debug(f"Combined Source DDL List: {len(combined_ddl)} which are: {combined_ddl}")
            
        except Exception as e:
                    
            print(f"Invalid Configuration Selected: {e}")
            logging.error("Invalid Configuration/Configuration not Present in db.ini or Issue with source config")
            raise
            
        try:
            
            for destination in destination_list:
                if destination == 'mysql' or destination == 'postgres':
                    destination_config = {
                        "host":config[destination]["host"],
                        "user":config[destination]["user"],
                        "password":config[destination]["password"],
                        "database":config[destination]["database"],
                        "port":config[destination]["port"],
                    }
                elif destination == 'snowflake':
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
            
                try:
                    if destination == 'snowflake':
                        snowflake_ddl = [ToSnowflakeConverter.to_snowflake(ddl) for ddl in combined_ddl]
                        SnowflakeImporter(destination_config).import_ddl(snowflake_ddl)
                    elif destination == 'mysql':
                        mysql_ddl = [ToMySQLConverter.to_mysql(ddl) for ddl in combined_ddl]
                        MySQLImporter(destination_config).import_ddl(mysql_ddl)
                    elif destination == 'postgres':
                        postgres_ddl = [ToPostgresConverter.to_postgres(ddl) for ddl in combined_ddl]
                        PostgresImporter(destination_config).import_ddl(postgres_ddl)
                    else:
                        raise ValueError("Unsupported destination type")
                except Exception as e:
                    logging.error(f"Error In Importing: {e}")
            
        except Exception as e:
            logging.error("Error In Destination Config: {e}")
            raise
            
            
        logging.info("Migration completed successfully.")
        print("Migration completed successfully. Check the log file for details.")
        
        
    except Exception as e:
        print(f"Migration failed: {e}")
        print("Check the log file for more details.")
        