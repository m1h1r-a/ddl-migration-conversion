# detect_transfer.py

import time
import logging
from typing import List, Dict
from configparser import ConfigParser
import re

from extractors.mysql_extractor import MySQLExtractor
from extractors.postgres_extractor import PostgresExtractor
from extractors.snowflake_extractor import SnowflakeExtractor

from converters.to_mysql import ToMySQLConverter
from converters.to_postgres import ToPostgresConverter
from converters.to_snowflake import ToSnowflakeConverter

from importers.mysql_importer import MySQLImporter
from importers.postgres_importer import PostgresImporter
from importers.snowflake_importer import SnowflakeImporter

class DDLChangeDetector:
    def __init__(self):
        self.previous_ddl = {}

    
    def detect_changes(self, current_ddl: Dict[str, List[str]]) -> Dict[str, Dict[str, List[str]]]:
        changes = {}
        for source, ddl_list in current_ddl.items():
            if source not in self.previous_ddl:
                changes[source] = {'added': ddl_list, 'removed': []}
            else:
                current_set = set(ddl_list)
                previous_set = set(self.previous_ddl[source])
                added = current_set - previous_set
                removed = previous_set - current_set
                if added or removed:
                    changes[source] = {'added': list(added), 'removed': list(removed)}
        
        self.previous_ddl = current_ddl.copy()
        return changes

class Exporter:
    def mysql(self, source_config):
        mysqlExtractor = MySQLExtractor(source_config)
        return mysqlExtractor.extract_ddl()
    
    def postgres(self, source_config):
        postgresExtractor = PostgresExtractor(source_config)
        return postgresExtractor.extract_ddl()
    
    def snowflake(self, source_config):
        snowflakeExtractor = SnowflakeExtractor(source_config)
        return snowflakeExtractor.extract_ddl()

class DDLTransferManager:
    def __init__(self, source_list: List[str], destination_list: List[str], config_file: str):
        self.source_list = source_list
        self.destination_list = destination_list
        self.config = self.load_config(config_file)
        self.exporter = Exporter()
        self.change_detector = DDLChangeDetector()

    #connect config of db clients
    def load_config(self, config_file: str) -> ConfigParser:
        config = ConfigParser()
        try:
            config.read(config_file)
        except Exception as e:
            logging.error(f"Unable To Read from Configuration File: {e}")
            raise
        return config

    def get_current_ddl(self) -> Dict[str, List[str]]:
        current_ddl = {}
        for source in self.source_list:
            if source in ['mysql', 'postgres', 'snowflake']:
                source_config = dict(self.config[source])
                method = getattr(self.exporter, source)
                current_ddl[source] = method(source_config)
        return current_ddl

    
    def process_changes(self, changes: Dict[str, Dict[str, List[str]]]) -> None:
        for destination in self.destination_list:
            destination_config = dict(self.config[destination])
            
            if destination == 'snowflake':
                importer = SnowflakeImporter(destination_config)
                converter = ToSnowflakeConverter()
            elif destination == 'mysql':
                importer = MySQLImporter(destination_config)
                converter = ToMySQLConverter()
            elif destination == 'postgres':
                importer = PostgresImporter(destination_config)
                converter = ToPostgresConverter()
            else:
                raise ValueError(f"Unsupported destination type: {destination}")

            for source, change_dict in changes.items():
                modified_ddl_list = []
                
                # remove tables
                for ddl in change_dict['removed']:
                    table_name = self.extract_table_name(ddl)
                    
                    table_name = re.sub(r'`','',table_name)
                    table_name = re.sub(r'\w+\.(\w+)',r'\1',table_name)
                    drop_ddl = f"DROP TABLE IF EXISTS {table_name}"
                    modified_ddl_list.append(drop_ddl)
                
                # add or modify tables
                for ddl in change_dict['added']:
                    table_name = self.extract_table_name(ddl)
                    # coneriting syntax
                    table_name = re.sub(r'`','',table_name)
                    table_name = re.sub(r'\w+\.(\w+)',r'\1',table_name)
                    
                    drop_ddl = f"DROP TABLE IF EXISTS {table_name}"
                    
                    create_ddl = converter.to_snowflake(ddl) if destination == 'snowflake' else \
                                 converter.to_mysql(ddl) if destination == 'mysql' else \
                                 converter.to_postgres(ddl)
                    modified_ddl_list.extend([drop_ddl, create_ddl])
                
                importer.import_ddl(modified_ddl_list)
            
    def extract_table_name(self, ddl: str) -> str:
        words = ddl.split()
        try:
            create_index = words.index('CREATE')
            table_index = words.index('TABLE', create_index)
            return words[table_index + 1]
        except Exception as e:
            pattern = r'create\s+or\s+replace\s+TABLE\s+(?:[\w\.]+\.)*(\w+)'
            match = re.search(pattern, ddl, re.IGNORECASE)
            if match:
                return match.group(1)
            return None
            
    def run(self, check_interval: int = 5) :
        while True:
            try:
                current_ddl = self.get_current_ddl()
                changes = self.change_detector.detect_changes(current_ddl)
                
                if changes:
                    total_changes = sum(len(ddl_list) for ddl_list in changes.values())
                    logging.info(f"CHANGES DETECTED.")
                    print(f"CHANGES DETECTED.")
                    self.process_changes(changes)
                    
                    logging.info("Migration of changes completed successfully.")
                    print("Migration of changes completed successfully. Check the log file for details.")
                else:
                    logging.debug("No changes detected.")
                    print("No changes detected.")
                
                time.sleep(check_interval)

            except Exception as e:
                logging.error(f"Error in DDL transfer: {e}")
                print(f"Error occurred: {e}")
                print("Check the log file for more details.")
                time.sleep(check_interval)