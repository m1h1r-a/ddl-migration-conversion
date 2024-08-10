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