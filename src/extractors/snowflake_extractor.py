import snowflake.connector
from snowflake.connector.errors import ProgrammingError
import logging


class SnowflakeExtractor:
    
    def __init__(self,config):
        
        # account = 'kzxkdmo-lq95508'
        # user = 'root'
        # password = 'Password123'
        # warehouse = 'COMPUTE_WH'
        # database = 'SOURCE'
        # schema = 'PUBLIC'
        
        # self.account = account
        # self.user = user
        # self.password = password
        # self.warehouse = warehouse
        # self.database = database
        # self.schema = schema
        # self.connection = None
        # self.cur = None
        
        self.account = config['account']
        self.user = config['user']
        self.password = config['password']
        self.warehouse = config['warehouse']
        self.database = config['database']
        self.schema = config['schema']
        self.connection = None
        self.cursor = None
        
    def connect(self):
        try:
            self.connection = snowflake.connector.connect(
                account=self.account,
                user=self.user,
                password=self.password,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema
            )
            self.cur = self.connection.cursor()
            print("Connected to Snowflake successfully.")
        except ProgrammingError as e:
            print(f"Error connecting to Snowflake: {e}")
            raise
        
    
    def get_tables(self):
        try:
            self.cur.execute(f"SHOW TABLES IN {self.database}.{self.schema}")
            return [row[1] for row in self.cur.fetchall()]
        except ProgrammingError as e:
            print(f"Error fetching tables: {e}")
            return []
        
    def get_table_ddl(self, table_name):
        try:
            self.cur.execute(f"SELECT GET_DDL('TABLE', '{self.database}.{self.schema}.{table_name}')")
            return self.cur.fetchone()[0]
        except ProgrammingError as e:
            print(f"Error fetching DDL for table {table_name}: {e}")
            return None
                
    def extract_ddl(self):
  
        self.connect()
            
        ddl_list = []
        tables = self.get_tables()
        for table in tables:
            ddl = self.get_table_ddl(table)
            if ddl:
                ddl_list.append(ddl)
        return ddl_list
    
    
        