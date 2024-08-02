import psycopg2
from psycopg2 import sql
import logging
from typing import List, Dict


class PostgresExtractor():
    
    def __init__(self, config):
        self.config = config
        
        
    def extract_ddl(self):
        try:
            conn = psycopg2.connect(**self.config)
            cur = conn.cursor()
            
            cur.execute ("""SELECT table_name
                            FROM information_schema.tables
                            WHERE table_schema = 'public'
                            AND table_type = 'BASE TABLE';
                        """)
            tables = cur.fetchall()
            
            ddl_statements = []
            for table in tables:
                table_name = table[0]
                