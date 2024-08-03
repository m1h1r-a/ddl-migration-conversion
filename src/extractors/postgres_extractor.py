import psycopg2
from psycopg2 import sql
import logging
from typing import List, Dict


class PostgresExtractor():
    
    def __init__(self, config):
        self.config = config
        
        
    def extract_ddl(self):
        
        ddl_list = []
        try:
            conn = psycopg2.connect(**self.config)
            cur = conn.cursor()
            

            # Get all user tables
            cur.execute("""
                SELECT schemaname, tablename 
                FROM pg_tables 
                WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
            """)
            tables = cur.fetchall()

            for schema, table in tables:
                # Start building the CREATE TABLE statement
                ddl = f"CREATE TABLE {schema}.{table} (\n"

                # Get column information
                cur.execute("""
                    SELECT column_name, data_type, character_maximum_length, is_nullable, column_default
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                """, (schema, table))
                columns = cur.fetchall()

                for column in columns:
                    col_name, data_type, max_length, nullable, default = column
                    ddl += f"    {col_name} {data_type}"
                    if max_length:
                        ddl += f"({max_length})"
                    if nullable == 'NO':
                        ddl += " NOT NULL"
                    if default:
                        ddl += f" DEFAULT {default}"
                    ddl += ",\n"

                # Remove the trailing comma and newline
                ddl = ddl.rstrip(",\n")

                # Get constraints
                cur.execute("""
                    SELECT conname, pg_get_constraintdef(c.oid)
                    FROM pg_constraint c
                    JOIN pg_namespace n ON n.oid = c.connamespace
                    WHERE conrelid = %s::regclass
                """, (f"{schema}.{table}",))
                constraints = cur.fetchall()

                for constraint in constraints:
                    con_name, con_def = constraint
                    ddl += f",\n    CONSTRAINT {con_name} {con_def}"

                ddl += "\n);"

                # Get indexes (excluding those created for constraints)
                cur.execute("""
                    SELECT indexdef
                    FROM pg_indexes
                    WHERE schemaname = %s AND tablename = %s
                    AND indexname NOT IN (
                        SELECT conname 
                        FROM pg_constraint 
                        WHERE conrelid = %s::regclass
                    )
                """, (schema, table, f"{schema}.{table}"))
                indexes = cur.fetchall()

                for index in indexes:
                    ddl += f"\n\n{index[0]};"

                ddl_list.append(ddl)

            cur.close()
            conn.close()

        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL", error)
            
        # logging.debug(f"DDL Statements Post Extraction: {ddl_list}")

        return ddl_list


                