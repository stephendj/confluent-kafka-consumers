import json
import psycopg2
import time


class Postgres:
    def __init__(self, database=None, hostname=None, username=None, password=None):
        self.database = database
        self.hostname = hostname
        self.username = username
        self.password = password
        self.conn = self.get_connection()

    def get_connection(self):
        try:
            return psycopg2.connect(dbname=self.database,
                host=self.hostname,
                user=self.username,
                password=self.password
            )
        except psycopg2.OperationalError:
            time.sleep(3)
            return self.get_connection()

    def execute_query(self, query):
        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
            self.conn.commit()
        except Exception as e:
            raise Exception(f'Postgres exception for query \'{query}\'. Full exception: {e}')
        finally:
            cursor.close()
    
    def close_connection(self):
        self.conn.close()