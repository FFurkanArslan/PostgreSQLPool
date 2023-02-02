import psycopg2
from psycopg2 import pool
from config import config

class PostgreDB:
    CONN_POOL = None
    def __init__(self):
        print('constructor called')
        self.ps_connection = self.CONN_POOL.getconn()

    def __del__(self):
        print("Destructor called")
        self.CONN_POOL.putconn(self.ps_connection)

    @classmethod
    def create_pool(cls):
        params = config()
        try:
            cls.CONN_POOL = psycopg2.pool.ThreadedConnectionPool(5, 20, **params)
            if (cls.CONN_POOL):
                print("Connection pool created successfully using ThreadedConnectionPool")
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error while connecting to PostgreSQL", error)

    @classmethod
    def release_pool(cls):
        if cls.CONN_POOL:
            cls.CONN_POOL.closeall
        print("Threaded PostgreSQL connection pool is closed")

    def read_data(self,query):
        try:
            if (self.ps_connection):
                print("successfully recived connection from connection pool ")
                ps_cursor = self.ps_connection.cursor()
                ps_cursor.execute(query)
                result = ps_cursor.fetchall()
                data = [dict(zip([key[0] for key in ps_cursor.description], row)) for row in result]
                ps_cursor.close()
                return data
        except (Exception, psycopg2.DatabaseError) as error:
            return "Error reading data"
        return "No connection"

    def write_data(self,query):
        try:
            if (self.ps_connection):
                print("successfully recived connection from connection pool ")
                ps_cursor = self.ps_connection.cursor()
                ps_cursor.execute(query)
                ps_cursor.close()
                self.ps_connection.commit()
                return True
        except (Exception, psycopg2.DatabaseError) as error:
            return False
        return "No connection"

    def update_data(self,query):
        try:
            if (self.ps_connection):
                print("successfully received connection from connection pool ")
                ps_cursor = self.ps_connection.cursor()
                ps_cursor.execute(query)
                ps_cursor.close()
                self.ps_connection.commit()
                return True
        except (Exception, psycopg2.DatabaseError) as error:
            return False
        return "No connection"