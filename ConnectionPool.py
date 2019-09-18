import psycopg2 as ps
from psycopg2 import pool

class DbConnection:

    __connectPool=None

    @staticmethod
    def init(**kwargs):
        DbConnection.__connectPool=pool.SimpleConnectionPool(1,20,**kwargs)
        if DbConnection.__connectPool:
            print('Connection is success.')

    @staticmethod
    def getConnection():
        return DbConnection.__connectPool.getconn()

    @staticmethod
    def putConnection(connect):
        DbConnection.__connectPool.putconn(connect)

    @staticmethod
    def closeConnection():
        DbConnection.__connectPool.closeall()

    def __repr__(self):
        print("Connection to PostgreSQL with retrieved login informations.")

class ConnectionCursor:
    def __init__(self):
        self.conn = None
        self.cursor = None

    def __enter__(self):
        self.conn = DbConnection.getConnection()
        self.cursor = self.conn.cursor()
        return self.cursor

    def __exit__(self, exception_type, exception_value, exception_traceback):
        if exception_value:  # This is equivalent to `if exception_value is not None`
            self.conn.rollback()
        else:
            self.cursor.close()
            self.conn.commit()
        DbConnection.putConnection(self.conn)