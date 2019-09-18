import ConnectionPool as cp
from psycopg2 import DatabaseError
class Query:
    title = ['datetime', 'clickid', 'userid', 'usersessionid', 'ishit', 'teamid', 'teamlevel']
    data = None

    def __init__(self, count=None):
        self.cnt = count

    def load_all(self):
        with cp.ConnectionCursor() as cursor:
            if self.cnt is None or self.cnt == '*':
                cursor.execute('select * from game_clicks;')
            else:
                cursor.execute('select * from game_clicks limit %s;', (self.cnt,))

            record = cursor.fetchall()
            for r in record:
                print("The record details are:\n {}".format(r))

    @classmethod
    def bulk_insert(cls, dataframe):
        cls.data=dataframe
        cols=",".join([str(c) for c in cls.title])
        try:
            with cp.ConnectionCursor() as cursor:
                cursor.execute("truncate table game_clicks;")
                for i, row in dataframe.iterrows():
                    sql = "INSERT INTO game_clicks (" + cols + ") VALUES (" + "%s," * (len(row) - 1) + "%s);"
                    cursor.execute(sql, tuple(row))
            print("The data integration has been successfully completed.")
        except(Exception, DatabaseError) as error:
            print('Database error occured while data integration.')