import sqlite3
from sqlite3 import Error as SQLiteError
from settings import DB_FILE, TABLE_NAME

def create_connection(dbfile=DB_FILE):
    """ The function creates connection with SQLite database.
        :param dbfile: SQLite database file, optional
        :type dbfile: str
        :return: sqlite3.Connection     
    """
    conn = None
    try:
        conn = sqlite3.connect(dbfile)
        return conn
    except SQLiteError as err: 
        raise err
    
def insert_hourly_price(conn, data_tuple, ref=None, tblname=TABLE_NAME):
    """ The function inserts hourly price data (price_time, region, price) 
        into application database.
        :params conn: Connection to SQLite database
        :type conn: sqlite3.Connection
        :params data_tuple: price data tuple to be inserted into DB
        :type data_tuple: tuple
        :params ref: log reference
        :type ref: str
        :params tblname: table name
        :type tblname: str
        
        :return: int
    """
    sql_query = """
        INSERT INTO {0} (price_time, region, price)
        VALUES (?, ?, ?)    
    """.format(tblname)
    
    cursor = conn.cursor()
    try:
        cursor.execute(sql_query, data_tuple)
        return cursor.lastrowid
    except SQLiteError as err:
        raise err
    
