import sqlite3
from sqlite3 import Error as SQLiteError
from settings import DB_FILE, TABLE_NAME

from logger.app_logger import logger as applogger
from logger.insert_logger import logger as insertlogger

def create_connection(dbfile=DB_FILE):
    """ The function creates connection with SQLite database.
        :param dbfile: SQLite database file. If dbfile is equal to None, 
        the connection is established with the default appliction database.
        :type dbfile: str
        :return: SQLite DB connection object        
    """
    conn = None
    try:
        conn = sqlite3.connect(dbfile)
    except SQLiteError as err:
        msg = "Failed to establish connection to DB: {0}.\n".format(dbfile)
        msg += str(err)
        applogger.error(msg)
    return conn
    
def insert_hourly_price(conn, data_tuple, ref=None, tblname=TABLE_NAME):
    """ The function inserts hourly price data (price_time, region, price) 
        into application database.
        :params conn: Connection to SQLite database
        :type conn: SQLite DB connection object
        :params data_tuple: price data tuple to be inserted into DB
        :type data_tuple: tuple
        :params ref: log reference
        :type ref: str
        :params tblname: table name
        :type tblname: str
        
        :return: in case of success, -1 in case of failure.
    """
    sql_query = """
        INSERT INTO {0} (price_time, region, price)
        VALUES (?, ?, ?)    
    """.format(tblname)
    
    try:
        cursor = conn.cursor()
        cursor.execute(sql_query, data_tuple)
        return cursor.lastrowid
    except SQLiteError as err:
        if ref is not None:
            msg = ref + " "
        else:
            msg = ""
        msg += "Record insertion failed: {0}. ".format(str(data_tuple))
        msg += "  " + str(err)
        insertlogger.error(msg)
        return -1  
    
