import sqlite3
from etl.db_operations import create_connection, insert_hourly_price

# Functionality
def test_create_connection(db_file):
    # GIVEN: a valid db file location
    # WHEN: creating a connection to SQLite DB
    # THEN: returned SQLited DB connection
    
    dbfile = db_file
    dbconn = create_connection(dbfile)
    assert isinstance(dbconn, sqlite3.Connection)
    
def test_insert_hourly_price_to_empty_db(connect_empty_db, a_price_tuple):
    # GIVEN: a valid empty db and valid data tuple
    # WHEN calling insert_hourly_price() function
    # THEN: integer rowid = 1 is returned
    
    rowid = insert_hourly_price(connect_empty_db, a_price_tuple)
    
    assert isinstance(rowid, int)
    assert rowid == 1
    
def test_insert_hourly_price_to_nonempty_db(connect_to_db_with_6_entries, a_price_tuple):
    # GIVEN: a valid non empty db with 6 entries and valid data tuple
    # WHEN calling insert_hourly_price() function
    # THEN: integer rowid = 7 is returned
    
    rowid = insert_hourly_price(connect_to_db_with_6_entries, a_price_tuple)
    assert isinstance(rowid, int)
    assert rowid == 7
    
    
    
    
    