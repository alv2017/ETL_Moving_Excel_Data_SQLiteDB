import pytest
import sqlite3
from sqlite3 import Error as SQLiteError
from settings import DB_FILE_NAME, QUERIES

from logger.testing_logger import logger as testlogger

initial_db_entries = [
        ("2018-01-09 07:00", "LT", "22.18"),
        ("2018-01-09 07:00", "LV", "22.18"),
        ("2018-01-09 07:00", "EE", "22.18"),
        
        ("2018-02-25 18:00", "LT", "22.11"),
        ("2018-02-25 18:00", "LV", "22.11"),
        ("2018-02-25 18:00", "EE", "22.11")    
    ]

valid_db_entries = [
        ("2018-03-19 05:00", "LT", "23.18"),
        ("2018-03-19 05:00", "LV", "23.18"),
        ("2018-03-19 05:00", "EE", "23.18"),
        
        ("2018-07-02 11:00", "LT", "25.11"),
        ("2018-07-02 11:00", "LV", "25.11"),
        ("2018-07-02 11:00", "EE", "25.11")
        
    ]

data_for_row_processing = {
    "region_list": ["EE", "LT", "LV"],

    "sample_rows_from_excel": [
        {
            "data_row": ('15-01-2018', '05\xa0-\xa006', '26.32', '26.32', '26.32'),
            "expected_data": [
                                ("2018-01-15 05:00", "EE", "26.32"), 
                                ("2018-01-15 05:00", "LT", "26.32"), 
                                ("2018-01-15 05:00", "LV", "26.32")
                             ]
        },
        
        {
            "data_row": ('22-02-2019', '10\xa0-\xa011', '25.00', '25.00', '25.00'),
            "expected_data": [
                                ("2019-02-22 10:00", "EE", "25.00"), 
                                ("2019-02-22 10:00", "LT", "25.00"), 
                                ("2019-02-22 10:00", "LV", "25.00")
                             ]
        },
        
        {
            "data_row": ('18-03-2020', '20\xa0-\xa021', '27.01', '27.01', '27.01'),
            "expected_data": [
                                ("2020-03-18 20:00", "EE", "27.01"), 
                                ("2020-03-18 20:00", "LT", "27.01"), 
                                ("2020-03-18 20:00", "LV", "27.01")
                             ]
        },
        
    ],

    "sample_rows_from_csv": [
        {
            "data_row": ['15-01-2018', '05\xa0-\xa006', '26.32', '26.32', '26.32'],
            "expected_data": [
                                ("2018-01-15 05:00", "EE", "26.32"), 
                                ("2018-01-15 05:00", "LT", "26.32"), 
                                ("2018-01-15 05:00", "LV", "26.32")
                             ]
        },
        
        {
            "data_row": ['22-02-2019', '10\xa0-\xa011', '25.00', '25.00', '25.00'],
            "expected_data": [
                                ("2019-02-22 10:00", "EE", "25.00"), 
                                ("2019-02-22 10:00", "LT", "25.00"), 
                                ("2019-02-22 10:00", "LV", "25.00")
                             ]
        },
        
        {
            "data_row": ['18-03-2020', '20\xa0-\xa021', '27.01', '27.01', '27.01'],
            "expected_data": [
                                ("2020-03-18 20:00", "EE", "27.01"), 
                                ("2020-03-18 20:00", "LT", "27.01"), 
                                ("2020-03-18 20:00", "LV", "27.01")
                             ]
        },
        
    ],
    
    "sample_error_rows": [
        {
            "data_row": ('16-01-2018', '06\xa0-\xa007', None, None, None),
            "expected_data": [
                                ("2018-01-16 06:00", "EE", ""), 
                                ("2018-01-16 06:00", "LT", ""), 
                                ("2018-01-16 06:00", "LV", "")
                             ]
        },
        
        {
            "data_row": ['23-03-2019', '10\xa0-\xa011','','' ,'' ],
            "expected_data": [
                                ("2019-03-23 10:00", "EE", ""), 
                                ("2019-03-23 10:00", "LT", ""), 
                                ("2019-03-23 10:00", "LV", "")
                             ]
        },
        
    ],
}

def entry_id(fixture_value):
    return str(fixture_value)

@pytest.fixture()
def test_logger():
    return testlogger

@pytest.fixture()
def db_file(tmpdir_factory):
    db_dir = tmpdir_factory.getbasetemp()
    dbfile = str(db_dir.join(DB_FILE_NAME))
    return dbfile

@pytest.fixture()
def empty_db(db_file):
    create_query = QUERIES["create_prices_table"]
    delete_query = QUERIES["delete_from_prices"]
    dbconn = sqlite3.connect(db_file)
    cursor = dbconn.cursor()
    
    try:
        cursor.execute(create_query)
        cursor.execute(delete_query)
        dbconn.commit()
        return str(db_file)
    except SQLiteError as e:
        raise e
    finally:
        dbconn.close()
        
@pytest.fixture()
def connect_empty_db(empty_db):
    dbconn = sqlite3.connect(empty_db)
    yield dbconn
    if dbconn:
        dbconn.close()
        
@pytest.fixture()
def connect_to_db_with_6_entries(connect_empty_db):
    dbconn = connect_empty_db
    insert_query = QUERIES["insert_price"]
    delete_query = QUERIES["delete_from_prices"]
    cursor = dbconn.cursor()
    for dtuple in initial_db_entries:
        try:
            cursor.execute(insert_query,dtuple)
        except SQLiteError as err:
            raise err
    dbconn.commit()
    
    yield dbconn
    if dbconn:
        try:
            cursor.execute(delete_query)
            dbconn.commit()
        except SQLiteError as err:
            raise err
        dbconn.close()
 
@pytest.fixture(params=valid_db_entries, ids=entry_id)       
def a_price_tuple(request):
    return request.param

@pytest.fixture()
def region_list():
    return data_for_row_processing["region_list"]

@pytest.fixture(params=data_for_row_processing["sample_rows_from_excel"], ids=entry_id)
def sample_row_from_excel(request):
    return request.param

@pytest.fixture(params=data_for_row_processing["sample_rows_from_csv"], ids=entry_id)
def sample_row_from_csv(request):
    return request.param   

@pytest.fixture(params=data_for_row_processing["sample_error_rows"], ids=entry_id)
def sample_error_row(request):
    return request.param   
    
