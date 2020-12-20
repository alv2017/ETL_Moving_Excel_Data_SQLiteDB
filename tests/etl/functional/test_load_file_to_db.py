import os
from settings import DATA_DIR
from etl.load_data_to_db import load_file_to_db


def test_load_file_not_csv_not_xlsx(connect_empty_db, test_logger):
    # GIVEN: valid all valid parameters, data file extension not in (csv, xlsx)
    # WHEN: load_file_to_db is called
    # THEN: -1 is returned and nothing gets loaded
    conn = connect_empty_db
    data_file = os.path.join(DATA_DIR, "testing", "sample_data_file.txt")
    app_logger = test_logger
    read_logger = test_logger
    insert_logger = test_logger
    
    inserted_rows = load_file_to_db(conn, data_file, app_logger, read_logger, insert_logger,
                                    log_errors_to_file=False)
    assert inserted_rows == -1
    
def test_load_file_csv_to_empty_db(connect_empty_db, test_logger):
    # GIVEN: valid all valid parameters, data file is data/testing/sample_data_file.csv
    # WHEN: load_file_to_db is called
    # THEN: 10 is returned
    conn = connect_empty_db
    data_file = os.path.join(DATA_DIR, "testing", "sample_data_file.csv")
    app_logger = test_logger
    read_logger = test_logger
    insert_logger = test_logger
    
    inserted_rows = load_file_to_db(conn, data_file, app_logger, read_logger, insert_logger,
                                    log_errors_to_file=False)
    assert inserted_rows == 10
    
def test_load_file_excel_to_empty_db(connect_empty_db, test_logger):
    # GIVEN: valid all valid parameters, data file is data/testing/sample_data_file.xlsx
    # WHEN: load_file_to_db is called
    # THEN: 10 is returned
    conn = connect_empty_db
    data_file = os.path.join(DATA_DIR, "testing", "sample_data_file.xlsx")
    app_logger = test_logger
    read_logger = test_logger
    insert_logger = test_logger
    
    inserted_rows = load_file_to_db(conn, data_file, app_logger, read_logger, insert_logger,
                                    log_errors_to_file=False)
    assert inserted_rows == 10
    
def test_load_file_csv_twice_to_empty_db(connect_empty_db, test_logger):
    # GIVEN: valid all valid parameters, data file is data/testing/sample_data_file.csv
    # WHEN: load_file_to_db is called twice
    # THEN: 10 is returned on the first call and 0 is returned on the second call
    conn = connect_empty_db
    data_file = os.path.join(DATA_DIR, "testing", "sample_data_file.csv")
    app_logger = test_logger
    read_logger = test_logger
    insert_logger = test_logger
    
    # First call
    inserted_rows = load_file_to_db(conn, data_file, app_logger, read_logger, insert_logger,
                                    log_errors_to_file=False)
    assert inserted_rows == 10
    # Second call
    inserted_rows = load_file_to_db(conn, data_file, app_logger, read_logger, insert_logger,
                                    log_errors_to_file=False)
    assert inserted_rows == 0
    
def test_load_file_excel_twice_to_empty_db(connect_empty_db, test_logger):
    # GIVEN: valid all valid parameters, data file is data/testing/sample_data_file.csv
    # WHEN: load_file_to_db is called twice
    # THEN: 10 is returned on the first call and 0 is returned on the second call
    conn = connect_empty_db
    data_file = os.path.join(DATA_DIR, "testing", "sample_data_file.xlsx")
    app_logger = test_logger
    read_logger = test_logger
    insert_logger = test_logger
    
    # First call
    inserted_rows = load_file_to_db(conn, data_file, app_logger, read_logger, insert_logger,
                                    log_errors_to_file=False)
    assert inserted_rows == 10
    # Second call
    inserted_rows = load_file_to_db(conn, data_file, app_logger, read_logger, insert_logger,
                                    log_errors_to_file=False)
    assert inserted_rows == 0
    