import pytest
import os
import inspect
from logger.testing_logger import logger as testlogger
from etl.file_operations import log_missing_price_error, log_missing_data_summary
from etl.file_operations import process_data_row

# Test Data

data_tuples_with_missing_price = [
        ("2018-07-14 15:00", "LT", ""),
        ("2018-07-14 15:00", "LV", ""),
        ("2018-07-14 15:00", "EE", ""),
        ("2018-12-02 10:00", "LT", ""),
        ("2018-12-02 10:00", "LV", ""),
        ("2018-12-02 10:00", "EE", "")
        
    ]

# Function in test: log_missing_price_error()

@pytest.mark.parametrize('price_time,region,price', data_tuples_with_missing_price)
def test_log_missing_price_error(price_time, region, price, caplog):
    # GIVEN: valid input parameters
    # WHEN: log_missing_price_error function is called
    # THEN: the message gets logged
    data_file_name = "sample_data_file.csv"
    data_tuple = (price_time, region, price)
    log_missing_price_error(testlogger, data_tuple, data_file_name)
    expected_log_messages = ["{0}: The price is missing or not valid: {1}.".format(data_file_name, 
                                                                            str(data_tuple))]
    actual_log_messages = caplog.messages
    assert expected_log_messages == actual_log_messages
    
# Function in test: log_missing_data_summary()

def test_log_missing_data_summary_no_missing_entries_with_error_logging(caplog):
    # GIVEN: valid input parameters and missing_data_entries=0
    # WHEN: log_missing_data_summary function is called
    # THEN: no messages logged
    data_file = "location/of/sample_data_file.csv"
    missing_data_entries = 0
    log_errors_to_file = True
    log_missing_data_summary(testlogger, data_file, missing_data_entries, log_errors_to_file )
    expected_log_messages = []
    actual_log_messages = caplog.messages
    
    assert actual_log_messages == expected_log_messages
    
def test_log_missing_data_summary_no_missing_entries_without_error_logging(caplog):
    # GIVEN: valid input parameters and missing_data_entries=0
    # WHEN: log_missing_data_summary function is called
    # THEN: no messages logged
    data_file = "location/of/sample_data_file.csv"
    missing_data_entries = 0
    log_errors_to_file = False
    log_missing_data_summary(testlogger, data_file, missing_data_entries, log_errors_to_file )
    expected_log_messages = []
    actual_log_msg = caplog.messages
    
    assert actual_log_msg == expected_log_messages

@pytest.mark.parametrize('missing_data_entries', [1, 5, 10, 100])
def test_log_missing_data_summary_with_missing_entries_without_error_logging(missing_data_entries, caplog):
    # GIVEN: valid input parameters and missing_data_entries > 0
    # WHEN: log_missing_data_summary function is called
    # THEN: specified error messages gets logged
    data_file = "location/of/sample_data_file.csv"
    data_file_basename = os.path.basename(data_file)
    log_errors_to_file = False
    log_missing_data_summary(testlogger, data_file, missing_data_entries, log_errors_to_file )
    
    msg = "There is missing price data in the data file: {}\n!!! ".format(data_file_basename) 
    msg += "WARNING: Number of missing values: {} !!!\n\n".format(missing_data_entries)
    expected_messages = [msg]
    
    actual_log_messages = caplog.messages
    
    assert expected_messages == actual_log_messages
           

@pytest.mark.parametrize('data_file', [
        "location/of/sample_data_file.csv",
        "location/of/sample_data_file.xlsx"
    ])
def test_log_missing_data_summary_5_missing_entries_with_error_logging(data_file, caplog):
    # GIVEN: valid input parameters and missing_data_entries=5
    # WHEN: log_missing_data_summary function is called
    # THEN: specified error messages gets logged
    
    data_file_basename = os.path.basename(data_file)
    data_file_name, data_file_ext = data_file_basename.split(".")
    
    missing_data_entries = 5
    log_errors_to_file = True
    log_missing_data_summary(testlogger, data_file, missing_data_entries, log_errors_to_file )
    
    msg = "There is missing price data in the data file: {}\n!!! ".format(data_file_basename) 
    msg += "WARNING: Number of missing values: {} !!!\n".format(missing_data_entries)
    msg += "  For details check file's read log: read_error_{0}_{1}.log.\n".format(data_file_name,
                                                                                 data_file_ext)
    
    expected_messages = [msg]
    actual_log_messages = caplog.messages
    
    assert expected_messages == actual_log_messages


@pytest.mark.parametrize('missing_data_entries', [1, 5, 10, 100])
def test_log_missing_data_summary_with_missing_entries_with_error_logging(missing_data_entries, caplog):
    # GIVEN: valid input parameters and missing_data_entries > 0
    # WHEN: log_missing_data_summary function is called
    # THEN: specified error messages gets logged
    data_file = "location/of/sample_data_file.csv"
    data_file_basename = os.path.basename(data_file)
    data_file_name, data_file_ext = data_file_basename.split(".")
    
    log_errors_to_file = True
    log_missing_data_summary(testlogger, data_file, missing_data_entries, log_errors_to_file )
    
    msg = "There is missing price data in the data file: {}\n!!! ".format(data_file_basename) 
    msg += "WARNING: Number of missing values: {} !!!\n".format(missing_data_entries)
    msg += "  For details check file's read log: read_error_{0}_{1}.log.\n".format(data_file_name,
                                                                                 data_file_ext)
    
    expected_messages = [msg]
    actual_log_messages = caplog.messages
    
    assert expected_messages == actual_log_messages

# Function in test: process_data_row()

def test_process_data_row_from_excel(sample_row_from_excel, region_list):
    # GIVEN: valid input parameters
    # WHEN: process_data_row function is called
    # THEN: (processed rows) generator object is returned
    
    data_row = sample_row_from_excel["data_row"]
    expected_data = sample_row_from_excel["expected_data"]
    processed_rows = process_data_row(data_row, region_list)
    rowcount = 0
    for row in processed_rows:
        print(row)
        assert row == expected_data[rowcount]
        rowcount += 1
        
def test_process_data_row_from_csv(sample_row_from_csv, region_list):
    # GIVEN: valid input parameters
    # WHEN: process_data_row function is called
    # THEN: (processed rows) generator object is returned
    
    data_row = sample_row_from_csv["data_row"]
    expected_data = sample_row_from_csv["expected_data"]
    processed_rows = process_data_row(data_row, region_list)
    rowcount = 0
    for row in processed_rows:
        print(row)
        assert row == expected_data[rowcount]
        rowcount += 1


def test_process_data_row_from_csv_returns_generator(sample_row_from_csv, region_list):
    # GIVEN: valid input parameters
    # WHEN: process_data_row function is called
    # THEN: (processed rows) generator object is returned
    
    processed_rows = process_data_row(sample_row_from_csv, region_list)
    assert inspect.isgenerator(processed_rows)
    
def test_process_data_row_from_excel_returns_generator(sample_row_from_excel, region_list):
    # GIVEN: valid input parameters
    # WHEN: process_data_row function is called
    # THEN: (processed rows) generator object is returned
    
    processed_rows = process_data_row(sample_row_from_excel, region_list)
    assert inspect.isgenerator(processed_rows)
    
def test_process_data_row_with_errors(sample_error_row, region_list):
    # GIVEN: rows with invalid price data
    # WHEN: process_data_row function is called
    # THEN: (transformed rows) generator object is returned
    
    data_row = sample_error_row["data_row"]
    expected_data = sample_error_row["expected_data"]
    processed_rows = process_data_row(data_row, region_list)
    rowcount = 0
    for row in processed_rows:
        print(row)
        assert row == expected_data[rowcount]
        rowcount += 1
    