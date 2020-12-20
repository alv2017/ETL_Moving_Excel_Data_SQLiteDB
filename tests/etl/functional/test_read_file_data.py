import pytest
import os
import inspect
from etl.file_operations import read_csv_file_data
from etl.file_operations import read_excel_file_data
from settings import TEST_DATA_DIR 

@pytest.fixture()
def csv_file_location():
    return os.path.join(TEST_DATA_DIR, "sample_data_file.csv")

@pytest.fixture()
def excel_file_location():
    return os.path.join(TEST_DATA_DIR, "sample_data_file.xlsx")

# Function in test: read_csv_file_data()
def test_read_csv_file_data_returns_generator_object(csv_file_location,
                                                     test_logger):
    datagen = read_csv_file_data(csv_file_location, 
                                 app_logger=test_logger, read_logger=test_logger,
                                 log_errors_to_file=False)
    assert inspect.isgenerator(datagen)
    
def test_read_csv_file_data_generator_yields_10_items(csv_file_location, 
                                                      test_logger):
    """Only records with non-missing prices are yielded
    """
    datagen = read_csv_file_data(csv_file_location, 
                                 app_logger=test_logger, read_logger=test_logger,
                                 log_errors_to_file=False)
    cnt = 0
    for item in datagen:
        cnt += 1
    assert cnt == 10
    
def test_read_csv_file_data_generator_yields_tuples(csv_file_location, 
                                                      test_logger):
    datagen = read_csv_file_data(csv_file_location, 
                                 app_logger=test_logger, read_logger=test_logger,
                                 log_errors_to_file=False)
    for item in datagen:
        assert isinstance(item, tuple)
        
def test_read_csv_file_data_generator_yields_tuples_of_length_3(csv_file_location, 
                                                      test_logger):
    datagen = read_csv_file_data(csv_file_location, 
                                 app_logger=test_logger, read_logger=test_logger,
                                 log_errors_to_file=False)
    for item in datagen:
        assert len(item) == 3
        
def test_read_csv_file_data_contains_5_unique_regions(csv_file_location, 
                                                      test_logger):
    datagen = read_csv_file_data(csv_file_location, 
                                 app_logger=test_logger, read_logger=test_logger,
                                 log_errors_to_file=False)
    regions = set()
    for item in datagen:
        regions.add(item[1])
    assert len(regions)
    
def test_read_csv_file_data_contains_set_of_regions(csv_file_location, 
                                                      test_logger):
    expected_regions = {"SYS", "FI", "EE", "LT", "LV"}
    datagen = read_csv_file_data(csv_file_location, 
                                 app_logger=test_logger, read_logger=test_logger,
                                 log_errors_to_file=False)
    regions = set()
    for item in datagen:
        regions.add(item[1])
    assert regions == expected_regions
    
def test_read_csv_file_data_contains_2_unique_dates(csv_file_location, 
                                                      test_logger):
    
    datagen = read_csv_file_data(csv_file_location, 
                                 app_logger=test_logger, read_logger=test_logger,
                                 log_errors_to_file=False)
    dates = set()
    for item in datagen:
        dates.add(item[0])
    assert len(dates) == 2
    
def test_read_csv_file_data_contains_set_of_dates(csv_file_location, 
                                                      test_logger):
    expected_dates = {"2018-01-01 01:00", "2018-01-01 02:00"}
    datagen = read_csv_file_data(csv_file_location, 
                                 app_logger=test_logger, read_logger=test_logger,
                                 log_errors_to_file=False)
    dates = set()
    for item in datagen:
        dates.add(item[0])
    assert dates == expected_dates
    
# Function in test: read_excel_file_data()
def test_read_excel_file_data_returns_generator_object(excel_file_location,
                                                     test_logger):
    datagen = read_excel_file_data(excel_file_location, 
                                   app_logger=test_logger, read_logger=test_logger,
                                   log_errors_to_file=False)
    assert inspect.isgenerator(datagen)
    
def test_read_excel_file_data_generator_yields_10_items(excel_file_location, 
                                                      test_logger):
    """Only records with non-missing prices are yielded
    """
    datagen = read_excel_file_data(excel_file_location, 
                                   app_logger=test_logger, read_logger=test_logger,
                                   log_errors_to_file=False)
    cnt = 0
    for item in datagen:
        cnt += 1
    assert cnt == 10
    
def test_read_excel_file_data_generator_yields_tuples(excel_file_location, 
                                                      test_logger):
    datagen = read_excel_file_data(excel_file_location, 
                                   app_logger=test_logger, read_logger=test_logger,
                                   log_errors_to_file=False)
    for item in datagen:
        assert isinstance(item, tuple)
        
def test_read_excel_file_data_contains_5_unique_regions(excel_file_location, 
                                                      test_logger):
    datagen = read_excel_file_data(excel_file_location, 
                                   app_logger=test_logger, read_logger=test_logger,
                                   log_errors_to_file=False)
    regions = set()
    for item in datagen:
        regions.add(item[1])
    assert len(regions)
    
def test_read_excel_file_data_contains_set_of_regions(excel_file_location, 
                                                      test_logger):
    expected_regions = {"SYS", "FI", "EE", "LT", "LV"}
    datagen = read_excel_file_data(excel_file_location, 
                                   app_logger=test_logger, read_logger=test_logger,
                                   log_errors_to_file=False)
    regions = set()
    for item in datagen:
        regions.add(item[1])
    assert regions == expected_regions

def test_read_excel_file_data_contains_2_unique_dates(excel_file_location, 
                                                      test_logger):
    
    datagen = read_excel_file_data(excel_file_location, 
                                   app_logger=test_logger, read_logger=test_logger,
                                   log_errors_to_file=False)
    dates = set()
    for item in datagen:
        dates.add(item[0])
    assert len(dates) == 2
    
def test_read_excel_file_data_contains_set_of_dates(excel_file_location, 
                                                      test_logger):
    expected_dates = {"2018-01-01 01:00", "2018-01-01 02:00"}
    datagen = read_excel_file_data(excel_file_location, 
                                   app_logger=test_logger, read_logger=test_logger,
                                   log_errors_to_file=False)
    dates = set()
    for item in datagen:
        dates.add(item[0])
    assert dates == expected_dates

