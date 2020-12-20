import os
from etl.load_data_to_db import log_insertion_error
from etl.load_data_to_db import log_insertion_summary

# Function in test log_insertion_error()

def test_log_insertion_error(a_price_tuple, test_logger, caplog):
    # GIVEN: valid input parameters
    # WHEN: log_insertion_error function is called
    # THEN: the error message gets logged
    
    
    data_tuple = a_price_tuple
    logger = test_logger
    db_error_msg = "SQLite database error"
    ref_file = "some/data/file.csv"
    ref_file_basename = os.path.basename(ref_file)
    
    expected_log_messages = ["{0}: Data insertion failed: {1}. {2}.".format(ref_file_basename, 
                                                            str(data_tuple), str(db_error_msg))]
    
    log_insertion_error(logger, data_tuple, db_error_msg, ref_file)
    actual_log_messages = caplog.messages
    
    assert actual_log_messages == expected_log_messages
     
# Function in test log_insertion_summary()
    
def test_log_insertion_summary_no_insert_errors_log_errors_true(test_logger, caplog):
    # GIVEN: valid input parameters, and no insertion errors (processed = inserted)
    # WHEN: log_insertion_error function is called
    # THEN: the specified error message gets logged
    logger = test_logger
    ref_file = "some/data/file.csv"
    number_of_processed = 10
    number_of_inserted = 10
    log_errors_to_file = True
    
    msg = "Data insertion is over."
    msg += "\n  Number of processed records: {}".format(number_of_processed)
    msg += "\n  Number of inserted records {}.\n".format(number_of_inserted)
    expected_log_messages = [msg]
    
    log_insertion_summary(logger, ref_file, number_of_processed, number_of_inserted, 
                          log_errors_to_file)
    
    actual_log_messages = caplog.messages
    assert actual_log_messages == expected_log_messages
    

def test_log_insertion_summary_no_insert_errors_log_errors_false(test_logger, caplog):
    # GIVEN: valid input parameters, and no insertion errors (processed = inserted)
    # WHEN: log_insertion_error function is called
    # THEN: the specified error message gets logged
    logger = test_logger
    ref_file = "some/data/file.csv"
    number_of_processed = 10
    number_of_inserted = 10
    log_errors_to_file = False
    
    msg = "Data insertion is over."
    msg += "\n  Number of processed records: {}".format(number_of_processed)
    msg += "\n  Number of inserted records {}.\n".format(number_of_inserted)
    expected_log_messages = [msg]
    
    log_insertion_summary(logger, ref_file, number_of_processed, number_of_inserted, 
                          log_errors_to_file)
    
    actual_log_messages = caplog.messages
    assert actual_log_messages == expected_log_messages

def test_log_insertion_summary_with_insert_errors_log_errors_true(test_logger, caplog):
    # GIVEN: valid input parameters, and insertion errors (processed > inserted)
    # WHEN: log_insertion_error function is called
    # THEN: the specified error message gets logged
    logger = test_logger
    ref_file = "some/data/file.csv"
    ref_file_name, ref_file_ext = os.path.basename(ref_file).split(".")
    number_of_processed = 20
    number_of_inserted = 10
    log_errors_to_file = True
    # Expected logged message
    msg = "Data insertion is over."
    msg += "\n  Number of processed records: {}".format(number_of_processed)
    msg += "\n  Number of inserted records {}.\n".format(number_of_inserted)
    msg += "!!! WARNING: Insertion of {0} row(s) failed !!!\n".format(number_of_processed - number_of_inserted)
    data_logfile_basename = "insert_error_" + ref_file_name + "_" + ref_file_ext + ".log"
    msg += "  Failed insertions log: {0}\n".format(data_logfile_basename) 
    expected_log_messages = [msg]
    
    log_insertion_summary(logger, ref_file, number_of_processed, number_of_inserted, 
                          log_errors_to_file)
    
    actual_log_messages = caplog.messages
    assert actual_log_messages == expected_log_messages

def test_log_insertion_summary_with_insert_errors_log_errors_false(test_logger, caplog):
    # GIVEN: valid input parameters, and insertion errors (processed > inserted)
    # WHEN: log_insertion_error function is called
    # THEN: the specified error message gets logged
    logger = test_logger
    ref_file = "some/data/file.csv"
    number_of_processed = 20
    number_of_inserted = 10
    log_errors_to_file = False
    # Expected logged message
    msg = "Data insertion is over."
    msg += "\n  Number of processed records: {}".format(number_of_processed)
    msg += "\n  Number of inserted records {}.\n".format(number_of_inserted)
    msg += "!!! WARNING: Insertion of {0} row(s) failed !!!\n".format(number_of_processed - number_of_inserted)
    expected_log_messages = [msg]
    
    log_insertion_summary(logger, ref_file, number_of_processed, number_of_inserted, 
                          log_errors_to_file)
    
    actual_log_messages = caplog.messages
    assert actual_log_messages == expected_log_messages

