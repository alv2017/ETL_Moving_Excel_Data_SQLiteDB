import os
from sqlite3 import Error as SQLiteError
from etl.file_operations import read_csv_file_data, read_excel_file_data
from etl.db_operations import create_connection, insert_hourly_price
from settings import INSERT_LOG_FILE, TABLE_NAME
from etl.logging_errors import copy_errors_to_error_log

def log_insertion_error(logger, data_tuple, db_error_msg, ref_file):
    ref_file_basename = os.path.basename(ref_file)
    msg = "{0}: Data insertion failed: {1}. {2}.".format(ref_file_basename, str(data_tuple),
                                                                str(db_error_msg))
    logger.error(msg)
    
def log_insertion_summary(logger, ref_file, number_of_processed, number_of_inserted, 
                          log_errors_to_file=True):
    ref_file_basename = os.path.basename(ref_file)
    ref_file_name, ref_file_ext = ref_file_basename.split(".")
    
    msg = "Data insertion is over."
    msg += "\n  Number of processed records: {}".format(number_of_processed)
    msg += "\n  Number of inserted records {}.\n".format(number_of_inserted)
    
    if number_of_processed > number_of_inserted:
        msg += "!!! WARNING: Insertion of {0} row(s) failed !!!\n".format(number_of_processed - number_of_inserted)
        if log_errors_to_file:
            data_logfile_basename = "insert_error_" + \
                ref_file_name + "_" + ref_file_ext + ".log"
            msg += "  Failed insertions log: {0}\n".format(data_logfile_basename) 
        
    logger.info(msg)
  
def load_file_to_db(conn, data_file, app_logger, read_logger, insert_logger, 
                    log_errors_to_file = True,
                    insert_logfile = INSERT_LOG_FILE,
                    wsname=None, tblname = TABLE_NAME):
    
    data_file_basename = os.path.basename(data_file)
    data_file_name = data_file_basename.split(".")[0]
    data_file_ext = data_file_basename.split(".")[1]
    
    #log reference
    logref = data_file_basename
    
    if data_file_ext == "csv":
        data = read_csv_file_data(data_file, app_logger, read_logger, 
                                  log_errors_to_file=log_errors_to_file)
    elif data_file_ext == "xlsx":
        data = read_excel_file_data(data_file, app_logger, read_logger, 
                                    log_errors_to_file=log_errors_to_file, wsname=wsname)
    else:
        msg = "Unknown file format: {0}. ".format(data_file_name)
        msg += "File data won't be processed."
        app_logger.error(msg)
        return -1
    
    msg = "Loading data from file to DB:  {0}".format(data_file_basename)
    app_logger.info(msg)
    
    processed_records = 0
    inserted_records = 0
    for data_tuple in data:
        processed_records += 1
        try:
            rowid = insert_hourly_price(conn, data_tuple, logref, tblname)
            if rowid > 0:
                inserted_records += 1
        except SQLiteError as err:
            log_insertion_error(insert_logger, data_tuple, err, data_file_basename)
    
    log_insertion_summary(app_logger, data_file, processed_records, inserted_records, 
                  log_errors_to_file)
    
    if processed_records > inserted_records and log_errors_to_file==True:
        copy_errors_to_error_log('insert_error', data_file_basename, insert_logfile)
        
    conn.commit()
    return inserted_records
       
if __name__ == "__main__":
    from settings import DATA_DIR
    from logger.app_logger import logger as app_logger
    from logger.read_logger import logger as read_logger
    from logger.insert_logger import logger as insert_logger
    #from logger.testing_logger import logger


    conn = create_connection()
    
    csv_file = "elspot-prices_2018_hourly_eur.csv"
    csv_flocation = os.path.join(DATA_DIR, csv_file)
    
    #excel_file = "elspot-prices_2018_hourly_eur.xlsx"
    #excel_flocation = os.path.join(DATA_DIR, excel_file)
    
    conn = create_connection()
    try:
        #load_file_to_db(conn, excel_flocation, app_logger, read_logger, insert_logger)
        load_file_to_db(conn, csv_flocation, app_logger, read_logger, insert_logger)
    except Exception as e:
        raise e
    finally:
        conn.close()
    