import os
from etl.file_operations import read_csv_file_data, read_excel_file_data
from etl.db_operations import create_connection, insert_hourly_price
from settings import LOG_DIR, INSERT_LOG_FILE, TABLE_NAME
from logger.app_logger import logger as applogger
from logger.log_errors import copy_errors_to_error_log
  
def load_file_to_db(conn, data_file, wsname=None, tblname = TABLE_NAME):
    
    data_file_name = os.path.basename(data_file)
    data_file_basename = data_file_name.split(".")[0]
    data_file_ext = data_file_name.split(".")[1]
    
    #log reference
    logref = data_file_name
    
    if data_file_ext == "csv":
        data = read_csv_file_data(data_file)
    elif data_file_ext == "xlsx":
        data = read_excel_file_data(data_file, wsname=wsname)
    else:
        msg = "Unknown file format: {0}. ".format(data_file_name)
        msg += "File data won't be processed."
        applogger.error(msg)
        return -1
    
    msg = "Loading data from file to DB:  {0}".format(data_file_name)
    applogger.info(msg)
    
    processed_records = 0
    inserted_records = 0
    for data_tuple in data:
        rowid = insert_hourly_price(conn, data_tuple, logref, tblname)
        processed_records += 1
        if rowid > 0:
            inserted_records += 1

    msg = "Data insertion is over."
    msg += "\n  Number of processed records: {}".format(processed_records)
    msg += "\n  Number of inserted records {}.\n".format(inserted_records)
    if processed_records > inserted_records:
        msg += "!!! WARNING: Insertion of {0} row(s) failed !!!\n".format(processed_records - inserted_records)
        data_logfile_name = "insert_error_" + \
            data_file_basename + "_" + data_file_ext + ".log"
        data_logfile = os.path.join(LOG_DIR, data_logfile_name)
        copy_errors_to_error_log(data_logfile, data_file_name, INSERT_LOG_FILE)
        
        msg += "  Failed insertions log:\n  {0}\n".format(data_logfile) 
    applogger.info(msg)
    conn.commit()
    return inserted_records
       
if __name__ == "__main__":
    from settings import DATA_DIR
    conn = create_connection()
    
    csv_file = "elspot-prices_2018_hourly_eur.csv"
    csv_flocation = os.path.join(DATA_DIR, csv_file)
    
    excel_file = "elspot-prices_2018_hourly_eur.xlsx"
    excel_flocation = os.path.join(DATA_DIR, excel_file)
    
    conn = create_connection()
    try:
        load_file_to_db(conn, excel_flocation)
        load_file_to_db(conn, csv_flocation)
    except Exception as e:
        raise e
    finally:
        conn.close()
    
    
    
    
    

