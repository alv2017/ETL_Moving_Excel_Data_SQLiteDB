import csv
import os
from openpyxl import load_workbook
from settings import LOG_DIR, READ_LOG_FILE
from logger.read_logger import logger as readlogger
from logger.app_logger import logger as applogger
from logger.log_errors import copy_errors_to_error_log

def read_csv_file_data(csv_file, skip_n_rows = 2):
    """ The function reads data from the pre-formatted csv file and logs data errors
        to the corresponding log file.
        :params csv_file: csv file from which we are going to read data
        :type csv_file: str
        :params skip_n_rows: the number of rows to skip, the default value is 2
        :type skip_n_rows: int
        :return: data tuples generator. Data tuples are of the form (price_time, region, price) 
    """
    csv_file_name = os.path.basename(csv_file)
    
    # log info
    msg = "Reading data from:\n  {0}".format(csv_file)
    applogger.info(msg)
    
    # missing data counter
    missing_data_cnt = 0

    with open(csv_file, newline='') as csvf:
        datareader = csv.reader(csvf, delimiter = ',', quotechar = '"')
        
        # Skipping n rows
        for i in range(skip_n_rows):
            next(datareader)
            
        # Regions
        regions = next(datareader)[2:]
        nregions = len(regions)
            
        # Data processing
        for row in datareader:
            # price time
            date = row[0].split("-")
            day = date[0]
            month = date[1]
            year = date[2]
            hour = row[1][:2]
            
            dt = str(year) + "-" + str(month) + "-" + str(day) + " " + str(hour) + ":" + "00"
            
            # Regions and Price
            for i in range(nregions):
                price = str(row[i+2])
 
                if len(price) > 0:
                    data_tuple = (dt, regions[i], price )
                    yield(data_tuple)
                else:
                    # Log missing price data
                    data_tuple = (dt, regions[i], price)
                    missing_data_cnt += 1
                    msg = "{0}: The price is missing: {0}.".format(csv_file_name, str(data_tuple))
                    readlogger.error("{0}: The price is missing: {1}.".format(
                        csv_file_name, str(data_tuple)))
    
    # Log info on missing price data          
    if missing_data_cnt > 0:
        msg = "There is missing price data in the data file:\n  {0}\n".format(csv_file)
        msg += "!!! WARNING: Number of missing values: {0} !!!\n".format(missing_data_cnt)
        readlog_name = "read_error_" + os.path.basename(csv_file).split(".")[0] + "_csv.log"
        readlog = os.path.join(LOG_DIR, readlog_name)
        msg += "  For details check read log:\n  {0}.".format(readlog)
        applogger.info(msg)
        copy_errors_to_error_log(readlog, csv_file_name, READ_LOG_FILE)

        
def read_excel_file_data(excel_file, wsname = None):
    """ The function reads data from the pre-formatted csv file and logs data errors
        to the corresponding log file.
        :params csv_file: csv file from which we are going to read data
        :type csv_file: str
        :params skip_n_rows: the number of rows to skip, the default value is 2
        :type skip_n_rows: int
        :return: data tuples generator. Data tuples are of the form (price_time, region, price) 
    """
    
    # log info
    msg = "Reading data from {0}".format(excel_file)
    applogger.info(msg)
    
    # missing data counter
    missing_data_cnt = 0
    
    if wsname is None:
        wsname = os.path.basename(excel_file).split(".")[0]
    
    wb = load_workbook(excel_file)
    ws = wb[wsname]
    
    # regions
    regions = []
    for cell in ws[3][2:]:
        regions.append(cell.value)
    nregions = len(regions)

    # data records
    for row in ws.iter_rows(min_row=4, min_col=1, values_only=True):
        # Price Date
        date = row[0].split('-')
        day = date[0]
        month = date[1]
        year = date[2]
        hour = row[1][:2]
        dt = str(year) + "-" + str(month) + "-" + str(day) + " " + str(hour) + ":" + "00"
        
        # Regions and Price
        for i in range(nregions):
            price = row[i+2]
            if price is not None:
                price = str(price)
            else:
                price = ''
                
            if len(price) > 0:
                data_tuple = (dt, regions[i], price )
                yield(data_tuple)
            else:
                # Log missing price data
                data_tuple = (dt, regions[i], price)
                missing_data_cnt += 1
                excel_file_name = os.path.basename(excel_file)
                msg = "{0}: The price is missing: {0}.".format(excel_file_name, str(data_tuple))
                readlogger.error("{0}: The price is missing: {1}.".format(
                    excel_file_name, str(data_tuple)))

    # Log info on missing price data          
    if missing_data_cnt > 0:
        msg = "There is missing price data in the data file:\n  {0}\n".format(excel_file)
        msg += "!!! WARNING: Number of missing values: {0} !!!\n".format(missing_data_cnt)
        readlog_name = "read_error_" + os.path.basename(excel_file).split(".")[0] + "_xlsx.log"
        readlog = os.path.join(LOG_DIR, readlog_name)
        msg += "  For details check read log:\n  {0}.".format(readlog)
        applogger.info(msg)
        copy_errors_to_error_log(readlog, excel_file_name, READ_LOG_FILE)

                
if __name__ == "__main__":
    from settings import DATA_DIR
    
    csv_cnt = 0
    excel_cnt = 0
    
    csv_file = os.path.join(DATA_DIR, "elspot-prices_2018_hourly_eur.csv")
    excel_file = os.path.join(DATA_DIR, "elspot-prices_2018_hourly_eur.xlsx")
    
    excel_data = read_excel_file_data(excel_file)
    csv_data = read_csv_file_data(csv_file)
    
        
    for row in excel_data:
        excel_cnt += 1
        
    for row in csv_data:
        csv_cnt += 1
        
    print("Number of db entries from excel file: {0}".format(excel_cnt))
    print("Number of db entries from csv file: {0}".format(csv_cnt))
    
    

    