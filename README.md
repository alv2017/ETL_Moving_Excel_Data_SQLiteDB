# ETL: Moving prices data stored in *.xlsx and *.csv files to the SQLite database

### Business Case

We have a data visualisation application built in Excel. We want
to replace the existing Excel application with the web application. Web application has
the same functionality as the Excel application. The maintenance of the web application is
going to be performed by the company central office. The web application will be used
by the company offices all around the globe.

### Assignment

Our assignment is to migrate the data that is currently stored in *.xlsx and/or *.csv files
into the web application database. For simplicity for data migration we will use a SQLite
database. The data has to be moved to the table called **hourly_prices**

**hourly_prices** table structure:
(price_time, region, price)

Currently the SQLite database is located in the project folder called **db**,
and the database file is called prices.db. If there is a need one can modify 
the location of the database by editing the project file **settings.py**

The data files in *.xlsx are located in the project directory called data,
however one can modify the location of the data directory by editing the project file
**settings.py**

We also have files in *.csv format, the data in *.csv files is identical to the data in
*.xlsx files, and potentially we can migrate the data either from *.xlsx files or
from *.csv files.

### Solution

The main goal is to move the data reliably and consistently. This was achieved by
writing ETL scripts and implementing error logging. If one wants
to investigate the errors found during the data upload the log file
**logs/app.log** is a good starting point.

Data is of reasonably good quality, and, normally, it is difficult to capture discrepancies in 
this kind of data, however, thanks to implemented error logging, we managed to capture
errors within our data. The data errors may be reviewed by checking logs in the
project directory called **logs**.  

The project presented in the repository is a completed project, i.e. the data moving from
Excel files to the SQLite database is already completed. The data that was located in the
Excel files was moved to SQLite database **db/prices.db**. The data migration issues are 
listed in the logs. The logs are located in the project directory called **logs**.

In order to re-run the project one needs to empty the database **prices.db** and 
run the script

```
$ python -m data_loading
```

### Project Structure

```
ETL_Moving_PriceData_to_SQLite 
	|_ data
	|_ db
		|_ prices.db
	|
	|_ etl
		|_ db_operations.py
		|_ file_operatins.py
		|_ load_data_to_db.py
	|
	|_ logger
			|_ app_logger.py
			|_ insert_logger.py
			|_ log_errors.py
			|_ read_logger.py
	|
	|_ logs
	|_ data_loading.py
	|_ README.md
	|_ settings.py
```
	
#### Project Directories
	
**data**

The directory contains the files that have to be loaded to SQLite database.

**db**

The directory contains a SQLite database **prices.db**


#### Project Modules

**etl**

ETL module.

**logger**

Logger module. We have a separate loggers for data reading from files and data
uploading to SQLite database.

**logs**

The directory contains application and error logs.
The file called **app.log** contains the log of file uploading, and it is 
a good starting point for investigating other log files.

**data_loading.py**

The file contains a script that loads data from data files to SQLite data base.

**settings.py**

The file contains project settings.
	
	
	
	
	
