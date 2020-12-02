# Logging errors to specified error file

def copy_errors_to_error_log(error_logfile, data_file_name, logfile):
    with open(error_logfile, 'w') as errlog:
        with open(logfile) as log:
            for line in log:
                if data_file_name in line:
                    errlog.write(line)