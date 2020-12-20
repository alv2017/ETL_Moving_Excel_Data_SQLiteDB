# Logging errors to specified error log file
import os

def copy_errors_to_error_log(error_logfile_prefix, datafile, logfile):
    
    error_logfile_dir = os.path.dirname(logfile)
    
    datafile_basename, datafile_ext = os.path.basename(datafile).split(".")
    error_logfile_name = error_logfile_prefix + "_" + datafile_basename + "_" + \
        datafile_ext + ".log"
        
    error_logfile = os.path.join(error_logfile_dir, error_logfile_name)
    
    with open(error_logfile, 'w') as errlog:
        with open(logfile) as log:
            for line in log:
                if os.path.basename(datafile) in line:
                    errlog.write(line)
                    