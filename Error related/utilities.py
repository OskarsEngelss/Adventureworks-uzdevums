import datetime
import json

def log_error_to_warehouse(hook, source_table, natural_key, error, failed_data="{}"):
    """
    Centralized Error Logger. 
    Logs row-level failures into the error_records table.
    """
    error_type = type(error).__name__
    # Clean up error message for SQL (handle single quotes)
    error_msg = str(error).replace("'", "''")[:500] 
    
    # Ensure failed_data is a string (JSON)
    if not isinstance(failed_data, str):
        failed_data = json.dumps(failed_data)
    failed_data = failed_data.replace("'", "''")

    log_sql = f"""
        INSERT INTO adventureworks_errors.error_records 
        (ErrorDate, SourceTable, RecordNaturalKey, ErrorType, ErrorSeverity, 
         ErrorMessage, FailedData, IsRecoverable, RetryCount, IsResolved)
        VALUES (NOW(), '{source_table}', '{natural_key}', '{error_type}', 
                'Warning', '{error_msg}', '{failed_data}', 1, 0, 0)
    """
    hook.run(log_sql)

def get_yesterday():
    """Returns yesterday's date string"""
    import pendulum
    return pendulum.now().subtract(days=1).to_date_string()