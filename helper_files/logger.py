# logger.py

_log_file = None

def initialize_logger(filename="application.log", mode="a"):
    global _log_file
    if _log_file is not None and not _log_file.closed:
        print(f"Warning: Logger already initialized with file '{_log_file.name}'. Closing previous file.")
        _log_file.close()
    try:
        _log_file = open(filename, mode)
        print(f"Logger initialized. Messages will be written to '{filename}'.")
    except IOError as e:
        print(f"Error: Could not open log file '{filename}': {e}")
        _log_file = None

def log(message):
    global _log_file
    if _log_file:
        try:
            _log_file.write(str(message) + '\n')
            _log_file.flush()
        except Exception as e:
            print(f"Error writing to log file: {e} - Message: {message}")
    else:
        print(f"Logger not initialized. Printing to console: {message}")

def close_logger():
    global _log_file
    if _log_file:
        try:
            _log_file.close()
            print(f"Logger closed. File '{_log_file.name}' is now closed.")
        except Exception as e:
            print(f"Error closing log file: {e}")
        finally:
            _log_file = None