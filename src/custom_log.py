from loguru import logger
from typing import Optional

from src.logs import LogData, LogLevel

def build_log_info(context: Optional[str] = None, msg: Optional[str] = None, params: Optional[list[tuple[str, str]]] = None) -> dict[str, str]:
    """Build the dictionary with the data to be logged.

    Args:
        context (str, optional): A context to be logged.
        msg (str, optional): A message to be logged.
        params (list[tuple[str, str]], optional):
            The parameters to be included in the params key (e.g., [(key, value), ...])
        
    Returns:
        dict: A dictionary with the data to be logged.
    """
    data = {'params': {}}

    if not context and not msg and not params:
        return {}
    
    if context:
        data['context'] = context

    if msg:
        data['message'] = msg

    if params:
        for param in params:
            data['params'][param[0]] = param[1]

    return data


def custom_logger(log_data: LogData, level: LogLevel = LogLevel.INFO) -> None:
    """Customize logging.

    Args:
        log_data (LogData): A dictionary with information to be displayed (context, parameters, and message)
        level (LogLevel): DEBUG | INFO | WARNING | ERROR | CRITICAL | SUCCESS | EXCEPTION
    """
    if not log_data:  # If 'log_data' is empty,
        logger.warning("Logger called with empty data")  # log a warning message
        return

    # Retrieve logging data
    context = log_data.get('context')
    params = log_data.get('params')
    msg = log_data.get('message')

    # If 'log_data' has only 'None' or empty strings log a warning message
    if all(not item for item in [context, params, msg]):
        logger.warning("Logger called with malformed data")
        return
    
    log_content = ""  # Initiate the log message

    # Mount log content string
    if context:
        log_content = log_content + context

    if params:
        if log_content:
            params_str = " | " + " | ".join([f"{key.strip()}: {str(value).strip()}" for key, value in params.items()])
        else:
            params_str = " | ".join([f"{key.strip()}: {str(value).strip()}" for key, value in params.items()])

        log_content = log_content + params_str

    if msg:
        if not log_content:
            log_content = log_content + msg
        else:
            log_content = log_content + " | " + msg

    # Log according to the specified log level
    if level == LogLevel.EXCEPTION:
        logger.opt(depth=1).exception(log_content)  # Special handling for EXCEPTION
    else:
        logger.opt(depth=1).log(level.name, log_content)


if __name__ == "__main__":
    log_data = build_log_info(context="Logger", params=[("Location", "BR"), ("Station", "ABC123"), ("Total", "1000"),])
    custom_logger(log_data, LogLevel.SUCCESS)

    log_data = build_log_info(context="Logger", msg="This is a test", params=[("Location", "BR"), ("Station", "ABC123"), ("Total", "1000"),])
    custom_logger(log_data, LogLevel.DEBUG)

    log_data = {'craft': 'Falcon 9', 'launch': 'CRS-23', 'status': 'Success'}
    custom_logger(log_data, LogLevel.INFO)