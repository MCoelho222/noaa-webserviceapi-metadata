from loguru import logger
from typing import Optional


def build_log_info(context: Optional[str] = None, msg: Optional[str] = None, params: Optional[list[tuple[str, str]]] = None) -> dict[str, str]:
    """Build the log content.

    Args:
        context (str, optional): A context to be logged.
        msg (str, optional): A message to be logged.
        params (list[tuple[str, str]], optional):
            The parameters to be included in the params key (e.g., [(key, value), ...])
        
    Returns:
        str: A string to be logged.
    """
    content = ""  # Initiate the log message
    # Mount log content
    if context:
        content = content + context
    if params:
        if content:
            params_str = " | " + " | ".join([f"{key.strip()}: {str(value).strip()}" for key, value in params])
        else:
            params_str = " | ".join([f"{key.strip()}: {str(value).strip()}" for key, value in params])
        content = content + params_str
    if msg:
        if not content:
            content = content + msg
        else:
            content = content + " | " + msg
    return content

if __name__ == "__main__":
    log_data = build_log_info(context="Logger", params=[("Location", "BR"), ("Station", "ABC123"), ("Total", "1000"),])
    logger.success(log_data)

    log_data = build_log_info(context="Logger", msg="This is a test", params=[("Location", "BR"), ("Station", "ABC123"), ("Total", "1000"),])
    logger.debug(log_data)
