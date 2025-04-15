from loguru import logger
from typing import Optional


def format_log_content(
        context: Optional[str]=None,
        msg: Optional[str]=None,
        param_tuples: Optional[list[tuple[str, str]]]=None,
        only_values: bool=False) -> dict[str, str]:
    """Format the log content.

    Args:
        context (str, optional): A context to be logged.
        msg (str, optional): A message to be logged.
        param_tuples (list[tuple[str, str]], optional):
            The parameters to be included in the param_tuples key (e.g., [(key, value), ...])
        
    Returns:
        str: A string to be logged.
    """
    content = ""  # Initiate the log message
    # Mount log content
    if context:
        content = content + context

    if param_tuples:
        params = ""
        params = [str(value) for _, value in param_tuples] \
            if only_values else [f"{key}: {str(value)}" for key, value in param_tuples]
        param_tuples_str = " | " + " | ".join(params) if content else " | ".join(params)
        content = content + param_tuples_str

    if msg:
        if not content:
            content = content + msg
        else:
            content = content + " | " + msg
    return content

if __name__ == "__main__":
    log_data = format_log_content(context="Logger", param_tuples=[("Location", "BR"), ("Station", "ABC123"), ("Total", "1000"),])
    logger.success(log_data)

    log_data = format_log_content(context="Logger", msg="This is a test", param_tuples=[("Location", "BR"), ("Station", "ABC123"), ("Total", "1000"),])
    logger.debug(log_data)
