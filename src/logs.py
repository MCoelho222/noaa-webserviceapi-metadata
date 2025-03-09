import enum
from typing import TypedDict

class LogLevel(enum.Enum):
    TRACE = 5
    DEBUG = 10
    INFO = 20
    SUCCESS = 25
    WARNING = 30
    ERROR = 40
    CRITICAL = 50
    EXCEPTION = 60

class LogData(TypedDict):
    context: str | None
    params: dict[str, str] | None
    message: str