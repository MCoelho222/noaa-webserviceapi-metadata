from datetime import datetime
from dateutil.relativedelta import relativedelta

def divide_date_range(start_date: str, end_date: str, step_months: int) -> list[tuple[str, str]]:
    """Split a date range into smaller ranges.

    Args:
        start_date (str): The start date from the range.
        end_date (str): The end date from the range.
        step_months (int): The steps to split the range in month-based units

    Returns:
        list: A list with tuples representing each smaller date range (e.g., [(2020-01-01, 2020-04-01), ...])
    """
    # Parse timestamps
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    intervals = []  # To store smaller ranges

    # Start making the smaller intervals by incrementing 'step_months' to the start date
    # while it's lower than the end date
    current_start = start
    while current_start < end:
        current_end = min(current_start + relativedelta(months=step_months), end)
        intervals.append((current_start.strftime("%Y-%m-%d"), current_end.strftime("%Y-%m-%d")))
        current_start = current_end  # Move to the next interval

    return intervals

if __name__ == "__main__":
    # Test
    divide_date_range('2020-01-01', '2021-12-31', 3)