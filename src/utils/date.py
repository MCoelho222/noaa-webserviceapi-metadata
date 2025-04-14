from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta


def is_more_than_10_years(start_date_str, end_date_str):
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    
    ten_years_later = start_date + relativedelta(years=10)
    return end_date > ten_years_later

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


def generate_year_date_range(start_date_str: str, end_date_str: str, interval_years: int) -> tuple[str, str]:
    # Convert strings to datetime objects
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    
    ranges = []
    current_start = start_date

    while current_start <= end_date:
        try:
            # Calculate next end date by adding interval years
            next_start = current_start.replace(year=current_start.year + interval_years)
        except ValueError:
            # Handle leap years
            next_start = current_start.replace(month=3, day=1, year=current_start.year + interval_years)

        # Subtract one day to get the actual end of current range
        current_end = next_start - timedelta(days=1)

        # Clip the last range end to end_date if it goes past
        if current_end > end_date:
            current_end = end_date

        ranges.append((
            current_start.strftime("%Y-%m-%d"),
            current_end.strftime("%Y-%m-%d")
        ))

        # Move to the next start date
        current_start = current_end + timedelta(days=1)

    return ranges
if __name__ == "__main__":
    divide_date_range('2020-01-01', '2021-12-31', 3)
    print(generate_year_date_range('1900-01-01', '2025-04-14', 10))