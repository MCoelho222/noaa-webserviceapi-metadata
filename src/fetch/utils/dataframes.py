import pandas as pd

def make_dataframe(noaa_json: dict) -> pd.DataFrame:
    # Convert to DataFrame
    df = pd.DataFrame(noaa_json)

    # Convert date to datetime format
    df['date'] = pd.to_datetime(df['date'])

    # If 'value' represents temperature in tenths of degrees Celsius (NOAA convention), convert it
    # df['value'] = df['value'] / 10  # Convert from tenths of °C to °C

    # Rename 'value' to 'temperature_C' for clarity
    # df.rename(columns={'value': 'temperature_C'}, inplace=True)

    # Display DataFrame
    # print(df)
    return df