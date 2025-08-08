# NOAA Web Service API Metadata Tools

A Python toolkit for efficiently fetching and managing weather data from the NOAA Web Services API. The library features intelligent caching through whitelists/blacklists to optimize API usage by tracking which stations have actual data and avoiding empty requests.

## Quick Start

### Prerequisites

- Python 3.8+
- NOAA API token (free from [NOAA Climate Data Online](https://www.ncdc.noaa.gov/cdo-web/token))

### Installation

```bash
pip install -r requirements.txt
```

### Environment Setup

Create a `.env` file in the project root:

```
NOAA_API_TOKEN=your_token_here
NOAA_API_URL=https://www.ncdc.noaa.gov/cdo-web/api/v2/
```

### Basic Usage

**Fetch weather stations:**

```python
import asyncio
from src.NOAAStations import NOAAStations

async def main():
    stations = NOAAStations()
    data = await stations.fetch_stations(
        datasetid='GSOM',  # Global Summary of the Month
        locationid='FIPS:BR'  # Brazil
    )
    print(f"Found {len(data['results'])} stations")

asyncio.run(main())
```

**Fetch weather data:**

```python
from src.NOAAData import NOAAData

async def main():
    noaa = NOAAData(
        datasetid='GSOM',
        startdate='2020-01-01',
        enddate='2020-12-31'
    )

    data = await noaa.fetch_location_by_stations(
        locationid='FIPS:BR',
        save=True  # Automatically save to CSV
    )

asyncio.run(main())
```

## Key Features

- **Smart Caching**: Automatically maintains whitelists of stations with data and blacklists of empty stations
- **Batch Processing**: Handles large date ranges by splitting into manageable chunks
- **Rate Limiting**: Respects NOAA API limits (5 requests/second)
- **Async Support**: Efficient concurrent requests for faster data retrieval
- **Auto-pagination**: Handles large datasets with automatic offset management
- **CSV Export**: Built-in data export functionality

## Project Structure

- `src/NOAAData.py` - Main class for fetching weather data
- `src/NOAAStations.py` - Station metadata and discovery
- `src/NOAALocations.py` - Geographic location management
- `src/whitelist.py` - Intelligent caching system
- `src/utils/` - Helper utilities for dates, logging, and data processing

## Testing

```bash
pytest
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
