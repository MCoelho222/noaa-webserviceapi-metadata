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

## Fetching Logic & Constraints

### API Limits & Rate Limiting

- **Request Rate**: Maximum 5 concurrent requests per second (enforced by semaphore + 0.2s delay)
- **Request Limit**: 1000 records per request (NOAA API default)
- **Retry Logic**: Exponential backoff for 503 errors (up to 5 retries: 1s, 2s, 4s, 8s, 16s)
- **Timeout Handling**: Automatic blacklisting of consistently failing requests

### Time Range Management

- **10-Year Rule**: Date ranges >10 years are automatically split into 10-year chunks
- **Chunk Processing**: Each chunk is processed independently to avoid API timeouts
- **Date Validation**: Start date must be before end date (raises ValueError if not)

```python
# Example: 25-year range automatically splits into 3 chunks
noaa = NOAAData(
    datasetid='GSOM',
    startdate='2000-01-01',  # Will be split into:
    enddate='2025-01-01'     # 2000-2010, 2010-2020, 2020-2025
)
```

### Pagination & Offsets

- **Auto-detection**: First fetches 1 record to determine total count
- **Offset Calculation**: Automatically calculates offsets in 1000-record increments
- **Progress Tracking**: Logs "Fetching offset X/Y..." for multi-page requests

```python
# For 3,500 records, generates offsets: [0, 1000, 2000, 3000]
# Fetches 4 separate requests and combines results
```

### Intelligent Caching

The library uses a sophisticated caching system with whitelists and blacklists to optimize API usage and avoid redundant requests.

#### Whitelist Structure & Content

Whitelists are JSON files that store successful location-station combinations with detailed metadata:

```json
{
  "target": "locationcategoryid",
  "description": "CNTRY",
  "metadata": {
    "created": "2025-08-08T11:41:54.705782+00:00",
    "updated": "2025-08-08T11:54:16.811311+00:00",
    "total_items": 151938,
    "total_size": "17.55 MB",
    "FIPS:AE": {
      "name": "United Arab Emirates",
      "items": 7917,
      "size": "938.91 KB",
      "status": "Complete",
      "count": "5/5" // successful stations / total stations
    }
  },
  "FIPS:AE": {
    "GHCND:AE000041196": {
      "items": 1527,
      "size": "181.23 KB"
    },
    "GHCND:AEM00041194": {
      "items": 2312,
      "size": "273.41 KB"
    }
  }
}
```

**Whitelist Features:**

- **Hierarchical Structure**: Country codes (FIPS:XX) contain station IDs with data counts
- **Size Tracking**: Monitors data volume per station and total cache size
- **Status Management**: Tracks completion status (Complete/Incomplete) per location
- **Success Ratios**: Records successful vs total stations for each location
- **Timestamps**: Creation and last update times for cache freshness

#### Blacklist Structure & Content

Blacklists are simple text files containing query strings that consistently return empty results:

```
datasetid=GSOM&startdate=2010-01-01&enddate=2019-12-31&stationid=GHCND:AGM00060506&locationid=FIPS:AG&limit=1000&includemetadata=True
datasetid=GSOM&startdate=2020-01-01&enddate=2025-08-08&stationid=GHCND:AFM00040948&locationid=FIPS:AF&limit=1000&includemetadata=True
```

**Blacklist Features:**

- **Full Query Storage**: Complete parameter strings for exact matching
- **Automatic Addition**: Failed requests automatically added after verification
- **Skip Logic**: Blacklisted queries are skipped entirely, saving API calls
- **Persistence**: Maintained across sessions to avoid repeat failures

#### Cache Behavior

**Whitelist Usage:**

- Before fetching stations for a location, checks if whitelist exists and is "Complete"
- If complete, uses only whitelisted stations (dramatically reduces API calls)
- New successful stations automatically added to whitelist
- Tracks data volume and success rates for performance monitoring

**Blacklist Usage:**

- Every request checked against blacklist before execution
- Failed requests (empty results) added to blacklist after confirmation
- Multi-chunk date ranges: individual failing chunks are blacklisted
- Prevents repeated attempts to fetch from known empty endpoints

**Performance Impact:**

- **API Call Reduction**: 70-90% fewer requests for previously scanned locations
- **Faster Execution**: Skip blacklisted requests immediately
- **Smart Pagination**: Only paginate through known good offsets
- **Progressive Learning**: System gets more efficient over time

### Logging & Monitoring

The library provides detailed logging at multiple levels:

- **INFO**: Request parameters, progress updates, successful data retrieval
- **SUCCESS**: Data found with count information
- **DEBUG**: Blacklisted requests, empty results, detailed offset tracking
- **ERROR**: API errors, authentication issues, parsing failures
- **WARNING**: Large date ranges that may take significant time

```python
# Example log output:
# INFO: Fetching data... [datasetid=GSOM, locationid=FIPS:BR, startdate=2020-01-01]
# INFO: Fetching offset 2/4...
# SUCCESS: Data found for range 2020-01-01 to 2020-12-31 [Returned items: 850/850]
# DEBUG: Blacklisted. Skipping... [datasetid=GSOM, stationid=GHCND:EMPTY001]
```

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
