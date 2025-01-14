Overview
This repository contains an ETL (Extract, Transform, Load) job that enriches restaurant data with:

Valid latitude and longitude values from the OpenCage Geocoding API (for rows missing lat/lng).
A 4-character Geohash for each valid restaurant coordinate.
Weather information, matched by Geohash, from a given set of Parquet weather data

.
├── restaurant_weather_etl.py   # Main ETL logic
├── zip_unpacker.py             # Script to unzip & merge weather data
├── test.py                     # Unit tests
├── README.md                   # (This file)
├── requirements.txt            # Dependencies

Preparing the Weather Data
zip_unpacker.py
A script to unzip and merge weather data from multiple .zip archives into a single Weather/ directory, ignoring macOS _MACOSX/ metadata.
Place your ZIP files in Weather_zipped/ (or any other folder).
Run:
```
python zip_unpacker.py
```
The extracted data will appear under Weather/.

Solution Outline
1. Check Restaurant Data for Missing Coordinates
Load CSV containing id, city, country, lat, lng.
Identify which rows have null lat/lng.
2. Enrich Missing Coordinates via OpenCage Geocoding API
Concatenate city, country into an address for geocoding.
Call OpenCage API to retrieve (lat, lng) if confidence >= 5.
3. Generate Geohash
Use a 4-character geohash for (lat, lng).
4. Left Join Restaurant & Weather Data
Read weather data from multiple Parquet files.
Generate geohash for weather coordinates, then join on geohash.
5. Store Enriched Data in Parquet Format
Write the joined data to enriched_data/, partitioned by geohash.

Testing
test.py contains unittest-based tests for both pure Python logic and Spark transformations.
