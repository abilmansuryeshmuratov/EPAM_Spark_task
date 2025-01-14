from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, when
import requests
import geohash as geohash
import os
from pyspark.sql.functions import concat_ws

def create_spark_session():
    return SparkSession.builder \
        .appName("Restaurant-Weather-ETL") \
        .config("spark.driver.memory", "4g") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.driver.extraClassPath", "hadoop-common-3.3.4.jar") \
        .config("spark.driver.extraJavaOptions", "-Dos.arch=amd64") \
        .master("local[*]") \
        .getOrCreate()

def geocode_address(address, api_key):

    # Get latitude and longitude from address using OpenCage Forward Geocoding API
    
    base_url = "https://api.opencagedata.com/geocode/v1/json"
    
    params = {
        'q': address,
        'key': api_key,
        'limit': 1,
        'no_annotations': 1,
        'no_record': 1
    }
    
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()
        
        if data.get('status', {}).get('code') != 200:
            print(f"OpenCage API error: {data.get('status', {}).get('message')}")
            return None, None
        
        if not data.get('results'):
            print(f"No results found for address: {address}")
            return None, None
        
        first_result = data['results'][0]
        confidence = first_result.get('confidence', 0)
        
        if confidence < 5:
            print(f"Low confidence ({confidence}/10) for address: {address}")
            return None, None
            
        location = first_result['geometry']
        return location['lat'], location['lng']
        
    except requests.exceptions.RequestException as e:
        print(f"Request error while geocoding address '{address}': {e}")
    except (KeyError, ValueError) as e:
        print(f"Error parsing geocoding response for address '{address}': {e}")
    except Exception as e:
        print(f"Unexpected error while geocoding address '{address}': {e}")
    
    return None, None

def create_geocoding_udf(api_key):
    
    # Create a UDF for geocoding addresses with rate limiting, retries, and a simple in-process cache to avoid repeated calls for the same address.
    
    from time import sleep
    from functools import wraps

    geocode_cache = {}
    
    def rate_limited(max_retries=3, delay_seconds=1):
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                for attempt in range(max_retries):
                    try:
                        result = func(*args, **kwargs)
                        if result != (None, None):
                            return result
                        if attempt < max_retries - 1:
                            sleep(delay_seconds * (attempt + 1))
                    except Exception as e:
                        if attempt == max_retries - 1:
                            print(f"[FINAL RETRY FAILED] Address: {args}, Error: {e}")
                            return None, None
                        sleep(delay_seconds * (attempt + 1))
                return None, None
            return wrapper
        return decorator
    
    @rate_limited()
    def geocode(address):
        if not address or not isinstance(address, str):
            return None, None
        # Call the actual function
        return geocode_address(address, api_key)
    
    def geocode_wrapper(address):
        
        # CHECK THE CACHE FIRST:
        if address in geocode_cache:
            return geocode_cache[address]

        lat, lng = geocode(address)
        lat_lng = [lat, lng] if lat is not None and lng is not None else [None, None]
        geocode_cache[address] = lat_lng
        return lat_lng
    
    return udf(geocode_wrapper, "array<double>")

def generate_geohash(lat, lng):
    # Generate 4-character geohash from latitude and longitude
    try:
        if lat is not None and lng is not None:
            return geohash.encode(float(lat), float(lng), precision=4)
    except Exception as e:
        print(f"Error generating geohash: {e}")
    return None

def process_restaurants(spark, input_path, api_key):
    # Load CSV
    restaurants_df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Create a new 'address' column by concatenating city, country
    restaurants_df = restaurants_df.withColumn(
        "address",
        concat_ws(", ", col("city"), col("country"))
    )

    # Create UDF for geocoding
    geocode_udf = create_geocoding_udf(api_key)

    # Geocode where lat/lng is null
    restaurants_with_coords = restaurants_df.withColumn(
        "geocoded",
        when(
            (col("lat").isNull() | col("lng").isNull()),
            geocode_udf(col("address"))
        )
    )

    final_restaurants = restaurants_with_coords \
        .withColumn("lat", when(col("lat").isNull(), col("geocoded")[0]).otherwise(col("lat"))) \
        .withColumn("lng", when(col("lng").isNull(), col("geocoded")[1]).otherwise(col("lng")))

    # Generate 4-character geohash
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType

    geohash_udf = udf(generate_geohash, StringType())

    final_restaurants = final_restaurants.withColumn(
        "geohash",
        geohash_udf(col("lat"), col("lng"))
    )

    return final_restaurants

def scan_weather_folders(weather_path):
    #Scan weather folder structure to find all year/month/day combinations.
    # Ignores .DS_Store and other hidden files.
    
    periods = []
    
    for root, dirs, files in os.walk(weather_path):
        dirs[:] = [d for d in dirs if not d.startswith('.')]
        path_parts = root.split(os.sep)
        year_part = next((p for p in path_parts if p.startswith('year=')), None)
        month_part = next((p for p in path_parts if p.startswith('month=')), None)
        day_part = next((p for p in path_parts if p.startswith('day=')), None)
        
        if all([year_part, month_part, day_part]):
            parquet_files = [f for f in files if f.endswith('.parquet') and not f.startswith('.')]
            if parquet_files:
                year = int(year_part.split('=')[1])
                month = int(month_part.split('=')[1])
                day = int(day_part.split('=')[1])
                periods.append((year, month, day))
    
    return sorted(periods)

def process_weather(spark, weather_path):
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType

    weather_df = spark.read \
        .option("basePath", weather_path) \
        .parquet(weather_path)

    # Rename columns
    weather_df = weather_df.withColumnRenamed("lat", "weather_lat") \
                           .withColumnRenamed("lng", "weather_lng")

    geohash_udf = udf(generate_geohash, StringType())
    weather_df = weather_df.withColumn(
        "geohash",
        geohash_udf("weather_lat", "weather_lng")
    )

    return weather_df

def join_data(restaurants_df, weather_df):
    joined_df = restaurants_df.join(
        weather_df,
        restaurants_df.geohash == weather_df.geohash,
        how="left"
    ).drop(weather_df.geohash)
    return joined_df

def save_results(df, output_path):
    df.write \
      .mode("overwrite") \
      .partitionBy("geohash") \
      .parquet(output_path)

def main():
    OPENCAGE_API_KEY = "" # I do not include my API key, since I paid for it, xD
    RESTAURANT_PATH = "restaurant_csv"
    WEATHER_PATH = "Weather"
    OUTPUT_PATH = "enriched_data" 
    spark = create_spark_session()
    
    try:
        # Process restaurants
        restaurants_df = process_restaurants(spark, RESTAURANT_PATH, OPENCAGE_API_KEY)
        
        # Process weather
        weather_df = process_weather(spark, WEATHER_PATH)
        
        # Join data
        result_df = join_data(restaurants_df, weather_df)
        
        # Save results
        save_results(result_df, OUTPUT_PATH)
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
