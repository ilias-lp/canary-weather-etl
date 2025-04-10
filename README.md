# Cloud based setup for historical weather database

In this personal data engineering project, the aim is to set up a cloud based database comprising an extensive set of historical weather metrics for several recent years across multiple locations in Canary Islands. The use case of such database is finding out best spots and periods for different sorts of activities (hiking, surfing, beach, etc.) that depend on weather conditions. Next to setting up a bulk of historical data, monthly periodic updates are a part of the scope for keeping the database content up-to-date.

**Project Outline**

Amazon Web Services (AWS) platform will be used as cloud provider for the whole Extract, Transform and Load (ETL) sequence.

![Cloud ETL pipeline](https://github.com/user-attachments/assets/2b32e185-5ca7-45ef-8694-9f6d033fea8c)



## Data Extraction

Data collection scope is daily historical records of 4+ years from 2021-01-01 until present (March 2025) for various weather stations across each of the seven Canary Islands.

**Stations distribution**

Tenerife (largest and most diverse)

4 stations covering: 
- Northern coast (Puerto de la Cruz) - humid, cloudier 
- Southern coast (Costa Adeje) - sunny, drier 
- Central highlands (Vilaflor) - cooler, mountain conditions 
- Eastern area (Santa Cruz) - urban conditions

Gran Canaria

3 stations covering: 
- Las Palmas (northeast) - urban, coastal 
- Maspalomas (south) - desert-like conditions 
- Tejeda (central mountains) - altitude effects

Lanzarote

2 stations covering: 
- Arrecife (east coast) 
- Mancha Blanca (Timanfaya area with volcanic microclimate)

Fuerteventura

2 stations covering: 
- Corralejo (northern area)
- Costa Calma (southern peninsula)

La Palma

1 station covering: 
- Santa Cruz de La Palma

La Gomera

1 station covering: 
- San Sebastian de La Gomera

El Hierro

1 station covering: 
- Valverde

There are multiple sources that provide API acces to historical weather data. However many of them have strict limitations for their free tiers and/or do not have a large set of metrics that one might need. After the sources research, they were narrowed down to two following APIs:

**Open-meteo Historical Weather API** https://open-meteo.com/en/docs/historical-weather-api

With free 10 000 API calls per day and a broad list of daily weather variables, this source is chosen to be the main one. Below is the full list of daily weather variables available for collection.

Weather code

Maximum Temperature (2 m)

Minimum Temperature (2 m)

Mean Temperature (2 m)

Maximum Apparent Temperature (2 m)

Minimum Apparent Temperature (2 m)

Mean Apparent Temperature (2 m)

Sunrise

Sunset

Daylight Duration

Sunshine Duration

Precipitation Sum

Rain Sum

Snowfall Sum

Precipitation Hours

Maximum Wind Speed (10 m)

Maximum Wind Gusts (10 m)

Dominant Wind Direction (10 m)

Shortwave Radiation Sum

Reference Evapotranspiration (ET₀)

It can be seen that it comprises all essential variables but still misses some other metrics of potential interest such as UV index or percentage of cloudcover. This brings us to the second data source for additional variables.

**Visual Crossing Weather API** https://www.visualcrossing.com/weather-api/
This one has much less generous free tier but is compensated by a reasonably cheap metered subscription type charging 0.0001 USD per API call.
Additional variables collected from here are:

datetime (for reference)

cloudcover

visibility

solarradiation

solarenergy

uvindex

moonphase

conditions

description

icon

The next step is programmatic implementation of API calls from 2 given sources by making use of Lambda function as IDE platform and S3 bucket as the output storage destination.

Below is Lambda function code for Open-meteo API that doesn't require API key and runs smoothly as long as the number of calls stays below 10 000 which implies execution of the code below in 3 steps by commenting out 2/3 of locations for each call, as 365 days * 4+ years * 14 stations = 20 440+ calls required in total.

```python
import json
import boto3
import urllib.request
import urllib.parse
from datetime import datetime
import time

# AWS S3 setup
S3_BUCKET = "canary-weather-raw"
s3_client = boto3.client("s3")

# Dictionary to map locations to names
LOCATIONS = {
    "Tenerife/Puerto_de_la_Cruz": (28.414, -16.5487),
    "Tenerife/Costa_Adeje": (28.1227, -16.726),
    "Tenerife/Vilaflor": (28.1562, -16.6359),
    "Tenerife/Santa_Cruz_de_Tenerife": (28.4682, -16.2546),
    "Gran_Canaria/Las_Palmas_de_Gran_Canaria": (28.0997, -15.4134),
    "Gran_Canaria/Maspalomas": (27.7606, -15.586),
    "Gran_Canaria/Tejeda": (27.9951, -15.6154),
    "Lanzarote/Arrecife": (28.963, -13.5477),
    "Lanzarote/Mancha_Blanca": (29.0431, -13.6891),
    "Fuerteventura/Corralejo": (28.7308, -13.8675),
    "Fuerteventura/Costa_Calma": (28.1615, -14.2269),
    "La_Palma/Santa_Cruz_de_La_Palma": (28.6835, -17.7642),
    "La_Gomera/San_Sebastian_de_La_Gomera": (28.0916, -17.1133),
    "El_Hierro/Valverde": (27.8063, -17.9158)
}

def lambda_handler(event, context):
    today = datetime.today().strftime("%Y-%m-%d")
    year, month, day = today.split("-")
    
    for location_name, (lat, lon) in LOCATIONS.items():
        base_url = "https://archive-api.open-meteo.com/v1/archive"
        
        daily_params = [
            "weather_code", "temperature_2m_max", "temperature_2m_min", "temperature_2m_mean",
            "apparent_temperature_max", "apparent_temperature_min", "apparent_temperature_mean",
            "sunrise", "sunset", "daylight_duration", "sunshine_duration", "precipitation_sum",
            "rain_sum", "snowfall_sum", "precipitation_hours", "wind_speed_10m_max",
            "wind_gusts_10m_max", "wind_direction_10m_dominant", "shortwave_radiation_sum",
            "et0_fao_evapotranspiration"
        ]
        
        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": "2021-01-01",
            "end_date": today,
            "daily": ",".join(daily_params),
            "wind_speed_unit": "ms",
            "timezone": "auto"
        }
        
        query_string = urllib.parse.urlencode(params)
        url = f"{base_url}?{query_string}"
        
        # Add retry logic
        max_retries = 5
        retry_delay = 0.2
        
        for attempt in range(max_retries):
            try:
                with urllib.request.urlopen(url) as response:
                    response_data = json.loads(response.read().decode('utf-8'))
                    break
            except Exception as e:
                if attempt < max_retries - 1:
                    # Exponential backoff
                    time.sleep(retry_delay * (2 ** attempt))
                else:
                    raise Exception(f"Failed to fetch data after {max_retries} attempts: {str(e)}")
        
        # Extract the data
        daily_data = {
            "location": location_name,
            "latitude": lat,
            "longitude": lon,
            "elevation": response_data.get("elevation", 0),
            "timezone": response_data.get("timezone", ""),
            "daily": {}
        }
        
        # Get the time values
        time_values = response_data.get("daily", {}).get("time", [])
        daily_data["daily"]["date"] = time_values
        
        # Extract all daily variables
        for variable_name in daily_params:
            daily_values = response_data.get("daily", {}).get(variable_name, [])
            daily_data["daily"][variable_name] = daily_values
        
        # S3 path with location name, year, month, and day
        s3_key = f"historical_data/{location_name}/2021-01-01-to-{year}-{month}-{day}.json"
        
        # Save to S3
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json.dumps(daily_data),
            ContentType="application/json"
        )
    
    return {
        "statusCode": 200,
        "body": "Weather data successfully saved to S3!"
    }

```

In a similar way, Lambda function code for Visual Crossing API is collecting required data. This time the account based API key for billing is necessary. In order to avoid exposing of such sensitive credentials in the code, AWS Secrets Manager service is being used where API key is securely stored under a specified public name and being accessed through the get_secret function.


```python
import json
import boto3
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timezone
import os
import logging
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Dictionary of locations with their coordinates
LOCATIONS = {
    "Tenerife/Puerto_de_la_Cruz": (28.414, -16.5487),
    "Tenerife/Costa_Adeje": (28.1227, -16.726),
    "Tenerife/Vilaflor": (28.1562, -16.6359),
    "Tenerife/Santa_Cruz_de_Tenerife": (28.4682, -16.2546),
    "Gran_Canaria/Las_Palmas_de_Gran_Canaria": (28.0997, -15.4134),
    "Gran_Canaria/Maspalomas": (27.7606, -15.586),
    "Gran_Canaria/Tejeda": (27.9951, -15.6154),
    "Lanzarote/Arrecife": (28.963, -13.5477),
    "Lanzarote/Mancha_Blanca": (29.0431, -13.6891),
    "Fuerteventura/Corralejo": (28.7308, -13.8675),
    "Fuerteventura/Costa_Calma": (28.1615, -14.2269),
    "La_Palma/Santa_Cruz_de_La_Palma": (28.6835, -17.7642),
    "La_Gomera/San_Sebastian_de_La_Gomera": (28.0916, -17.1133),
    "El_Hierro/Valverde": (27.8063, -17.9158)
}

# S3 bucket details
BUCKET_NAME = "canary-weather-raw"

def get_secret():
    """
    Retrieve API key from AWS Secrets Manager
    """
    secret_name = "VISUAL_CROSSING_API_KEY"
    region_name = "eu-west-3"
    
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        
        # Log the response structure (without revealing the actual secret)
        logger.info(f"Secret response keys: {get_secret_value_response.keys()}")
        
        # Check if SecretString exists in the response
        if 'SecretString' not in get_secret_value_response:
            logger.error("SecretString not found in the response")
            if 'SecretBinary' in get_secret_value_response:
                logger.info("Secret is stored in binary format")
                # Handle binary secret if needed
                return None
            return None
            
        secret_string = get_secret_value_response['SecretString']
        
        # Verify we got a non-empty string
        if not secret_string:
            logger.error("Retrieved an empty secret string")
            return None
            
        logger.info(f"Retrieved secret string type: {type(secret_string)}")
        
        # Handle the case where the secret is stored as JSON
        try:
            # Try to parse as JSON first
            secret_json = json.loads(secret_string)
            logger.info(f"Secret JSON keys: {secret_json.keys() if isinstance(secret_json, dict) else 'Not a dictionary'}")
            
            # The key might be stored under a specific field name like "apiKey" or "key"
            if isinstance(secret_json, dict):
                for key in ["apiKey", "key", "API_KEY", "value", "VISUAL_CROSSING_API_KEY"]:
                    if key in secret_json:
                        api_key = secret_json[key]
                        logger.info(f"Found API key under field: {key}")
                        return api_key
                
                # If none of the expected keys are found but there's only one value
                if len(secret_json) == 1:
                    api_key = next(iter(secret_json.values()))
                    logger.info("Using the only value in the JSON")
                    return api_key
                    
                # Log all keys (without values) for debugging
                logger.error(f"Could not find expected key in JSON. Available keys: {list(secret_json.keys())}")
                
                # Return the entire JSON string
                logger.info("Returning the entire JSON string as a fallback")
                return secret_string
            else:
                logger.info("Secret JSON is not a dictionary, using raw string")
                return secret_string
                
        except json.JSONDecodeError:
            # If it's not JSON, assume the secret string is the API key itself
            logger.info("Secret is not in JSON format, using raw string")
            return secret_string
            
    except ClientError as e:
        logger.error(f"Error retrieving secret: {e}")
        return None

def get_weather_data(latitude, longitude, start_date, end_date, api_key):
    """
    Fetch weather data from Visual Crossing API using urllib
    """
    # Validate API key
    if not api_key:
        error_msg = "API key is None or empty"
        logger.error(error_msg)
        raise ValueError(error_msg)
        
    # Log the API key type and length
    logger.info(f"API key type: {type(api_key)}, length: {len(api_key)}")
    
    # Mask key for logging
    masked_key = api_key[:4] + "*" * (len(api_key) - 8) + api_key[-4:] if len(api_key) > 8 else "****"
    logger.info(f"Using API key: {masked_key}")
    
    # Build the base URL
    base_url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{latitude},{longitude}/{start_date}/{end_date}"
    
    # Build query parameters
    params = {
        "key": api_key,
        "include": "days",
        "elements": "datetime,cloudcover,visibility,solarradiation,solarenergy,uvindex,moonphase,conditions,description,icon"
    }
    
    query_string = urllib.parse.urlencode(params)
    url = f"{base_url}?{query_string}"
    
    # Log the URL (with masked API key) for debugging
    masked_url = url.replace(api_key, masked_key)
    logger.info(f"Making request to: {masked_url}")
    
    try:
        # Create a request with headers
        req = urllib.request.Request(
            url,
            headers={
                'User-Agent': 'Mozilla/5.0 (Compatible with Lambda)',
                'Accept': 'application/json'
            }
        )
        
        with urllib.request.urlopen(req) as response:
            data = response.read().decode('utf-8')
            # Log a snippet of the response
            logger.info(f"Response received, first 100 chars: {data[:100]}")
            return json.loads(data)
    except urllib.error.HTTPError as e:
        # Add more detailed error info
        error_message = f"HTTP Error {e.code}: {e.reason}"
        if hasattr(e, 'read'):
            error_content = e.read().decode('utf-8')
            error_message += f" - Response: {error_content}"
        logger.error(error_message)
        raise Exception(error_message)
    except urllib.error.URLError as e:
        logger.error(f"URL Error: {e.reason}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise

def save_to_s3(data, bucket_name, s3_key):
    """
    Save data to S3 bucket
    """
    # Validate inputs
    if data is None:
        error_msg = "Cannot save None data to S3"
        logger.error(error_msg)
        raise ValueError(error_msg)
        
    if not bucket_name:
        error_msg = "S3 bucket name is None or empty"
        logger.error(error_msg)
        raise ValueError(error_msg)
        
    if not s3_key:
        error_msg = "S3 key is None or empty"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    # Convert data to JSON string
    try:
        json_data = json.dumps(data, indent=2)
        logger.info(f"Successfully converted data to JSON. Size: {len(json_data)} bytes")
    except Exception as e:
        logger.error(f"Error converting data to JSON: {str(e)}")
        raise ValueError(f"Data cannot be converted to JSON: {str(e)}")
    
    # Save to S3
    s3_client = boto3.client('s3')
    try:
        response = s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json_data,
            ContentType='application/json'
        )
        logger.info(f"Successfully saved data to s3://{bucket_name}/{s3_key}")
        return response
    except Exception as e:
        logger.error(f"Error saving to S3: {str(e)}")
        raise

def lambda_handler(event, context):
    """
    AWS Lambda handler function
    """
    # Get API key from Secrets Manager
    try:
        api_key = get_secret()
    except Exception as e:
        logger.error(f"Failed to retrieve API key: {e}")
        return {
            "statusCode": 500,
            "body": {"error": "Failed to retrieve API key"}
        }
    
    # Set the start date (fixed as per requirement)
    start_date = "2021-01-01"
    # Set end date to today
    today = datetime.now(timezone.utc)
    end_date = today.strftime("%Y-%m-%d")
    
    # Extract components for S3 key
    year = today.strftime("%Y")
    month = today.strftime("%m")
    day = today.strftime("%d")
    
    results = {}
    
    for location_name, coordinates in LOCATIONS.items():
        latitude, longitude = coordinates
        
        try:
            logger.info(f"Fetching data for {location_name}")
            weather_data = get_weather_data(latitude, longitude, start_date, end_date, api_key)
            
            # Define the S3 key for this location
            s3_key = f"historical_data/{location_name}/vc{start_date}-to-{year}-{month}-{day}.json"
            
            # Save data to S3
            save_to_s3(weather_data, BUCKET_NAME, s3_key)
            
            results[location_name] = {
                "status": "success",
                "s3_key": s3_key
            }
            
        except Exception as e:
            logger.error(f"Failed to process {location_name}: {e}")
            results[location_name] = {
                "status": "error",
                "message": str(e)
            }
    
    return {
        "statusCode": 200,
        "body": results
    }

```

This concludes the historical raw data collection that is now stored in S3 and sorted in folders with location names.

![s3_1](https://github.com/user-attachments/assets/abc6b59d-6ced-430a-8e73-ce7bec1a2b32)

Each location folder contains 2 JSON files corresponding to 2 data sources of origin. In order to differentiate, files from Visual Crossing got assigned 'vc' prefix in their names.

![s3_2](https://github.com/user-attachments/assets/55c20375-c824-4e3c-894d-9a7ca2d1ab4f)

## Data Transformation

Before merging JSON files together it's important to point out that despite same file format, they have different internal structure.

Files from Open-meteo API have array structure that goes as follows:

```
{"location": "Tenerife/Vilaflor", "latitude": 28.1562, "longitude": -16.6359, "elevation": 1387.0, "timezone": "Atlantic/Canary", "daily": {"date": ["2021-01-01", "2021-01-02", "2021-01-03", "2021-01-04", "2021-01-05", "2021-01-06", "2021-01-07", "2021-01-08", "2021-01-09", "2021-01-10", "2021-01-11", "2021-01-12", "2021-01-13", ..... ]

"weather_code": [51, 51, 51, 63, 3, 63, 63, 63, 63, 51, 53, 53, 3, 3, 2, 2, 3, 51, 2, 2, 3, 51, 3, 3, 2, 1, 0, 0, 0, 0, 0, 3, 3, 2, 53, 63, 51, 51, 3, 51, 51, 3, 53, 0, 0, 2, 0, 3, 3, 1, 63, 51, 51, 3, 51, 2, 2, 55, 55, 51, 53, 51, 0, 61, 51, 51, 53, 53, 1, 0, 3, 3, 3, 3, ...... ]
```

Whereas files from Visual Crossing API have nested structure:

```
{
  "queryCost": 1530,
  "latitude": 28.1562,
  "longitude": -16.6359,
  "resolvedAddress": "28.1562,-16.6359",
  "address": "28.1562,-16.6359",
  "timezone": "Atlantic/Canary",
  "tzoffset": 0.0,
  "days": [
    {
      "datetime": "2021-01-01",
      "cloudcover": 60.8,
      "visibility": 10.9,
      "solarradiation": 76.3,
      "solarenergy": 6.5,
      "uvindex": 4.0,
      "moonphase": 0.58,
      "conditions": "Rain, Partially cloudy",
      "description": "Partly cloudy throughout the day with late afternoon rain.",
      "icon": "rain"
    },
    {
      "datetime": "2021-01-02",
      "cloudcover": 52.8,
      "visibility": 9.9,
      "solarradiation": 82.5,
      "solarenergy": 7.1,
      "uvindex": 3.0,
      "moonphase": 0.62,
      "conditions": "Rain, Partially cloudy",
      "description": "Partly cloudy throughout the day with early morning rain.",
      "icon": "rain"
    },
```
Therefore JSON files have to be processed differently depending on their internal structure before being merged.

AWS Glue script below makes use of PySpark library for data handling and transformation. It iterates separately through 2 sets of JSON files depending on their name prefix, flattens the data out and combines it into one columnar database in Parquet format. The output Parquet files are also stored in an S3 bucket which eliminates need for a relational database or a complex data warehouse system but still can be effectively queried and analyzed with SQL which makes it effective and cheap solution. Since Athena billing costs are calculated based on the volume of scanned data, the dataset is partitioned by **island**, **location_name**, **year** and **month** in order to reduce the amount of scanned data for queries that specify any of those key parameters.


```python
import sys
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Initialize Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define S3 paths
raw_bucket = "s3://canary-weather-raw/historical_data/"
output_bucket = "s3://canary-weather-clean/historical_data/"

# List all islands and locations
islands_locations = spark.read.format("json").option("inferSchema", "true").option("multiLine", "true").load(raw_bucket + "*/*/*.json")
islands_locations = islands_locations.select(F.regexp_extract(F.input_file_name(), "historical_data/(.*?)/(.*?)/", 1).alias("island"),
                                           F.regexp_extract(F.input_file_name(), "historical_data/(.*?)/(.*?)/", 2).alias("location")).distinct()

# Process each island and location
islands_locations_list = islands_locations.collect()

for row in islands_locations_list:
    island = row["island"]
    location = row["location"]
    
    print(f"Processing data for {island}/{location}")
    
    # Path to current location data
    location_path = f"{raw_bucket}{island}/{location}/"
    
    # Read VC JSON file
    vc_file_path = f"{location_path}vc*.json"
    try:
        vc_df = spark.read.format("json").option("inferSchema", "true").option("multiLine", "true").load(vc_file_path)
        
        # Extract nested data from days array
        vc_df = vc_df.select(
            F.lit(f"{island}/{location}").alias("location"),
            F.col("latitude"),
            F.col("longitude"),
            F.col("timezone"),
            F.explode("days").alias("day_data")
        )
        
        # Flatten the day_data structure
        vc_df = vc_df.select(
            F.col("location"),
            F.col("latitude"),
            F.col("longitude"),
            F.col("day_data.datetime").alias("date"),
            F.col("day_data.cloudcover").alias("cloudcover"),
            F.col("day_data.visibility").alias("visibility"),
            F.col("day_data.solarradiation").alias("solarradiation"),
            F.col("day_data.solarenergy").alias("solarenergy"),
            F.col("day_data.uvindex").alias("uvindex"),
            F.col("day_data.moonphase").alias("moonphase"),
            F.col("day_data.conditions").alias("conditions"),
            F.col("day_data.description").alias("description"),
            F.col("day_data.icon").alias("icon")
        )
    except Exception as e:
        print(f"Error processing VC file for {island}/{location}: {str(e)}")
        vc_df = None
    
    # Read date-prefixed JSON file
    date_file_path = f"{location_path}2*.json"
    try:
        date_df = spark.read.format("json").option("inferSchema", "true").option("multiLine", "true").load(date_file_path)
        
        # Get all the daily fields
        daily_fields = date_df.select("daily.*").columns
        
        # Create a base dataframe with the common fields
        base_df = date_df.select(
            F.lit(f"{island}/{location}").alias("location"),
            F.col("latitude"),
            F.col("longitude"),
            F.col("elevation"),
            F.col("timezone")
        )
        
        # Create a dataframe with exploded date column for joining
        dates_df = base_df.crossJoin(
            date_df.select(F.explode(F.col("daily.date")).alias("date"))
        )
        
        # Get the array lengths to verify all arrays have the same length
        array_lengths = {}
        for field in daily_fields:
            length_df = date_df.select(F.size(F.col(f"daily.{field}")).alias("length"))
            array_lengths[field] = length_df.collect()[0]["length"]
        
        # Check if all arrays have the same length
        array_length = next(iter(array_lengths.values()))
        all_same_length = all(length == array_length for length in array_lengths.values())
        
        if not all_same_length:
            raise ValueError("Arrays in daily fields have different lengths, cannot process reliably")
        
        # Create a proper dataset with one row per date, containing all data
        daily_data_rows = []
        
        # Extract all array data
        arrays_data = {}
        for field in daily_fields:
            field_data = date_df.select(F.col(f"daily.{field}")).collect()[0][0]
            arrays_data[field] = field_data
        
        # Create a dataframe with all fields properly aligned
        data_rows = []
        for i in range(array_length):
            row = {"date": arrays_data["date"][i]}
            for field in daily_fields:
                if field != "date":  # We already have date
                    row[field] = arrays_data[field][i] if i < len(arrays_data[field]) else None
            data_rows.append(row)
        
        # Create dataframe from the constructed rows
        daily_df = spark.createDataFrame(data_rows)
        
        # Add the common fields
        daily_df = daily_df.crossJoin(base_df.drop("date").limit(1))
        
    except Exception as e:
        print(f"Error processing date file for {island}/{location}: {str(e)}")
        daily_df = None
    
    # Combine the two datasets if both exist
    if vc_df is not None and daily_df is not None:
        # Join on location and date
        combined_df = vc_df.join(daily_df, ["location", "date", "latitude", "longitude"], "inner")
        
        # Extract year and month from date for partitioning
        combined_df = combined_df.withColumn("year", F.year(F.to_date(F.col("date"), "yyyy-MM-dd")))
        combined_df = combined_df.withColumn("month", F.month(F.to_date(F.col("date"), "yyyy-MM-dd")))
        
        # Create island and location columns for partitioning
        combined_df = combined_df.withColumn("island", F.lit(island))
        combined_df = combined_df.withColumn("location_name", F.lit(location))
        
        # Write to Parquet, partitioned by island, location, year, and month
        output_path = f"{output_bucket}"
        
        combined_df.write.mode("append") \
            .partitionBy("island", "location_name", "year", "month") \
            .parquet(output_path)
        
        print(f"Successfully processed and saved data for {island}/{location}")
    else:
        print(f"Skipping {island}/{location} as one or both source files are missing or invalid")

# Job completed
job.commit()
```

## Glue Data Catalog Setup

After the Glue job is completed, Parquet files are stored in a designated S3 bucket following specified partition structure.

![s3_3](https://github.com/user-attachments/assets/dd96850e-cfe8-4865-a7ca-184e0670bbf5)

In order to create a new catalogue table, a Glue crawler was created for the given S3 location that has automatically detected schema and partitions.

![crawler](https://github.com/user-attachments/assets/4fe42593-43fd-4116-96dc-2737597a119e)

![schema](https://github.com/user-attachments/assets/e6d707dd-50dd-452e-a220-454217f84f20)

## Ad-hoc Querying

Amazon Athena provides access to S3 objects by running conventional SQL queries on them. There are numerous ways to group and analyze obtained data that contains dozens of parameters. For example the query for finding out the sunniest location would be as follows:

```sql
SELECT location, ROUND((AVG(sunshine_duration)/3600), 2) AS avg_sunshine_hours
FROM "AwsDataCatalog"."canary-weather"."canary_weather_clean"
GROUP BY location
ORDER BY avg_sunshine_hours DESC;
```
where daily sunshine duration (given in seconds) gets converted to hours and its global average is calculated for each location.

![athena_1](https://github.com/user-attachments/assets/4b01b746-a494-40de-ad11-1913550a35a5)

The results rank all 14 locations from highest to lowest daily average sunshine duration. However it's a very general metric that doesn't illustrate changes by time of year. In order to find the sunniest months and their corresponding locations, the following query can be run:

```sql
SELECT location, month, ROUND((AVG(sunshine_duration)/3600), 2) AS avg_sunshine_hours
FROM "AwsDataCatalog"."canary-weather"."canary_weather_clean"
GROUP BY location, month
ORDER BY avg_sunshine_hours DESC
LIMIT 10;
```
![athena_2](https://github.com/user-attachments/assets/266634db-c405-4d0b-b174-c1c54d75db31)

This provides more insightful view such that June and July are the months wiht longest sunshine duration across the islands.

Another practical example is checking for average UV index number in a specific location for each month. Besides that, using a partitioned column in WHERE condition narrows down the amount of scanned data by factor 14 in our case, since data is evenly distributed among 14 locations.

```sql
SELECT month, ROUND((AVG(uvindex), 1) AS avg_uvindex
FROM "AwsDataCatalog"."canary-weather"."canary_weather_clean"
WHERE location_name = 'Las_Palmas_de_Gran_Canaria'
GROUP BY month
ORDER BY avg_uvindex DESC;
```
![athena_3](https://github.com/user-attachments/assets/9a37619c-4c67-4fdd-b844-7da0c7626c9e)

## Periodic Update

Periodic updates are necessary for maintaining the database's relevance. In order to minimize costs that are majorly charged per job run on AWS, a monthly update will be considered for implementation. It will comprise scheduled running of two Lambda functions (for each data source), Glue job triggered by successful completion of Lambda functions and triggering of Glue crawler for data catalog update by completion of the Glue job.

![Automated Database Update](https://github.com/user-attachments/assets/b93e17ca-cc14-4c69-b18e-ccd89051d3b4)

Collecting data for the past month only requires some changes in Lambda code as for extraction period and altering S3 bucket key where a specifically designated location for updates will be used. Taken into account non-critical nature of monthly raw data, the update folder content will be scheduled for deletion a few days after creation, through S3 Lifecycle Rules for having a clean /updates path for the next monthly update.

```python
import json
import boto3
import urllib.request
import urllib.parse
from datetime import datetime, timedelta
import time

# AWS S3 setup
S3_BUCKET = "canary-weather-raw"
s3_client = boto3.client("s3")

# Dictionary to map locations to names
LOCATIONS = {
    "Tenerife/Puerto_de_la_Cruz": (28.414, -16.5487),
    "Tenerife/Costa_Adeje": (28.1227, -16.726),
    "Tenerife/Vilaflor": (28.1562, -16.6359),
    "Tenerife/Santa_Cruz_de_Tenerife": (28.4682, -16.2546),
    "Gran_Canaria/Las_Palmas_de_Gran_Canaria": (28.0997, -15.4134),
    "Gran_Canaria/Maspalomas": (27.7606, -15.586),
    "Gran_Canaria/Tejeda": (27.9951, -15.6154),
    "Lanzarote/Arrecife": (28.963, -13.5477),
    "Lanzarote/Mancha_Blanca": (29.0431, -13.6891),
    "Fuerteventura/Corralejo": (28.7308, -13.8675),
    "Fuerteventura/Costa_Calma": (28.1615, -14.2269),
    "La_Palma/Santa_Cruz_de_La_Palma": (28.6835, -17.7642),
    "La_Gomera/San_Sebastian_de_La_Gomera": (28.0916, -17.1133),
    "El_Hierro/Valverde": (27.8063, -17.9158)
}

def lambda_handler(event, context):
    today = datetime.today().strftime("%Y-%m-%d")
    year, month, day = today.split("-")
    
    for location_name, (lat, lon) in LOCATIONS.items():
        base_url = "https://archive-api.open-meteo.com/v1/archive"
        
        daily_params = [
            "weather_code", "temperature_2m_max", "temperature_2m_min", "temperature_2m_mean",
            "apparent_temperature_max", "apparent_temperature_min", "apparent_temperature_mean",
            "sunrise", "sunset", "daylight_duration", "sunshine_duration", "precipitation_sum",
            "rain_sum", "snowfall_sum", "precipitation_hours", "wind_speed_10m_max",
            "wind_gusts_10m_max", "wind_direction_10m_dominant", "shortwave_radiation_sum",
            "et0_fao_evapotranspiration"
        ]
        
        today_dt = datetime.strptime(today, "%Y-%m-%d")
        start_date_dt = today_dt - timedelta(days=31)
        start_date = start_date_dt.strftime("%Y-%m-%d")

        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": start_date,
            "end_date": today,
            "daily": ",".join(daily_params),
            "wind_speed_unit": "ms",
            "timezone": "auto"
        }
        
        query_string = urllib.parse.urlencode(params)
        url = f"{base_url}?{query_string}"
        
        # Add retry logic
        max_retries = 5
        retry_delay = 0.2
        
        for attempt in range(max_retries):
            try:
                with urllib.request.urlopen(url) as response:
                    response_data = json.loads(response.read().decode('utf-8'))
                    break
            except Exception as e:
                if attempt < max_retries - 1:
                    # Exponential backoff
                    time.sleep(retry_delay * (2 ** attempt))
                else:
                    raise Exception(f"Failed to fetch data after {max_retries} attempts: {str(e)}")
        
        # Extract the data
        daily_data = {
            "location": location_name,
            "latitude": lat,
            "longitude": lon,
            "elevation": response_data.get("elevation", 0),
            "timezone": response_data.get("timezone", ""),
            "daily": {}
        }
        
        # Get the time values
        time_values = response_data.get("daily", {}).get("time", [])
        daily_data["daily"]["date"] = time_values
        
        # Extract all daily variables
        for variable_name in daily_params:
            daily_values = response_data.get("daily", {}).get(variable_name, [])
            daily_data["daily"][variable_name] = daily_values
        
        # S3 path with location name, year, month, and day
        s3_key = f"updates/{location_name}/{start_date}-to-{year}-{month}-{day}.json"
        
        # Save to S3
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json.dumps(daily_data),
            ContentType="application/json"
        )
    
    return {
        "statusCode": 200,
        "body": "Weather data successfully saved to S3!"
    }
```

```python
import json
import boto3
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timezone, timedelta
import os
import logging
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Dictionary of locations with their coordinates
LOCATIONS = {
    "Tenerife/Puerto_de_la_Cruz": (28.414, -16.5487),
    "Tenerife/Costa_Adeje": (28.1227, -16.726),
    "Tenerife/Vilaflor": (28.1562, -16.6359),
    "Tenerife/Santa_Cruz_de_Tenerife": (28.4682, -16.2546),
    "Gran_Canaria/Las_Palmas_de_Gran_Canaria": (28.0997, -15.4134),
    "Gran_Canaria/Maspalomas": (27.7606, -15.586),
    "Gran_Canaria/Tejeda": (27.9951, -15.6154),
    "Lanzarote/Arrecife": (28.963, -13.5477),
    "Lanzarote/Mancha_Blanca": (29.0431, -13.6891),
    "Fuerteventura/Corralejo": (28.7308, -13.8675),
    "Fuerteventura/Costa_Calma": (28.1615, -14.2269),
    "La_Palma/Santa_Cruz_de_La_Palma": (28.6835, -17.7642),
    "La_Gomera/San_Sebastian_de_La_Gomera": (28.0916, -17.1133),
    "El_Hierro/Valverde": (27.8063, -17.9158)
}

# S3 bucket details
BUCKET_NAME = "canary-weather-raw"

def get_secret():
    """
    Retrieve API key from AWS Secrets Manager
    """
    secret_name = "VISUAL_CROSSING_API_KEY"
    region_name = "eu-west-3"
    
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        
        # Log the response structure (without revealing the actual secret)
        logger.info(f"Secret response keys: {get_secret_value_response.keys()}")
        
        # Check if SecretString exists in the response
        if 'SecretString' not in get_secret_value_response:
            logger.error("SecretString not found in the response")
            if 'SecretBinary' in get_secret_value_response:
                logger.info("Secret is stored in binary format")
                # Handle binary secret if needed
                return None
            return None
            
        secret_string = get_secret_value_response['SecretString']
        
        # Verify we got a non-empty string
        if not secret_string:
            logger.error("Retrieved an empty secret string")
            return None
            
        logger.info(f"Retrieved secret string type: {type(secret_string)}")
        
        # Handle the case where the secret is stored as JSON
        try:
            # Try to parse as JSON first
            secret_json = json.loads(secret_string)
            logger.info(f"Secret JSON keys: {secret_json.keys() if isinstance(secret_json, dict) else 'Not a dictionary'}")
            
            # The key might be stored under a specific field name like "apiKey" or "key"
            if isinstance(secret_json, dict):
                for key in ["apiKey", "key", "API_KEY", "value", "VISUAL_CROSSING_API_KEY"]:
                    if key in secret_json:
                        api_key = secret_json[key]
                        logger.info(f"Found API key under field: {key}")
                        return api_key
                
                # If none of the expected keys are found but there's only one value, use that
                if len(secret_json) == 1:
                    api_key = next(iter(secret_json.values()))
                    logger.info("Using the only value in the JSON")
                    return api_key
                    
                # Log all keys (without values) for debugging
                logger.error(f"Could not find expected key in JSON. Available keys: {list(secret_json.keys())}")
                
                # Return the entire JSON string
                logger.info("Returning the entire JSON string as a fallback")
                return secret_string
            else:
                logger.info("Secret JSON is not a dictionary, using raw string")
                return secret_string
                
        except json.JSONDecodeError:
            # If it's not JSON, assume the secret string is the API key itself
            logger.info("Secret is not in JSON format, using raw string")
            return secret_string
            
    except ClientError as e:
        logger.error(f"Error retrieving secret: {e}")
        return None

def get_weather_data(latitude, longitude, start_date, end_date, api_key):
    """
    Fetch weather data from Visual Crossing API using urllib
    """
    # Validate API key
    if not api_key:
        error_msg = "API key is None or empty"
        logger.error(error_msg)
        raise ValueError(error_msg)
        
    # Log the API key type and length
    logger.info(f"API key type: {type(api_key)}, length: {len(api_key)}")
    
    # Mask key for logging
    masked_key = api_key[:4] + "*" * (len(api_key) - 8) + api_key[-4:] if len(api_key) > 8 else "****"
    logger.info(f"Using API key: {masked_key}")
    
    # Build the base URL
    base_url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{latitude},{longitude}/{start_date}/{end_date}"
    
    # Build query parameters
    params = {
        "key": api_key,
        "include": "days",
        "elements": "datetime,cloudcover,visibility,solarradiation,solarenergy,uvindex,moonphase,conditions,description,icon"
    }
    
    query_string = urllib.parse.urlencode(params)
    url = f"{base_url}?{query_string}"
    
    # Log the URL (with masked API key) for debugging
    masked_url = url.replace(api_key, masked_key)
    logger.info(f"Making request to: {masked_url}")
    
    try:
        # Create a request with headers
        req = urllib.request.Request(
            url,
            headers={
                'User-Agent': 'Mozilla/5.0 (Compatible with Lambda)',
                'Accept': 'application/json'
            }
        )
        
        with urllib.request.urlopen(req) as response:
            data = response.read().decode('utf-8')
            # Log a snippet of the response
            logger.info(f"Response received, first 100 chars: {data[:100]}")
            return json.loads(data)
    except urllib.error.HTTPError as e:
        # Add more detailed error info
        error_message = f"HTTP Error {e.code}: {e.reason}"
        if hasattr(e, 'read'):
            error_content = e.read().decode('utf-8')
            error_message += f" - Response: {error_content}"
        logger.error(error_message)
        raise Exception(error_message)
    except urllib.error.URLError as e:
        logger.error(f"URL Error: {e.reason}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise

def save_to_s3(data, bucket_name, s3_key):
    """
    Save data to S3 bucket
    """
    # Validate inputs
    if data is None:
        error_msg = "Cannot save None data to S3"
        logger.error(error_msg)
        raise ValueError(error_msg)
        
    if not bucket_name:
        error_msg = "S3 bucket name is None or empty"
        logger.error(error_msg)
        raise ValueError(error_msg)
        
    if not s3_key:
        error_msg = "S3 key is None or empty"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    # Convert data to JSON string
    try:
        json_data = json.dumps(data, indent=2)
        logger.info(f"Successfully converted data to JSON. Size: {len(json_data)} bytes")
    except Exception as e:
        logger.error(f"Error converting data to JSON: {str(e)}")
        raise ValueError(f"Data cannot be converted to JSON: {str(e)}")
    
    # Save to S3
    s3_client = boto3.client('s3')
    try:
        response = s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json_data,
            ContentType='application/json'
        )
        logger.info(f"Successfully saved data to s3://{bucket_name}/{s3_key}")
        return response
    except Exception as e:
        logger.error(f"Error saving to S3: {str(e)}")
        raise

def lambda_handler(event, context):
    """
    AWS Lambda handler function
    """
    # Get API key from Secrets Manager
    try:
        api_key = get_secret()
    except Exception as e:
        logger.error(f"Failed to retrieve API key: {e}")
        return {
            "statusCode": 500,
            "body": {"error": "Failed to retrieve API key"}
        }
    
    # Set the start date (fixed as per requirement)
    today = datetime.today().strftime("%Y-%m-%d")
    today_dt = datetime.strptime(today, "%Y-%m-%d")
    start_date_dt = today_dt - timedelta(days=31)
    start_date = start_date_dt.strftime("%Y-%m-%d")
    # Set end date to today
    today = datetime.now(timezone.utc)
    end_date = today.strftime("%Y-%m-%d")
    
    # Extract components for S3 key
    year = today.strftime("%Y")
    month = today.strftime("%m")
    day = today.strftime("%d")
    
    results = {}
    
    for location_name, coordinates in LOCATIONS.items():
        latitude, longitude = coordinates
        
        try:
            logger.info(f"Fetching data for {location_name}")
            weather_data = get_weather_data(latitude, longitude, start_date, end_date, api_key)
            
            # Define the S3 key for this location
            s3_key = f"updates/{location_name}/vc{start_date}-to-{year}-{month}-{day}.json"
            
            # Save data to S3
            save_to_s3(weather_data, BUCKET_NAME, s3_key)
            
            results[location_name] = {
                "status": "success",
                "s3_key": s3_key
            }
            
        except Exception as e:
            logger.error(f"Failed to process {location_name}: {e}")
            results[location_name] = {
                "status": "error",
                "message": str(e)
            }
    
    return {
        "statusCode": 200,
        "body": results
    }

```

The completion of both Lambda functions triggers the Glue transformation script via another EventBridge rule that appends new rows to existing dataset. 

```python
import sys
import json
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Initialize Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define S3 paths - directly use a fixed source path
raw_bucket = "s3://canary-weather-raw/updates/"
output_bucket = "s3://canary-weather-clean/historical_data/"

print(f"Using raw data path: {raw_bucket}")

# Check if output table exists and get its schema
def ensure_output_table_exists():
    try:
        # Try to read a small sample to see if data exists and get schema
        output_df = spark.read.parquet(output_bucket).limit(1)
        output_df.count()
        print("Output table exists")
        
        # Check data types of columns
        columns_info = [(field.name, field.dataType) for field in output_df.schema.fields]
        print("Existing schema:", columns_info)
        
        # Get column names to check for duplicates
        existing_columns = [field.name for field in output_df.schema.fields]
        if len(existing_columns) != len(set(existing_columns)):
            print("WARNING: Existing table has duplicate column names!")
            
        return True, output_df.schema
    except Exception as e:
        print(f"Output table does not exist: {str(e)}")
        print("Creating a minimal table structure for inserts")
        
        # Create a minimal dataframe with just a few key columns
        minimal_schema = StructType([
            StructField("location", StringType(), False),
            StructField("date", DateType(), False),  # Use DateType to ensure consistent type
            StructField("year", IntegerType(), False),
            StructField("month", IntegerType(), False),
            StructField("island", StringType(), False),
            StructField("location_name", StringType(), False)
        ])
        
        empty_df = spark.createDataFrame([], minimal_schema)
        empty_df.write.partitionBy("island", "location_name", "year", "month").parquet(output_bucket)
        print("Created initial table structure")
        return True, minimal_schema

# Register the output location as a temporary table for overwrites
table_exists, existing_schema = ensure_output_table_exists()
spark.read.parquet(output_bucket).createOrReplaceTempView("weather_data")

# List all islands and locations in the raw bucket
try:
    islands_locations = spark.read.format("json").option("inferSchema", "true").option("multiLine", "true").load(raw_bucket + "*/*/*.json")
    # Extract island and location from file path
    islands_locations = islands_locations.withColumn("file_path", F.input_file_name())
    
    # Extract island and location components from the file path
    islands_locations = islands_locations.withColumn(
        "island", 
        F.expr("regexp_extract(file_path, '/([^/]+)/[^/]+/[^/]+\\.json$', 1)")
    ).withColumn(
        "location", 
        F.expr("regexp_extract(file_path, '/[^/]+/([^/]+)/[^/]+\\.json$', 1)")
    )
    
    islands_locations = islands_locations.select("island", "location").distinct()
    
    # Filter out any empty values
    islands_locations = islands_locations.filter(
        (F.col("island").isNotNull()) & 
        (F.length(F.col("island")) > 0) &
        (F.col("location").isNotNull()) & 
        (F.length(F.col("location")) > 0)
    )
except Exception as e:
    print(f"Error identifying islands/locations: {str(e)}")
    islands_locations = spark.createDataFrame([], StructType([StructField("island", StringType(), False), StructField("location", StringType(), False)]))

# Process each island and location
islands_locations_list = islands_locations.collect()
print(f"Found {len(islands_locations_list)} island/location combinations to process")

for row in islands_locations_list:
    island = row["island"]
    location = row["location"]
    
    print(f"Processing data for {island}/{location}")
    
    # Path to current location data
    location_path = f"{raw_bucket}{island}/{location}/"
    
    # Read VC JSON file
    vc_file_path = f"{location_path}vc*.json"
    try:
        vc_df = spark.read.format("json").option("inferSchema", "true").option("multiLine", "true").load(vc_file_path)
        
        # Extract nested data from days array
        vc_df = vc_df.select(
            F.lit(f"{island}/{location}").alias("location"),
            F.col("latitude"),
            F.col("longitude"),
            F.col("timezone"),
            F.explode("days").alias("day_data")
        )
        
        # Flatten the day_data structure
        vc_df = vc_df.select(
            F.col("location"),
            F.col("latitude"),
            F.col("longitude"),
            F.col("day_data.datetime").alias("date"),
            F.col("day_data.cloudcover").alias("cloudcover"),
            F.col("day_data.visibility").alias("visibility"),
            F.col("day_data.solarradiation").alias("solarradiation"),
            F.col("day_data.solarenergy").alias("solarenergy"),
            F.col("day_data.uvindex").alias("uvindex"),
            F.col("day_data.moonphase").alias("moonphase"),
            F.col("day_data.conditions").alias("conditions"),
            F.col("day_data.description").alias("description"),
            F.col("day_data.icon").alias("icon")
        )
        
        # Convert date string to DateType to maintain consistency
        vc_df = vc_df.withColumn("date", F.to_date(F.col("date"), "yyyy-MM-dd"))
        
    except Exception as e:
        print(f"Error processing VC file for {island}/{location}: {str(e)}")
        vc_df = None
    
    # Read date-prefixed JSON file
    date_file_path = f"{location_path}2*.json"
    try:
        date_df = spark.read.format("json").option("inferSchema", "true").option("multiLine", "true").load(date_file_path)
        
        # Get all the daily fields
        daily_fields = date_df.select("daily.*").columns
        
        # Create a base dataframe with the common fields
        base_df = date_df.select(
            F.lit(f"{island}/{location}").alias("location"),
            F.col("latitude"),
            F.col("longitude"),
            F.col("elevation"),
            F.col("timezone")
        )
        
        # Get the array lengths to verify all arrays have the same length
        array_lengths = {}
        for field in daily_fields:
            length_df = date_df.select(F.size(F.col(f"daily.{field}")).alias("length"))
            array_lengths[field] = length_df.collect()[0]["length"]
        
        # Check if all arrays have the same length
        array_length = next(iter(array_lengths.values()))
        all_same_length = all(length == array_length for length in array_lengths.values())
        
        if not all_same_length:
            raise ValueError("Arrays in daily fields have different lengths, cannot process reliably")
        
        # Extract all array data
        arrays_data = {}
        for field in daily_fields:
            field_data = date_df.select(F.col(f"daily.{field}")).collect()[0][0]
            arrays_data[field] = field_data
        
        # Create a dataframe with all fields properly aligned
        data_rows = []
        for i in range(array_length):
            row = {"date": arrays_data["date"][i]}
            for field in daily_fields:
                if field != "date":  # We already have date
                    row[field] = arrays_data[field][i] if i < len(arrays_data[field]) else None
            data_rows.append(row)
        
        # Create dataframe from the constructed rows
        daily_df = spark.createDataFrame(data_rows)
        
        # Add the common fields
        daily_df = daily_df.crossJoin(base_df.drop("date").limit(1))
        
        # Convert date string to DateType to maintain consistency
        daily_df = daily_df.withColumn("date", F.to_date(F.col("date"), "yyyy-MM-dd"))
        
    except Exception as e:
        print(f"Error processing date file for {island}/{location}: {str(e)}")
        daily_df = None
    
    # Combine the two datasets if both exist
    if vc_df is not None and daily_df is not None:
        try:
            # Avoid duplicate columns during join
            # Get a list of columns in both dataframes
            vc_columns = set(vc_df.columns)
            daily_columns = set(daily_df.columns)
            
            # Identify common columns (beyond the join keys)
            common_columns = vc_columns.intersection(daily_columns) - {"location", "date", "latitude", "longitude"}
            
            if common_columns:
                print(f"Found overlapping columns: {common_columns}")
                # Rename duplicate columns in daily_df
                for col in common_columns:
                    daily_df = daily_df.withColumnRenamed(col, f"{col}_daily")
            
            # Join on location and date
            combined_df = vc_df.join(daily_df, ["location", "date", "latitude", "longitude"], "inner")
            
            # Extract year and month from date for partitioning
            combined_df = combined_df.withColumn("year", F.year(F.col("date")))
            combined_df = combined_df.withColumn("month", F.month(F.col("date")))
            
            # Create island and location columns for partitioning
            combined_df = combined_df.withColumn("island", F.lit(island))
            combined_df = combined_df.withColumn("location_name", F.lit(location))
            
            # Check for and fix any duplicate column names
            column_names = combined_df.columns
            if len(column_names) != len(set(column_names)):
                print("WARNING: Found duplicate column names in the combined dataframe")
                # Find duplicates
                from collections import Counter
                duplicates = [item for item, count in Counter(column_names).items() if count > 1]
                print(f"Duplicate columns: {duplicates}")

                # Create a new dataframe with unique column names
                unique_columns = []
                column_counter = {}
                
                for col in column_names:
                    if col in column_counter:
                        column_counter[col] += 1
                        unique_columns.append(f"{col}_{column_counter[col]}")
                    else:
                        column_counter[col] = 0
                        unique_columns.append(col)
                
                # Select with renamed columns
                select_expr = [F.col(original).alias(new) for original, new in zip(column_names, unique_columns)]
                combined_df = combined_df.select(*select_expr)
            
            # Get a list of distinct dates
            update_dates = combined_df.select("date").distinct().collect()
            dates_list = [row["date"] for row in update_dates]
            
            print(f"Data contains {len(dates_list)} unique dates")
            
            # Register this as a temp table
            combined_df.createOrReplaceTempView("new_data")
            
            # Find all affected partitions
            affected_partitions = combined_df.select("island", "location_name", "year", "month").distinct().collect()
            
            print(f"This data will affect {len(affected_partitions)} partitions")
            
            # Process each partition separately for overwrite
            for partition in affected_partitions:
                p_island = partition["island"]
                p_location = partition["location_name"]
                p_year = partition["year"]
                p_month = partition["month"]
                
                partition_filter = f"""
                    island = '{p_island}' AND 
                    location_name = '{p_location}' AND 
                    year = {p_year} AND 
                    month = {p_month}
                """
                
                # Get new data for this partition
                new_partition_data = spark.sql(f"SELECT * FROM new_data WHERE {partition_filter}")
                
                # Define partition path
                partition_path = f"{output_bucket}island={p_island}/location_name={p_location}/year={p_year}/month={p_month}/"
                
                try:
                    # Check if this partition exists
                    spark.read.parquet(partition_path).limit(1).count()
                    
                    # Get existing data, but filter out dates that are in our update
                    existing_partition_data = spark.sql(f"""
                        SELECT * FROM weather_data 
                        WHERE {partition_filter}
                        AND NOT EXISTS (
                            SELECT 1 FROM new_data
                            WHERE new_data.date = weather_data.date
                            AND new_data.location = weather_data.location
                            AND {partition_filter}
                        )
                    """)
                    
                    # Combine existing (non-overlapping) data with new data
                    final_partition_data = existing_partition_data.union(new_partition_data)
                    
                    # Write with overwrite mode for this specific partition
                    final_partition_data.write \
                        .mode("overwrite") \
                        .parquet(partition_path)
                    
                    print(f"Updated existing partition: {p_island}/{p_location}/{p_year}/{p_month}")
                    
                except Exception as e:
                    print(f"Partition doesn't exist yet or error: {str(e)}")
                    print(f"Writing new partition: {p_island}/{p_location}/{p_year}/{p_month}")
                    
                    # Write directly as this partition doesn't exist yet
                    new_partition_data.write \
                        .mode("overwrite") \
                        .parquet(partition_path)
            
            print(f"Successfully processed and saved data for {island}/{location}")
        except Exception as e:
            print(f"Error combining and writing data for {island}/{location}: {str(e)}")
    else:
        print(f"Skipping {island}/{location} as one or both source files are missing or invalid")

print("Data processing completed successfully")
job.commit()
```

Finally, another EventBridge rule starts Glue crawler that updates the existing data catalog after a successful Glue job completion which concludes the monthly update process.

