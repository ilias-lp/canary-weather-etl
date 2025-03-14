# Cloud based ETL design for historical weather database



**Project Outline:**

- Data scope and sources overview
- Extraction of historical raw data in JSON format via API calls with Lambda function from multiple sources
- Merging and transformation of raw data with AWS Glue into one dataset in Parquet format stored in S3 bucket
- Data cataloging with Glue catalogue and ad-hoc querying with Athena
- Setting up periodic update pipeline for current data that runs automatically through Eventbridge, Lambda, S3 and Glue services

## Data collection

Project scope is collection of daily historical data of 4+ years from 2021-01-01 until current date (March 2025) for various weather stations across each of the seven Canary Islands.

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

There are multiple sources that provide API acces to historical weather data. However many of them have strict limitations for their free tiers and/or do not have an extensive set of metrics that one might need. After a thourough research, sources were narrowed down to two following ones:

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

It can be seen that it comprises all essential variables but still misses some of other metrics of potential interest such as UV index or percentage of cloudcover. This brings us to the second data source for additional variables.

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

Below is Lambda function code for Open-meteo API that doesn't require an API key and runs smoothly as long as the number of calls stays below 10 000 which implies execution of the code below in 3 steps by commenting out 2/3 of locations for each call, as 365 days * 4+ years * 14 stations = 20 440+ calls required in total.

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

In a similar way, Lambda function code for Visual Crossing API is collecting required data. But this time an account based API key for billing is necessary. In order to avoid exposing of such sensitive credentials in the code, AWS Secrets Manager service is being used where API key is securely stored under a specified public name and being accessed through the get_secret function.


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
                
                # If none of the expected keys are found but there's only one value, use that
                if len(secret_json) == 1:
                    api_key = next(iter(secret_json.values()))
                    logger.info("Using the only value in the JSON")
                    return api_key
                    
                # Log all keys (without values) for debugging
                logger.error(f"Could not find expected key in JSON. Available keys: {list(secret_json.keys())}")
                
                # Last resort: return the entire JSON string
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

This concludes the historical raw data collection that is now stored in S3 and organized by its location.

![s3_1](https://github.com/user-attachments/assets/abc6b59d-6ced-430a-8e73-ce7bec1a2b32)

Each location folder contains 2 JSON files corresponding to 2 data sources of origin. In order to differentiate, files from Visual Crossing got assigned 'vc' prefix in their names.

![s3_2](https://github.com/user-attachments/assets/55c20375-c824-4e3c-894d-9a7ca2d1ab4f)

## Data transformation

Before merging JSON files together it's important to point out that despite same file format, they have different internal structure.

Files from Open-meteo API have array structure that goes as follows:

{"location": "Tenerife/Vilaflor", "latitude": 28.1562, "longitude": -16.6359, "elevation": 1387.0, "timezone": "Atlantic/Canary", "daily": {"date": ["2021-01-01", "2021-01-02", "2021-01-03", "2021-01-04", "2021-01-05", "2021-01-06", "2021-01-07", "2021-01-08", "2021-01-09", "2021-01-10", "2021-01-11", "2021-01-12", "2021-01-13", ..... ]

"weather_code": [51, 51, 51, 63, 3, 63, 63, 63, 63, 51, 53, 53, 3, 3, 2, 2, 3, 51, 2, 2, 3, 51, 3, 3, 2, 1, 0, 0, 0, 0, 0, 3, 3, 2, 53, 63, 51, 51, 3, 51, 51, 3, 53, 0, 0, 2, 0, 3, 3, 1, 63, 51, 51, 3, 51, 2, 2, 55, 55, 51, 53, 51, 0, 61, 51, 51, 53, 53, 1, 0, 3, 3, 3, 3, ...... ]

Whereas files from Visual Crossing API have nested structure:

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
      
    }, ...



