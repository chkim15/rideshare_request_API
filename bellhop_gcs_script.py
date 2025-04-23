#!/usr/bin/env python3
"""
Hourly Bellhop API Data Collection with Google Cloud Storage
This script automatically collects ride pricing data for predefined routes every hour,
storing results in Google Cloud Storage for persistence.
"""
import os
import json
import csv
import time
import logging
import io
from datetime import datetime
import requests
from google.cloud import storage

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Google Cloud Storage settings
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME")

# LA1: LA's HIGH INCOME ORIGIN-DESTINATION PAIRS
LA1_PAIRS = [
    {"id": 1, "origin_id": 1, "destination_id": 2},
    {"id": 2, "origin_id": 3, "destination_id": 4},
    {"id": 3, "origin_id": 5, "destination_id": 6},
    {"id": 4, "origin_id": 7, "destination_id": 8},
    {"id": 5, "origin_id": 9, "destination_id": 10},
    {"id": 6, "origin_id": 11, "destination_id": 12},
    {"id": 7, "origin_id": 13, "destination_id": 14},
    {"id": 8, "origin_id": 15, "destination_id": 16},
    {"id": 9, "origin_id": 17, "destination_id": 18},
    {"id": 10, "origin_id": 19, "destination_id": 20},
]

LA1_PLACES = [
    {"id": 1, "name": "Ritz Carlton, 900 W Olympic Blvd, LA", "lat": 34.04544986014083, "lng": -118.26660977509258},
    {"id": 2, "name": "Kirkland & Ellis 555 Flower St., LA", "lat": 34.05101706931945, "lng": -118.25779177509229},
    {"id": 3, "name": "1000 S. Grand Ave, LA", "lat": 34.04246123549105, "lng": -118.26103417509272},
    {"id": 4, "name": "333 S Hope St LA", "lat": 34.053068, "lng": -118.252985},
    {"id": 5, "name": "1198 Roberto Ln, Bel Air CA", "lat": 34.097417, "lng": -118.461621},
    {"id": 6, "name": "Bel Air Country Club 10768 Bellagio Road, LA", "lat": 34.079513798810254, "lng": -118.4502622174189},
    {"id": 7, "name": "Four Seasons Hotel at Beverly Hills", "lat": 34.073142646957265, "lng": -118.38923370392745},
    {"id": 8, "name": "The Grove 189 The Grove Drive, LA", "lat": 34.07209175317847, "lng": -118.35750567509143},
    {"id": 9, "name": "Chateau Marmont, 8221 W Sunset Blvd, LA", "lat": 34.09834841616102, "lng": -118.36848287509024},
    {"id": 10, "name": "Live Nation 9348 Civic Center Dr. Beverly Hills", "lat": 34.074530158923814, "lng": -118.3986892364688},
    {"id": 11, "name": "Century Towers Residences, 2220 Ave of the Stars, LA", "lat": 34.05224295602889, "lng": -118.40877668858411},
    {"id": 12, "name": "Beverly Hills Hotel 9641 W Sunset Blvd, Beverly Hills", "lat": 34.081968921589876, "lng": -118.413392275091},
    {"id": 13, "name": "Hotel Bel Air 701 Stone Canyon Rd, LA", "lat": 34.086738838609214, "lng": -118.44639258072958},
    {"id": 14, "name": "1999 Ave of the Stars, LA", "lat": 34.05902098805131, "lng": -118.41711034625591},
    {"id": 15, "name": "Hotel Bel Air 701 Stone Canyon Rd, LA", "lat": 34.086738838609214, "lng": -118.44639258072958},
    {"id": 16, "name": "Live Nation 9348 Civic Center Dr. Beverly Hills", "lat": 34.07455681930882, "lng": -118.39873215181318},
    {"id": 17, "name": "W Hollywood, 6250 Hollywood Blvd., Hollywood", "lat": 34.101082649373154, "lng": -118.32582818858184},
    {"id": 18, "name": "Walt Disney 500 S. Buena Vista St. Burbank", "lat": 34.1562691206309, "lng": -118.325189003924},
    {"id": 19, "name": "Chateau Marmont, 8221 W Sunset Blvd, LA", "lat": 34.098357300457344, "lng": -118.36846141741805},
    {"id": 20, "name": "IBM 348 Hauser Blvd, Los Angeles", "lat": 34.0694950578102, "lng": -118.3505841462555},
]

# LA2: LA's RANDOM ORIGIN-DESTINATION PAIRS
LA2_PAIRS = [
    {"id": 1, "origin_id": 1, "destination_id": 2},
    {"id": 2, "origin_id": 3, "destination_id": 4},
    {"id": 3, "origin_id": 5, "destination_id": 6},
    {"id": 4, "origin_id": 7, "destination_id": 8},
    {"id": 5, "origin_id": 9, "destination_id": 10},
    {"id": 6, "origin_id": 11, "destination_id": 12},
    {"id": 7, "origin_id": 13, "destination_id": 14},
    {"id": 8, "origin_id": 15, "destination_id": 16},
    {"id": 9, "origin_id": 17, "destination_id": 18},
    {"id": 10, "origin_id": 19, "destination_id": 20},
]

LA2_PLACES = [
    {"id": 1, "name": "1413 S Oakhurst Dr, Los Angeles, CA 90035", "lat": 34.05497468370488, "lng": -118.39099984625615},
    {"id": 2, "name": "9725 Gregory Way, Beverly Hills, CA 90212", "lat": 34.062398158552796, "lng": -118.40703176160011},
    {"id": 3, "name": "1932 Holmby Ave, West Los Angeles", "lat": 34.05402154126493, "lng": -118.4238958327644},
    {"id": 4, "name": "2034 Cotner Ave F3, Los Angeles, CA 90025", "lat": 34.0417150975965, "lng": -118.44082083276484},
    {"id": 5, "name": "5880 Pickford St, Los Angeles, CA 90019", "lat": 34.04758376943641, "lng": -118.36812777509247},
    {"id": 6, "name": "1046 Redondo Blvd, Los Angeles, CA 90019", "lat": 34.05587448210048, "lng": -118.34580469043648},
    {"id": 7, "name": "229 S Mansfield Ave, Los Angeles, CA 90036", "lat": 34.07031550733673, "lng": -118.34102593276367},
    {"id": 8, "name": "800-898 N Stanley Ave, Los Angeles, CA 90046", "lat": 34.08565254334001, "lng": -118.35611504625471},
    {"id": 9, "name": "301 Irving Blvd. LA", "lat": 34.076549358626764, "lng": -118.31897608858301},
    {"id": 10, "name": "1000 Ridgeley Dr, LA", "lat": 34.05729893404654, "lng": -118.35221737509205},
    {"id": 11, "name": "3720 Halldale Ave, Los Angeles, CA 90018", "lat": 34.020337667957605, "lng": -118.30274610392964},
    {"id": 12, "name": "6608 Normandie Ave, Los Angeles", "lat": 33.97899782857493, "lng": -118.30003113276743},
    {"id": 13, "name": "1417 Orchard Ave., Los Angeles", "lat": 34.04589999988305, "lng": -118.28867446160076},
    {"id": 14, "name": "34.0352934, -118.2458547", "lat": 34.0352934, "lng": -118.2458547},
    {"id": 15, "name": "3411 Normandie Ave, Los Angeles", "lat": 34.025271136645635, "lng": -118.3004307327655},
    {"id": 16, "name": "1673 West Blvd, Los Angeles, CA 90019", "lat": 34.043778673780096, "lng": -118.33590223276474},
    {"id": 17, "name": "1112 N Hoover St, Los Angeles, CA 90029", "lat": 34.09156558848393, "lng": -118.28431831741841},
    {"id": 18, "name": "460 S Chevy Chase Dr, Glendale, CA 91205", "lat": 34.1410471695555, "lng": -118.23919080392464},
    {"id": 19, "name": "4655 W Washington Blvd, Los Angeles", "lat": 34.040150021784946, "lng": -118.34140303946852},
    {"id": 20, "name": "11730 Palms Blvd, Los Angeles", "lat": 34.01565611870629, "lng": -118.43111784625765},
]

# Chicago1: Chicago's HIGH INCOME ORIGIN-DESTINATION PAIRS
CHICAGO1_PAIRS = [
    {"id": 1, "origin_id": 1, "destination_id": 2},
    {"id": 2, "origin_id": 3, "destination_id": 4},
    {"id": 3, "origin_id": 5, "destination_id": 6},
    {"id": 4, "origin_id": 7, "destination_id": 8},
    {"id": 5, "origin_id": 9, "destination_id": 10},
    {"id": 6, "origin_id": 11, "destination_id": 12},
    {"id": 7, "origin_id": 13, "destination_id": 14},
    {"id": 8, "origin_id": 15, "destination_id": 16},
    {"id": 9, "origin_id": 17, "destination_id": 18},
    {"id": 10, "origin_id": 19, "destination_id": 20},
]

CHICAGO1_PLACES = [
    {"id": 1, "name": "Peninsula Hotel, 108 East Superior Street, Chicago", "lat": 41.896034648849415, "lng": -87.6250967458956},
    {"id": 2, "name": "JP Morgan, 10 S Dearborn St., Chicago", "lat": 41.88184132959059, "lng": -87.62962990171587},
    {"id": 3, "name": "Park Hyatt, 108 East Superior Street, Chicago", "lat": 41.89613500694018, "lng": -87.62507620356772},
    {"id": 4, "name": "CME Group, 20 South Wacker Drive, Chicago", "lat": 41.88130482652534, "lng": -87.63725027473248},
    {"id": 5, "name": "Peninsula Hotel, 108 East Superior Street, Chicago", "lat": 41.896034648849415, "lng": -87.6250967458956},
    {"id": 6, "name": "233 S Wacker Dr, Chicago, IL 60606", "lat": 41.878984906479424, "lng": -87.63590067473257},
    {"id": 7, "name": "Four Seasons Hotel, 120 East Delaware Place, Chicago", "lat": 41.89947035629683, "lng": -87.6251379170593},
    {"id": 8, "name": "Mondelez Intl., 905 W Fulton Market, Chicago", "lat": 41.88677945463177, "lng": -87.65009791705995},
    {"id": 9, "name": "850 North Lake Shore Drive, Chicago", "lat": 41.89841398733954, "lng": -87.61873897658424},
    {"id": 10, "name": "234 S Wacker Dr, Chicago, IL 60606", "lat": 41.87850212543636, "lng": -87.63704298822427},
    {"id": 11, "name": "Peninsula Hotel, 108 East Superior Street, Chicago", "lat": 41.896034648849415, "lng": -87.6250967458956},
    {"id": 12, "name": "McDonald's 110 N Carpenter St, Chicago", "lat": 41.88333850044072, "lng": -87.65339219007666},
    {"id": 13, "name": "1837 North Fremont St., Chicago", "lat": 41.91497577599523, "lng": -87.65053140356683},
    {"id": 14, "name": "CME Group, 20 S Wacker Dr, Chicago, IL 60606", "lat": 41.88132080239109, "lng": -87.63726100356858},
    {"id": 15, "name": "2350 North Orchard St., Chicago", "lat": 41.92521402553522, "lng": -87.64654221891071},
    {"id": 16, "name": "233 S Wacker Dr, Chicago, IL 60606", "lat": 41.878984906479424, "lng": -87.63590067473257},
    {"id": 17, "name": "2350 North Orchard St., Chicago", "lat": 41.92521402553522, "lng": -87.64654221891071},
    {"id": 18, "name": "Jones, Day 10 N Wacker Drive, Chicago", "lat": 41.88228200070876, "lng": -87.63721798822417},
    {"id": 19, "name": "Marriott Marquis", "lat": 41.85413044996789, "lng": -87.62054703240601},
    {"id": 20, "name": "2350 North Orchard St., Chicago", "lat": 41.92525393773527, "lng": -87.64648857473024},
]

# Chicago2: Chicago's RANDOM ORIGIN-DESTINATION PAIRS
CHICAGO2_PAIRS = [
    {"id": 1, "origin_id": 1, "destination_id": 2},
    {"id": 2, "origin_id": 3, "destination_id": 4},
    {"id": 3, "origin_id": 5, "destination_id": 6},
    {"id": 4, "origin_id": 7, "destination_id": 8},
    {"id": 5, "origin_id": 9, "destination_id": 10},
    {"id": 6, "origin_id": 11, "destination_id": 12},
    {"id": 7, "origin_id": 13, "destination_id": 14},
    {"id": 8, "origin_id": 15, "destination_id": 16},
    {"id": 9, "origin_id": 17, "destination_id": 18},
    {"id": 10, "origin_id": 19, "destination_id": 20},
]

CHICAGO2_PLACES = [
    {"id": 1, "name": "120 S State St #205, Chicago", "lat": 41.88100946689491, "lng": -87.62723285938812},
    {"id": 2, "name": "920 W Randolph St, Chicago", "lat": 41.8846932964883, "lng": -87.65068973240444},
    {"id": 3, "name": "421 West Huron St., Chicago", "lat": 41.89460309687061, "lng": -87.6395318458957},
    {"id": 4, "name": "One S. Dearborn S., Chicago", "lat": 41.8817566137375, "lng": -87.62859023637145},
    {"id": 5, "name": "1950 West 21 Street, Chicago", "lat": 41.85437143217611, "lng": -87.6754902900782},
    {"id": 6, "name": "1002 S Racine Ave, Chicago", "lat": 41.869241402443905, "lng": -87.65675640356918},
    {"id": 7, "name": "698-664 N Hoyne Ave, Chicago", "lat": 41.894203711528725, "lng": -87.6794165170596},
    {"id": 8, "name": "815-899 N Lawndale Ave, Chicago", "lat": 41.896149879100456, "lng": -87.71852289007606},
    {"id": 9, "name": "1063 W. Polk St., Chicago", "lat": 41.87176899738908, "lng": -87.65363523240511},
    {"id": 10, "name": "37 E Madison St, Chicago", "lat": 41.882356230106865, "lng": -87.62793380171584},
    {"id": 11, "name": "535-599 N Paulina St, Chicago", "lat": 41.89226151614296, "lng": -87.66943541705969},
    {"id": 12, "name": "4164-4198 W Augusta Blvd, Chicago", "lat": 41.89912737128119, "lng": -87.73045155938718},
    {"id": 13, "name": "342 W Carroll Ave, Chicago", "lat": 41.888021251992114, "lng": -87.66285116124038},
    {"id": 14, "name": "1479 S Clark St, Chicago", "lat": 41.86221428750783, "lng": -87.63031354774995},
    {"id": 15, "name": "185 W Washington St, Chicago", "lat": 41.8831619623203, "lng": -87.63369137473234},
    {"id": 16, "name": "2435 W Roosevelt Rd, Chicago, IL 60608", "lat": 41.86662138517686, "lng": -87.68747987658581},
    {"id": 17, "name": "2533 W Pope John Paul II Dr, Chicago", "lat": 41.815578305863475, "lng": -87.68849473240793},
    {"id": 18, "name": "1460 S Clark St, Chicago", "lat": 41.862650115623964, "lng": -87.63027090356947},
    {"id": 19, "name": "3240 N Kimball Ave, Chicago", "lat": 41.94101569785078, "lng": -87.71280727472941},
    {"id": 20, "name": "615 W. Madison Ave., Chicago", "lat": 41.88184381359574, "lng": -87.64318027473243},
]

def initialize_gcs_client():
    """Initialize Google Cloud Storage client"""
    try:
        # When running in GitHub Actions, the credentials will be injected from secrets
        return storage.Client()
    except Exception as e:
        logger.error(f"Failed to initialize GCS client: {e}")
        raise

def get_place_by_id(sample_places, place_id):
    """Get a place by its ID"""
    for place in sample_places:
        if place['id'] == place_id:
            return place
    return None

def get_prices(api_key, api_secret, pickup_lat, pickup_lng, dest_lat, dest_lng, max_retries=5):
    """Make API call to Bellhop to get ride prices with retry logic for rate limiting"""
    headers = {
        "accept": "application/json",
        "X-API-KEY": api_key,
        "X-API-SECRET": api_secret,
        "Content-Type": "application/json"
    }
    
    payload = {
        "pickup": {
            "latitude": pickup_lat,
            "longitude": pickup_lng
        },
        "destination": {
            "latitude": dest_lat,
            "longitude": dest_lng
        }
    }
    
    url = "https://api.bellhop.me/api/rich-intelligent-pricing"
    
    # Exponential backoff parameters
    base_delay = 10  # Start with a 10-second delay
    
    for attempt in range(max_retries):
        try:
            response = requests.post(url, headers=headers, json=payload)
            
            # If we get a rate limit error, wait and retry
            if response.status_code == 429:
                retry_delay = base_delay * (2 ** attempt)  # Exponential backoff
                logger.warning(f"Rate limit hit. Retrying in {retry_delay} seconds... (Attempt {attempt+1}/{max_retries})")
                time.sleep(retry_delay)
                continue
                
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429 and attempt < max_retries - 1:
                # This case is handled above, just continue the loop
                continue
            logger.error(f"HTTP error fetching ride prices: {e}")
            return None
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching ride prices: {e}")
            return None
            
    logger.error(f"Failed to get prices after {max_retries} attempts due to rate limiting")
    return None

def save_results_to_gcs_json(client, data, sample_type, pair_id):
    """Save API response to a JSON file in Google Cloud Storage"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"json/data_{sample_type}_pair{pair_id}_{timestamp}.json"
    
    try:
        bucket = client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(filename)
        
        # Convert to JSON string
        json_string = json.dumps(data, indent=2)
        
        # Upload
        blob.upload_from_string(json_string, content_type="application/json")
        
        logger.info(f"JSON data saved to gs://{GCS_BUCKET_NAME}/{filename}")
        return f"gs://{GCS_BUCKET_NAME}/{filename}"
    except Exception as e:
        logger.error(f"Error saving JSON to GCS: {e}")
        return None

def download_csv_from_gcs(client, csv_filename="ride_prices.csv"):
    """Download existing CSV from GCS, or return empty data if it doesn't exist"""
    try:
        bucket = client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(csv_filename)
        
        if blob.exists():
            content = blob.download_as_text()
            return content
        else:
            logger.info(f"CSV file {csv_filename} does not exist in GCS yet. Will create a new one.")
            return None
    except Exception as e:
        logger.error(f"Error downloading CSV from GCS: {e}")
        return None

def append_to_csv_and_upload(client, csv_data, data_rows, fieldnames, csv_filename="ride_prices.csv"):
    """Append new rows to CSV data and upload to GCS"""
    try:
        output = io.StringIO()
        
        if csv_data is None:
            # Create new CSV with header
            writer = csv.DictWriter(output, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data_rows)
        else:
            # Append to existing CSV data
            # First, write the existing data
            output.write(csv_data)
            
            # Check if we need to add a newline
            if not csv_data.endswith('\n'):
                output.write('\n')
            
            # Then append the new rows
            writer = csv.DictWriter(output, fieldnames=fieldnames)
            writer.writerows(data_rows)
        
        # Upload back to GCS
        bucket = client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(csv_filename)
        blob.upload_from_string(output.getvalue(), content_type="text/csv")
        
        logger.info(f"CSV data updated in gs://{GCS_BUCKET_NAME}/{csv_filename}")
        return True
    except Exception as e:
        logger.error(f"Error appending to CSV and uploading to GCS: {e}")
        return False

def save_results_to_csv(client, data, sample_type, pickup_name, dest_name):
    """Process API response and save to CSV in Google Cloud Storage"""
    csv_filename = "ride_prices.csv"
    
    # Get the current time
    timestamp = datetime.now()
    date_str = timestamp.strftime("%Y-%m-%d")
    time_str = timestamp.strftime("%H:%M:%S")
    
    # Extract search ID
    search_id = data.get("search_id", "")
    
    # Parse the results
    results = data.get("results", [])
    if not results or not results[0].get("prices"):
        logger.warning("No ride options to save")
        return None
    
    # Prepare rows for CSV
    rows = []
    for price in results[0].get("prices", []):
        provider = price.get("provider", "")
        product = price.get("product", "")
        service_level = price.get("service_level", "")
        
        # Price in dollars
        price_min = price.get("price_min", 0) / 100 if price.get("price_min") is not None else 0
        price_max = price.get("price_max", 0) / 100 if price.get("price_max") is not None else 0

        # Discounted Price in dollars
        price_min_discounted = price.get("price_min_discounted", 0) / 100 if price.get("price_min_discounted") is not None else 0
        price_max_discounted = price.get("price_max_discounted", 0) / 100 if price.get("price_max_discounted") is not None else 0
        
        # Wait time in seconds
        wait_min = price.get("est_pickup_wait_time", {}).get("min", 0)
        wait_max = price.get("est_pickup_wait_time", {}).get("max", 0)
        
        # Trip details
        trip_seconds = price.get("est_time_after_pickup_till_dropoff", 0)
        distance_meters = price.get("distance_meters", 0)
        surge = price.get("surge_multiplier", 1.0)
        
        # Create a row
        row = {
            "date": date_str,
            "time": time_str,
            "search_id": search_id,
            "sample_type": sample_type,
            "pickup": pickup_name,
            "destination": dest_name,
            "provider": provider,
            "product": product,
            "service_level": service_level,
            "price_min": f"{price_min:.2f}",
            "price_max": f"{price_max:.2f}",
            "price_min_discounted": f"{price_min_discounted:.2f}",
            "price_max_discounted": f"{price_max_discounted:.2f}",
            "wait_min_seconds": wait_min,
            "wait_max_seconds": wait_max if wait_max else "",
            "trip_seconds": trip_seconds,
            "distance_meters": distance_meters,
            "surge_multiplier": surge
        }
        rows.append(row)
    
    # Define fieldnames (columns)
    fieldnames = [
        "date", "time", "search_id", "sample_type", "pickup", "destination", 
        "provider", "product", "service_level", 
        "price_min", "price_max", "price_min_discounted", "price_max_discounted",
        "wait_min_seconds", "wait_max_seconds", 
        "trip_seconds", "distance_meters", "surge_multiplier"
    ]

# Download existing CSV
    existing_csv_data = download_csv_from_gcs(client, csv_filename)
    
    # Append new data and upload
    success = append_to_csv_and_upload(client, existing_csv_data, rows, fieldnames, csv_filename)
    
    if success:
        return f"gs://{GCS_BUCKET_NAME}/{csv_filename}"
    else:
        return None

def process_pair(api_key, api_secret, gcs_client, sample_type, pair, places):
    """Process a single origin-destination pair"""
    origin_id = pair["origin_id"]
    dest_id = pair["destination_id"]
    
    # Get origin and destination place details
    origin = get_place_by_id(places, origin_id)
    destination = get_place_by_id(places, dest_id)
    
    if not origin or not destination:
        logger.error(f"Invalid place IDs: origin_id={origin_id}, dest_id={dest_id}")
        return
    
    # Log collection attempt
    logger.info(f"Collecting {sample_type} - Pair {pair['id']}: {origin['name']} to {destination['name']}")
    
    # Get price data with retry logic
    response = get_prices(
        api_key,
        api_secret,
        origin["lat"],
        origin["lng"],
        destination["lat"],
        destination["lng"]
    )
    
    if not response:
        logger.error(f"Failed to collect data for {sample_type} - Pair {pair['id']}")
        return
    
    # Save results to both JSON and CSV in GCS
    save_results_to_gcs_json(gcs_client, response, sample_type, pair['id'])
    save_results_to_csv(gcs_client, response, sample_type, origin['name'], destination['name'])
    
    # Add a longer delay between API calls to avoid rate limiting
    logger.info(f"Waiting 15 seconds before next API call to avoid rate limiting...")
    time.sleep(15)

def collect_all_samples():
    """Collect data for all sample pairs"""
    start_time = datetime.now()
    logger.info(f"Starting data collection cycle at {start_time}")
    
    # Get API credentials from environment variables
    api_key = os.environ.get("BELLHOP_API_KEY")
    api_secret = os.environ.get("BELLHOP_API_SECRET")
    
    if not api_key or not api_secret:
        logger.error("Error: API credentials not found in environment variables")
        return
    
    # Initialize GCS client
    try:
        gcs_client = initialize_gcs_client()
    except Exception as e:
        logger.error(f"Failed to initialize GCS: {e}")
        return
    
    # Process pairs with error handling and splits
    try:
        # Process LA1 - LA HIGH INCOME routes
        logger.info("Starting LA1 (HIGH INCOME) collection...")
        
        for i, pair in enumerate(LA1_PAIRS):
            try:
                process_pair(api_key, api_secret, gcs_client, "LA1", pair, LA1_PLACES)
                logger.info(f"Successfully processed LA1 pair {pair['id']} ({i+1}/{len(LA1_PAIRS)})")
            except Exception as e:
                logger.error(f"Error processing LA1 pair {pair['id']}: {e}")
                # Continue with next pair instead of exiting
                continue
        
        # Add a delay between samples to recover from potential rate limiting
        logger.info("Completed LA1. Waiting 60 seconds before starting LA2...")
        time.sleep(60)
        
        # Process LA2 - LA RANDOM routes
        logger.info("Starting LA2 (RANDOM) collection...")
        
        for i, pair in enumerate(LA2_PAIRS):
            try:
                process_pair(api_key, api_secret, gcs_client, "LA2", pair, LA2_PLACES)
                logger.info(f"Successfully processed LA2 pair {pair['id']} ({i+1}/{len(LA2_PAIRS)})")
            except Exception as e:
                logger.error(f"Error processing LA2 pair {pair['id']}: {e}")
                # Continue with next pair instead of exiting
                continue
        
        # Add a delay between LA and Chicago samples
        logger.info("Completed LA2. Waiting 60 seconds before starting Chicago1...")
        time.sleep(60)
        
        # Process Chicago1 - Chicago HIGH INCOME routes
        logger.info("Starting Chicago1 (HIGH INCOME) collection...")
        
        for i, pair in enumerate(CHICAGO1_PAIRS):
            try:
                process_pair(api_key, api_secret, gcs_client, "Chicago1", pair, CHICAGO1_PLACES)
                logger.info(f"Successfully processed Chicago1 pair {pair['id']} ({i+1}/{len(CHICAGO1_PAIRS)})")
            except Exception as e:
                logger.error(f"Error processing Chicago1 pair {pair['id']}: {e}")
                # Continue with next pair instead of exiting
                continue
        
        # Add a delay between Chicago1 and Chicago2
        logger.info("Completed Chicago1. Waiting 60 seconds before starting Chicago2...")
        time.sleep(60)
        
        # Process Chicago2 - Chicago RANDOM routes
        logger.info("Starting Chicago2 (RANDOM) collection...")
        
        for i, pair in enumerate(CHICAGO2_PAIRS):
            try:
                process_pair(api_key, api_secret, gcs_client, "Chicago2", pair, CHICAGO2_PLACES)
                logger.info(f"Successfully processed Chicago2 pair {pair['id']} ({i+1}/{len(CHICAGO2_PAIRS)})")
            except Exception as e:
                logger.error(f"Error processing Chicago2 pair {pair['id']}: {e}")
                # Continue with next pair instead of exiting
                continue
        
        # Add a delay before Sample 3
        logger.info("Completed Chicago2. Waiting 60 seconds before starting Sample3...")
        time.sleep(60)
        
        # Process Sample 3 - NYC Residential to Airport Routes
        logger.info("Starting Sample 3 collection...")

        for i, pair in enumerate(SAMPLE3_PAIRS):
            try:
                process_pair(api_key, api_secret, gcs_client, "Sample3", pair, SAMPLE3_PLACES)
                logger.info(f"Successfully processed Sample3 pair {pair['id']} ({i+1}/{len(SAMPLE3_PAIRS)})")
            except Exception as e:
                logger.error(f"Error processing Sample3 pair {pair['id']}: {e}")
                # Continue with next pair instead of exiting
                continue
                
    except Exception as e:
        logger.error(f"Unexpected error in collection process: {e}")
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    logger.info(f"Completed data collection cycle in {duration:.2f} seconds")

if __name__ == "__main__":
    try:
        collect_all_samples()
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")