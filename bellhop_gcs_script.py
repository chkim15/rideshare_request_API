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

# Sample 1: Prestigious Origin-Destination Pairs (Luxury residences/hotels to corporate HQs)
SAMPLE1_PAIRS = [
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

SAMPLE1_PLACES = [
    {"id": 1, "name": "15 Central Park West", "lat": 40.769000, "lng": -73.981400},
    {"id": 2, "name": "Goldman Sachs HQ", "lat": 40.714700, "lng": -74.013600},
    {"id": 3, "name": "Central Park Tower", "lat": 40.765900, "lng": -73.982000},
    {"id": 4, "name": "JP Morgan HQ", "lat": 40.755600, "lng": -73.977500},
    {"id": 5, "name": "432 Park Avenue", "lat": 40.761600, "lng": -73.971800},
    {"id": 6, "name": "CitiGroup HQ", "lat": 40.720600, "lng": -74.012800},
    {"id": 7, "name": "15 Hudson Yards", "lat": 40.753600, "lng": -74.002200},
    {"id": 8, "name": "McKinsey (WTC)", "lat": 40.712800, "lng": -74.011900},
    {"id": 9, "name": "One57 (157 W 57th St)", "lat": 40.765300, "lng": -73.979000},
    {"id": 10, "name": "BCG (Hudson Yards)", "lat": 40.753600, "lng": -74.002200},
    {"id": 11, "name": "220 Central Park South", "lat": 40.766700, "lng": -73.980900},
    {"id": 12, "name": "Google NYC", "lat": 40.740800, "lng": -74.003300},
    {"id": 13, "name": "Four Seasons Hotel New York", "lat": 40.762600, "lng": -73.969700},
    {"id": 14, "name": "Skadden Arps", "lat": 40.751700, "lng": -73.997200},
    {"id": 15, "name": "The Beekman (Thompson Hotel)", "lat": 40.711200, "lng": -74.006600},
    {"id": 16, "name": "Morgan Stanley HQ", "lat": 40.758300, "lng": -73.968600},
    {"id": 17, "name": "The St. Regis New York", "lat": 40.761600, "lng": -73.974400},
    {"id": 18, "name": "AIG", "lat": 40.705600, "lng": -74.009100},
    {"id": 19, "name": "Four Seasons Hotel New York Downtown", "lat": 40.712600, "lng": -74.009700},
    {"id": 20, "name": "Bain & Company", "lat": 40.756200, "lng": -73.981100},
]

# Sample 2: Random Manhattan locations with nearly identical distances
SAMPLE2_PAIRS = [
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

SAMPLE2_PLACES = [
    {"id": 1, "name": "Random Origin 1", "lat": 40.794705, "lng": -73.971795},
    {"id": 2, "name": "Random Destination 1", "lat": 40.739203, "lng": -74.000226},
    {"id": 3, "name": "Random Origin 2", "lat": 40.721926, "lng": -74.003187},
    {"id": 4, "name": "Random Destination 2", "lat": 40.711705, "lng": -74.007952},
    {"id": 5, "name": "Random Origin 3", "lat": 40.744180, "lng": -73.998954},
    {"id": 6, "name": "Random Destination 3", "lat": 40.782621, "lng": -73.954126},
    {"id": 7, "name": "Random Origin 4", "lat": 40.710470, "lng": -74.007748},
    {"id": 8, "name": "Random Destination 4", "lat": 40.750068, "lng": -73.991665},
    {"id": 9, "name": "Random Origin 5", "lat": 40.771169, "lng": -73.957614},
    {"id": 10, "name": "Random Destination 5", "lat": 40.786449, "lng": -73.976858},
    {"id": 11, "name": "Random Origin 6", "lat": 40.716300, "lng": -74.004792},
    {"id": 12, "name": "Random Destination 6", "lat": 40.747133, "lng": -74.000472},
    {"id": 13, "name": "Random Origin 7", "lat": 40.706267, "lng": -74.012561},
    {"id": 14, "name": "Random Destination 7", "lat": 40.726520, "lng": -73.996706},
    {"id": 15, "name": "Random Origin 8", "lat": 40.782064, "lng": -73.956258},
    {"id": 16, "name": "Random Destination 8", "lat": 40.737330, "lng": -73.998871},
    {"id": 17, "name": "Random Origin 9", "lat": 40.804611, "lng": -73.954223},
    {"id": 18, "name": "Random Destination 9", "lat": 40.749035, "lng": -73.989995},
    {"id": 19, "name": "Random Origin 10", "lat": 40.780999, "lng": -73.946376},
    {"id": 20, "name": "Random Destination 10", "lat": 40.748841, "lng": -73.993437},
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

def get_prices(api_key, api_secret, pickup_lat, pickup_lng, dest_lat, dest_lng):
    """Make API call to Bellhop to get ride prices"""
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
    
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching ride prices: {e}")
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
            "price_min_dollars": f"{price_min:.2f}",
            "price_max_dollars": f"{price_max:.2f}",
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
        "price_min_dollars", "price_max_dollars", 
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
    
    # Get price data
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
    
    # Add a small delay between API calls to avoid rate limiting
    time.sleep(2)

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
    
    # Process Sample 1 - Prestigious routes
    for pair in SAMPLE1_PAIRS:
        process_pair(api_key, api_secret, gcs_client, "Sample1", pair, SAMPLE1_PLACES)
    
    # Process Sample 2 - Random routes with similar distances
    for pair in SAMPLE2_PAIRS:
        process_pair(api_key, api_secret, gcs_client, "Sample2", pair, SAMPLE2_PLACES)
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    logger.info(f"Completed data collection cycle in {duration:.2f} seconds")

if __name__ == "__main__":
    try:
        collect_all_samples()
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")