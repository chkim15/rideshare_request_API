# main.py - Google Cloud Function for Bellhop API Data Collection

import json
import csv
import os
import logging
import time
from datetime import datetime
import requests
from google.cloud import storage
import functions_framework  # Import the functions_framework package

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Sample 1: Prestigious Origin-Destination Pairs
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

# Cloud Storage bucket name (you'll need to create this bucket)
BUCKET_NAME = "bellhop-ride-data"

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

def save_results_to_json(data, sample_type, pair_id):
    """Save API response to a JSON file in Google Cloud Storage"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"json/data_{sample_type}_pair{pair_id}_{timestamp}.json"
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(filename)
    
    blob.upload_from_string(
        json.dumps(data, indent=2),
        content_type="application/json"
    )
    
    logger.info(f"JSON data saved to gs://{BUCKET_NAME}/{filename}")
    return f"gs://{BUCKET_NAME}/{filename}"

def parse_ride_data(data, sample_type, pickup_name, dest_name):
    """Parse API response into CSV-ready rows"""
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
        return []
    
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
    
    return rows

def append_to_csv_in_storage(rows):
    """Append rows to CSV file in Google Cloud Storage"""
    if not rows:
        return None
    
    # Define fieldnames (columns)
    fieldnames = [
        "date", "time", "search_id", "sample_type", "pickup", "destination", 
        "provider", "product", "service_level", 
        "price_min_dollars", "price_max_dollars", 
        "wait_min_seconds", "wait_max_seconds", 
        "trip_seconds", "distance_meters", "surge_multiplier"
    ]
    
    csv_filename = "ride_prices.csv"
    
    # Initialize Storage client
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(csv_filename)
    
    # Check if file exists
    file_exists = blob.exists()
    
    # Create CSV in memory
    csv_content = ""
    
    if not file_exists:
        # Create new file with header
        csv_content += ",".join(fieldnames) + "\n"
    else:
        # Download existing content
        try:
            existing_content = blob.download_as_text()
            csv_content = existing_content
        except Exception as e:
            logger.error(f"Error downloading existing CSV: {e}")
            # If error, start fresh
            csv_content = ",".join(fieldnames) + "\n"
    
    # Add new rows
    for row in rows:
        # Ensure values are properly escaped and ordered according to fieldnames
        csv_row = [str(row.get(field, "")) for field in fieldnames]
        csv_content += ",".join(csv_row) + "\n"
    
    # Upload back to storage
    try:
        blob.upload_from_string(csv_content, content_type="text/csv")
        logger.info(f"CSV data appended to gs://{BUCKET_NAME}/{csv_filename}")
        return f"gs://{BUCKET_NAME}/{csv_filename}"
    except Exception as e:
        logger.error(f"Error appending to CSV: {e}")
        return None

def process_pair(api_key, api_secret, sample_type, pair, places):
    """Process a single origin-destination pair"""
    origin_id = pair["origin_id"]
    dest_id = pair["destination_id"]
    
    # Get origin and destination place details
    origin = get_place_by_id(places, origin_id)
    destination = get_place_by_id(places, dest_id)
    
    if not origin or not destination:
        logger.error(f"Invalid place IDs: origin_id={origin_id}, dest_id={dest_id}")
        return None
    
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
        return None
    
    # Save results to JSON
    save_results_to_json(response, sample_type, pair['id'])
    
    # Parse and prepare for CSV
    rows = parse_ride_data(response, sample_type, origin['name'], destination['name'])
    
    return rows

# Use the functions_framework decorator to specify HTTP trigger
@functions_framework.http
def collect_bellhop_data(request):
    """
    Cloud Function entry point triggered by HTTP request or Cloud Scheduler
    """
    start_time = datetime.now()
    logger.info(f"Starting data collection cycle at {start_time}")
    
    # Get API credentials from environment variables
    api_key = os.environ.get("BELLHOP_API_KEY")
    api_secret = os.environ.get("BELLHOP_API_SECRET")
    
    if not api_key or not api_secret:
        error_msg = "Error: API credentials not found in environment variables"
        logger.error(error_msg)
        return error_msg, 500
    
    all_csv_rows = []
    
    # Process Sample 1 - Prestigious routes
    for pair in SAMPLE1_PAIRS:
        rows = process_pair(api_key, api_secret, "Sample1", pair, SAMPLE1_PLACES)
        if rows:
            all_csv_rows.extend(rows)
        time.sleep(1)  # Small delay between calls
    
    # Process Sample 2 - Random routes with similar distances
    for pair in SAMPLE2_PAIRS:
        rows = process_pair(api_key, api_secret, "Sample2", pair, SAMPLE2_PLACES)
        if rows:
            all_csv_rows.extend(rows)
        time.sleep(1)  # Small delay between calls
    
    # Append all data to CSV
    if all_csv_rows:
        append_to_csv_in_storage(all_csv_rows)
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    logger.info(f"Completed data collection cycle in {duration:.2f} seconds")
    
    return f"Successfully collected data for {len(all_csv_rows)} ride options", 200

# For local testing
if __name__ == "__main__":
    # This code block only runs when executing the file directly
    # It won't run when deployed to Cloud Functions
    from flask import Flask, request as flask_request
    
    app = Flask(__name__)
    
    @app.route("/", methods=["GET", "POST"])
    def index():
        return collect_bellhop_data(flask_request)
    
    if os.environ.get("PORT"):
        port = int(os.environ.get("PORT"))
    else:
        port = 8080
        
    print(f"Starting Flask server on port {port}")
    app.run(host="0.0.0.0", port=port)

# #!/usr/bin/env python3
# """
# Bellhop to BigQuery CLI application
# """
# import os
# import argparse
# import json
# from dotenv import load_dotenv
# from src.api import BellhopAPI
# from src.storage import BigQueryStorage

# def parse_args():
#     """Parse command line arguments"""
#     parser = argparse.ArgumentParser(description='Fetch ride prices and save to BigQuery')
#     parser.add_argument('--pickup-lat', type=float, required=True, help='Pickup latitude')
#     parser.add_argument('--pickup-lng', type=float, required=True, help='Pickup longitude')
#     parser.add_argument('--dest-lat', type=float, required=True, help='Destination latitude')
#     parser.add_argument('--dest-lng', type=float, required=True, help='Destination longitude')
#     parser.add_argument('--print-response', action='store_true', help='Print API response')
#     parser.add_argument('--save', action='store_true', help='Save results to BigQuery')
#     return parser.parse_args()

# def display_results(response_data):
#     """Display ride pricing results in a readable format"""
#     if not response_data:
#         print("No results available")
#         return

#     print("\n=== AVAILABLE RIDES ===")
#     print("=" * 70)
    
#     results = response_data.get("results", [])
#     if not results or not results[0].get("prices"):
#         print("No ride options available")
#         return
    
#     # Group by provider
#     providers = {}
#     for price in results[0].get("prices", []):
#         provider = price.get("provider")
#         if provider not in providers:
#             providers[provider] = []
#         providers[provider].append(price)
    
#     # Sort each provider's options by price
#     for provider, options in providers.items():
#         options.sort(key=lambda x: x.get("price_min", 0))
    
#     # Display results by provider
#     for provider, options in providers.items():
#         print(f"\n{provider} OPTIONS:")
#         print("-" * 70)
#         print(f"{'PRODUCT':<20} {'PRICE':<15} {'WAIT TIME':<15} {'TRIP TIME':<15}")
#         print("-" * 70)
        
#         for option in options:
#             product = option.get("product", "Unknown")
            
#             # Format price in dollars
#             price_min = option.get("price_min", 0) / 100 if option.get("price_min") else 0
#             price = f"${price_min:.2f}"
            
#             # Format wait time
#             wait_seconds = option.get("est_pickup_wait_time", {}).get("min", "?")
#             wait_time = f"{wait_seconds} seconds"
            
#             # Format trip time
#             trip_seconds = option.get("est_time_after_pickup_till_dropoff", 0)
#             trip_time = f"{trip_seconds} seconds"
            
#             print(f"{product:<20} {price:<15} {wait_time:<15} {trip_time:<15}")

# def main():
#     """Main application function"""
#     # Load environment variables
#     load_dotenv()
    
#     # Parse command line arguments
#     args = parse_args()
    
#     # Initialize API client
#     api_client = BellhopAPI(
#         api_key=os.environ.get("BELLHOP_API_KEY"),
#         api_secret=os.environ.get("BELLHOP_API_SECRET")
#     )
    
#     # Fetch ride prices
#     print(f"Fetching ride prices from ({args.pickup_lat}, {args.pickup_lng}) to ({args.dest_lat}, {args.dest_lng})...")
#     response = api_client.get_prices(
#         pickup_lat=args.pickup_lat, 
#         pickup_lng=args.pickup_lng,
#         dest_lat=args.dest_lat,
#         dest_lng=args.dest_lng
#     )
    
#     # Print raw response if requested
#     if args.print_response:
#         print("\nRaw API response:")
#         print(json.dumps(response, indent=2))
    
#     # Display formatted results
#     display_results(response)
    
#     # Save to BigQuery if requested
#     if args.save:
#         storage = BigQueryStorage()
#         storage.save(
#             response,
#             pickup_lat=args.pickup_lat,
#             pickup_lng=args.pickup_lng,
#             dest_lat=args.dest_lat,
#             dest_lng=args.dest_lng
#         )
#         print("\nResults saved to BigQuery successfully!")

# if __name__ == "__main__":
#     main()

# # main.py - Google Cloud Function for Bellhop API Data Collection
# import json
# import csv
# import os
# import logging
# import time
# from datetime import datetime
# import requests
# from google.cloud import storage

# # Configure logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# # Sample 1: Prestigious Origin-Destination Pairs
# SAMPLE1_PAIRS = [
#     {"id": 1, "origin_id": 1, "destination_id": 2},
#     {"id": 2, "origin_id": 3, "destination_id": 4},
#     {"id": 3, "origin_id": 5, "destination_id": 6},
#     {"id": 4, "origin_id": 7, "destination_id": 8},
#     {"id": 5, "origin_id": 9, "destination_id": 10},
#     {"id": 6, "origin_id": 11, "destination_id": 12},
#     {"id": 7, "origin_id": 13, "destination_id": 14},
#     {"id": 8, "origin_id": 15, "destination_id": 16},
#     {"id": 9, "origin_id": 17, "destination_id": 18},
#     {"id": 10, "origin_id": 19, "destination_id": 20},
# ]

# SAMPLE1_PLACES = [
#     {"id": 1, "name": "15 Central Park West", "lat": 40.769000, "lng": -73.981400},
#     {"id": 2, "name": "Goldman Sachs HQ", "lat": 40.714700, "lng": -74.013600},
#     {"id": 3, "name": "Central Park Tower", "lat": 40.765900, "lng": -73.982000},
#     {"id": 4, "name": "JP Morgan HQ", "lat": 40.755600, "lng": -73.977500},
#     {"id": 5, "name": "432 Park Avenue", "lat": 40.761600, "lng": -73.971800},
#     {"id": 6, "name": "CitiGroup HQ", "lat": 40.720600, "lng": -74.012800},
#     {"id": 7, "name": "15 Hudson Yards", "lat": 40.753600, "lng": -74.002200},
#     {"id": 8, "name": "McKinsey (WTC)", "lat": 40.712800, "lng": -74.011900},
#     {"id": 9, "name": "One57 (157 W 57th St)", "lat": 40.765300, "lng": -73.979000},
#     {"id": 10, "name": "BCG (Hudson Yards)", "lat": 40.753600, "lng": -74.002200},
#     {"id": 11, "name": "220 Central Park South", "lat": 40.766700, "lng": -73.980900},
#     {"id": 12, "name": "Google NYC", "lat": 40.740800, "lng": -74.003300},
#     {"id": 13, "name": "Four Seasons Hotel New York", "lat": 40.762600, "lng": -73.969700},
#     {"id": 14, "name": "Skadden Arps", "lat": 40.751700, "lng": -73.997200},
#     {"id": 15, "name": "The Beekman (Thompson Hotel)", "lat": 40.711200, "lng": -74.006600},
#     {"id": 16, "name": "Morgan Stanley HQ", "lat": 40.758300, "lng": -73.968600},
#     {"id": 17, "name": "The St. Regis New York", "lat": 40.761600, "lng": -73.974400},
#     {"id": 18, "name": "AIG", "lat": 40.705600, "lng": -74.009100},
#     {"id": 19, "name": "Four Seasons Hotel New York Downtown", "lat": 40.712600, "lng": -74.009700},
#     {"id": 20, "name": "Bain & Company", "lat": 40.756200, "lng": -73.981100},
# ]

# # Sample 2: Random Manhattan locations with nearly identical distances
# SAMPLE2_PAIRS = [
#     {"id": 1, "origin_id": 1, "destination_id": 2},
#     {"id": 2, "origin_id": 3, "destination_id": 4},
#     {"id": 3, "origin_id": 5, "destination_id": 6},
#     {"id": 4, "origin_id": 7, "destination_id": 8},
#     {"id": 5, "origin_id": 9, "destination_id": 10},
#     {"id": 6, "origin_id": 11, "destination_id": 12},
#     {"id": 7, "origin_id": 13, "destination_id": 14},
#     {"id": 8, "origin_id": 15, "destination_id": 16},
#     {"id": 9, "origin_id": 17, "destination_id": 18},
#     {"id": 10, "origin_id": 19, "destination_id": 20},
# ]

# SAMPLE2_PLACES = [
#     {"id": 1, "name": "Random Origin 1", "lat": 40.794705, "lng": -73.971795},
#     {"id": 2, "name": "Random Destination 1", "lat": 40.739203, "lng": -74.000226},
#     {"id": 3, "name": "Random Origin 2", "lat": 40.721926, "lng": -74.003187},
#     {"id": 4, "name": "Random Destination 2", "lat": 40.711705, "lng": -74.007952},
#     {"id": 5, "name": "Random Origin 3", "lat": 40.744180, "lng": -73.998954},
#     {"id": 6, "name": "Random Destination 3", "lat": 40.782621, "lng": -73.954126},
#     {"id": 7, "name": "Random Origin 4", "lat": 40.710470, "lng": -74.007748},
#     {"id": 8, "name": "Random Destination 4", "lat": 40.750068, "lng": -73.991665},
#     {"id": 9, "name": "Random Origin 5", "lat": 40.771169, "lng": -73.957614},
#     {"id": 10, "name": "Random Destination 5", "lat": 40.786449, "lng": -73.976858},
#     {"id": 11, "name": "Random Origin 6", "lat": 40.716300, "lng": -74.004792},
#     {"id": 12, "name": "Random Destination 6", "lat": 40.747133, "lng": -74.000472},
#     {"id": 13, "name": "Random Origin 7", "lat": 40.706267, "lng": -74.012561},
#     {"id": 14, "name": "Random Destination 7", "lat": 40.726520, "lng": -73.996706},
#     {"id": 15, "name": "Random Origin 8", "lat": 40.782064, "lng": -73.956258},
#     {"id": 16, "name": "Random Destination 8", "lat": 40.737330, "lng": -73.998871},
#     {"id": 17, "name": "Random Origin 9", "lat": 40.804611, "lng": -73.954223},
#     {"id": 18, "name": "Random Destination 9", "lat": 40.749035, "lng": -73.989995},
#     {"id": 19, "name": "Random Origin 10", "lat": 40.780999, "lng": -73.946376},
#     {"id": 20, "name": "Random Destination 10", "lat": 40.748841, "lng": -73.993437},
# ]

# # Cloud Storage bucket name (you'll need to create this bucket)
# BUCKET_NAME = "bellhop-ride-data"

# def get_place_by_id(sample_places, place_id):
#     """Get a place by its ID"""
#     for place in sample_places:
#         if place['id'] == place_id:
#             return place
#     return None

# def get_prices(api_key, api_secret, pickup_lat, pickup_lng, dest_lat, dest_lng):
#     """Make API call to Bellhop to get ride prices"""
#     headers = {
#         "accept": "application/json",
#         "X-API-KEY": api_key,
#         "X-API-SECRET": api_secret,
#         "Content-Type": "application/json"
#     }
    
#     payload = {
#         "pickup": {
#             "latitude": pickup_lat,
#             "longitude": pickup_lng
#         },
#         "destination": {
#             "latitude": dest_lat,
#             "longitude": dest_lng
#         }
#     }
    
#     url = "https://api.bellhop.me/api/rich-intelligent-pricing"
    
#     try:
#         response = requests.post(url, headers=headers, json=payload)
#         response.raise_for_status()
#         return response.json()
#     except requests.exceptions.RequestException as e:
#         logger.error(f"Error fetching ride prices: {e}")
#         return None

# def save_results_to_json(data, sample_type, pair_id):
#     """Save API response to a JSON file in Google Cloud Storage"""
#     timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
#     filename = f"json/data_{sample_type}_pair{pair_id}_{timestamp}.json"
    
#     storage_client = storage.Client()
#     bucket = storage_client.bucket(BUCKET_NAME)
#     blob = bucket.blob(filename)
    
#     blob.upload_from_string(
#         json.dumps(data, indent=2),
#         content_type="application/json"
#     )
    
#     logger.info(f"JSON data saved to gs://{BUCKET_NAME}/{filename}")
#     return f"gs://{BUCKET_NAME}/{filename}"

# def parse_ride_data(data, sample_type, pickup_name, dest_name):
#     """Parse API response into CSV-ready rows"""
#     # Get the current time
#     timestamp = datetime.now()
#     date_str = timestamp.strftime("%Y-%m-%d")
#     time_str = timestamp.strftime("%H:%M:%S")
    
#     # Extract search ID
#     search_id = data.get("search_id", "")
    
#     # Parse the results
#     results = data.get("results", [])
#     if not results or not results[0].get("prices"):
#         logger.warning("No ride options to save")
#         return []
    
#     # Prepare rows for CSV
#     rows = []
#     for price in results[0].get("prices", []):
#         provider = price.get("provider", "")
#         product = price.get("product", "")
#         service_level = price.get("service_level", "")
        
#         # Price in dollars
#         price_min = price.get("price_min", 0) / 100 if price.get("price_min") is not None else 0
#         price_max = price.get("price_max", 0) / 100 if price.get("price_max") is not None else 0
        
#         # Wait time in seconds
#         wait_min = price.get("est_pickup_wait_time", {}).get("min", 0)
#         wait_max = price.get("est_pickup_wait_time", {}).get("max", 0)
        
#         # Trip details
#         trip_seconds = price.get("est_time_after_pickup_till_dropoff", 0)
#         distance_meters = price.get("distance_meters", 0)
#         surge = price.get("surge_multiplier", 1.0)
        
#         # Create a row
#         row = {
#             "date": date_str,
#             "time": time_str,
#             "search_id": search_id,
#             "sample_type": sample_type,
#             "pickup": pickup_name,
#             "destination": dest_name,
#             "provider": provider,
#             "product": product,
#             "service_level": service_level,
#             "price_min_dollars": f"{price_min:.2f}",
#             "price_max_dollars": f"{price_max:.2f}",
#             "wait_min_seconds": wait_min,
#             "wait_max_seconds": wait_max if wait_max else "",
#             "trip_seconds": trip_seconds,
#             "distance_meters": distance_meters,
#             "surge_multiplier": surge
#         }
#         rows.append(row)
    
#     return rows

# def append_to_csv_in_storage(rows):
#     """Append rows to CSV file in Google Cloud Storage"""
#     if not rows:
#         return None
    
#     # Define fieldnames (columns)
#     fieldnames = [
#         "date", "time", "search_id", "sample_type", "pickup", "destination", 
#         "provider", "product", "service_level", 
#         "price_min_dollars", "price_max_dollars", 
#         "wait_min_seconds", "wait_max_seconds", 
#         "trip_seconds", "distance_meters", "surge_multiplier"
#     ]
    
#     csv_filename = "ride_prices.csv"
    
#     # Initialize Storage client
#     storage_client = storage.Client()
#     bucket = storage_client.bucket(BUCKET_NAME)
#     blob = bucket.blob(csv_filename)
    
#     # Check if file exists
#     file_exists = blob.exists()
    
#     # Create CSV in memory
#     csv_content = ""
    
#     if not file_exists:
#         # Create new file with header
#         csv_content += ",".join(fieldnames) + "\n"
#     else:
#         # Download existing content
#         try:
#             existing_content = blob.download_as_text()
#             csv_content = existing_content
#         except Exception as e:
#             logger.error(f"Error downloading existing CSV: {e}")
#             # If error, start fresh
#             csv_content = ",".join(fieldnames) + "\n"
    
#     # Add new rows
#     for row in rows:
#         # Ensure values are properly escaped and ordered according to fieldnames
#         csv_row = [str(row.get(field, "")) for field in fieldnames]
#         csv_content += ",".join(csv_row) + "\n"
    
#     # Upload back to storage
#     try:
#         blob.upload_from_string(csv_content, content_type="text/csv")
#         logger.info(f"CSV data appended to gs://{BUCKET_NAME}/{csv_filename}")
#         return f"gs://{BUCKET_NAME}/{csv_filename}"
#     except Exception as e:
#         logger.error(f"Error appending to CSV: {e}")
#         return None

# def process_pair(api_key, api_secret, sample_type, pair, places):
#     """Process a single origin-destination pair"""
#     origin_id = pair["origin_id"]
#     dest_id = pair["destination_id"]
    
#     # Get origin and destination place details
#     origin = get_place_by_id(places, origin_id)
#     destination = get_place_by_id(places, dest_id)
    
#     if not origin or not destination:
#         logger.error(f"Invalid place IDs: origin_id={origin_id}, dest_id={dest_id}")
#         return None
    
#     # Log collection attempt
#     logger.info(f"Collecting {sample_type} - Pair {pair['id']}: {origin['name']} to {destination['name']}")
    
#     # Get price data
#     response = get_prices(
#         api_key,
#         api_secret,
#         origin["lat"],
#         origin["lng"],
#         destination["lat"],
#         destination["lng"]
#     )
    
#     if not response:
#         logger.error(f"Failed to collect data for {sample_type} - Pair {pair['id']}")
#         return None
    
#     # Save results to JSON
#     save_results_to_json(response, sample_type, pair['id'])
    
#     # Parse and prepare for CSV
#     rows = parse_ride_data(response, sample_type, origin['name'], destination['name'])
    
#     return rows

# def collect_bellhop_data(request):
#     """
#     Cloud Function entry point triggered by HTTP request or Cloud Scheduler
#     """
#     start_time = datetime.now()
#     logger.info(f"Starting data collection cycle at {start_time}")
    
#     # Get API credentials from environment variables
#     api_key = os.environ.get("BELLHOP_API_KEY")
#     api_secret = os.environ.get("BELLHOP_API_SECRET")
    
#     if not api_key or not api_secret:
#         error_msg = "Error: API credentials not found in environment variables"
#         logger.error(error_msg)
#         return error_msg, 500
    
#     all_csv_rows = []
    
#     # Process Sample 1 - Prestigious routes
#     for pair in SAMPLE1_PAIRS:
#         rows = process_pair(api_key, api_secret, "Sample1", pair, SAMPLE1_PLACES)
#         if rows:
#             all_csv_rows.extend(rows)
#         time.sleep(1)  # Small delay between calls
    
#     # Process Sample 2 - Random routes with similar distances
#     for pair in SAMPLE2_PAIRS:
#         rows = process_pair(api_key, api_secret, "Sample2", pair, SAMPLE2_PLACES)
#         if rows:
#             all_csv_rows.extend(rows)
#         time.sleep(1)  # Small delay between calls
    
#     # Append all data to CSV
#     if all_csv_rows:
#         append_to_csv_in_storage(all_csv_rows)
    
#     end_time = datetime.now()
#     duration = (end_time - start_time).total_seconds()
#     logger.info(f"Completed data collection cycle in {duration:.2f} seconds")
    
#     return f"Successfully collected data for {len(all_csv_rows)} ride options", 200