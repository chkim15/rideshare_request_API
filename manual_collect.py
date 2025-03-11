#!/usr/bin/env python3
"""
Simplified manual collection script to avoid import issues.
This script directly uses the API client without complex imports.
"""
import os
import json
import time
from datetime import datetime
from dotenv import load_dotenv
import requests

# Define popular places that can be used for pickup or destination
POPULAR_PLACES = [
    {"id": 1, "name": "Times Square", "lat": 40.758896, "lng": -73.985130},
    {"id": 2, "name": "Empire State Building", "lat": 40.748817, "lng": -73.985428},
    {"id": 3, "name": "Grand Central Terminal", "lat": 40.7527, "lng": -73.9772},
    {"id": 4, "name": "JFK Airport", "lat": 40.6413, "lng": -73.7781},
    {"id": 5, "name": "LaGuardia Airport", "lat": 40.7769, "lng": -73.8740},
    {"id": 6, "name": "Central Park", "lat": 40.7812, "lng": -73.9665},
    {"id": 7, "name": "Wall Street", "lat": 40.7068, "lng": -74.0089},
    {"id": 8, "name": "Brooklyn Bridge", "lat": 40.7061, "lng": -73.9969},
    {"id": 9, "name": "Williamsburg", "lat": 40.7081, "lng": -73.9571},
    {"id": 10, "name": "Hudson Yards", "lat": 40.7539, "lng": -74.0024}
]

def get_prices(api_key, api_secret, pickup_lat, pickup_lng, dest_lat, dest_lng):
    """
    Direct API call to Bellhop without using the API client class
    """
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
        print(f"Error fetching ride prices: {e}")
        return None

def save_results_to_file(data, route_name):
    """Save API response to a JSON file"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"data_{route_name.replace(' ', '_')}_{timestamp}.json"
    
    os.makedirs("data", exist_ok=True)
    filepath = os.path.join("data", filename)
    
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"Data saved to {filepath}")
    return filepath

def display_ride_options(response_data):
    """Display ride options in a readable format"""
    if not response_data:
        print("No data available")
        return
    
    results = response_data.get("results", [])
    if not results or not results[0].get("prices"):
        print("No ride options available")
        return
    
    # Group by provider
    providers = {}
    for price in results[0].get("prices", []):
        provider = price.get("provider")
        if provider not in providers:
            providers[provider] = []
        providers[provider].append(price)
    
    # Sort each provider's options by price
    for provider, options in providers.items():
        options.sort(key=lambda x: x.get("price_min", 0))
    
    # Display results by provider
    for provider, options in providers.items():
        print(f"\n{provider} OPTIONS:")
        print("-" * 80)
        print(f"{'PRODUCT':<20} {'PRICE':<15} {'WAIT TIME':<20} {'TRIP TIME':<15} {'DISTANCE':<10}")
        print("-" * 80)
        
        for option in options:
            product = option.get("product", "Unknown")
            
            # Format price in dollars
            price_min = option.get("price_min", 0) / 100 if option.get("price_min") else 0
            price = f"${price_min:.2f}"
            
            # Format wait time
            wait_seconds = option.get("est_pickup_wait_time", {}).get("min", 0)
            wait_minutes = wait_seconds // 60 if wait_seconds else 0
            wait_time = f"{wait_minutes} min"
            
            # Format trip time
            trip_seconds = option.get("est_time_after_pickup_till_dropoff", 0)
            trip_minutes = trip_seconds / 60 if trip_seconds else 0
            trip_time = f"{trip_minutes:.1f} min"
            
            # Format distance
            distance_meters = option.get("distance_meters", 0)
            distance = f"{distance_meters*0.000621371:.2f} miles"
            
            print(f"{product:<20} {price:<15} {wait_time:<20} {trip_time:<15} {distance:<10}")
    
    print(f"\nSearch ID: {response_data.get('search_id')}")
    print(f"Timestamp: {response_data.get('timestamp')}")

def display_places():
    """Display all available popular places"""
    print("\nAvailable Popular Places:")
    print("-" * 60)
    print(f"{'ID':<4} {'NAME':<25} {'COORDINATES'}")
    print("-" * 60)
    for place in POPULAR_PLACES:
        print(f"{place['id']:<4} {place['name']:<25} ({place['lat']:.6f}, {place['lng']:.6f})")

def get_place_by_id(place_id):
    """Get a place by its ID"""
    for place in POPULAR_PLACES:
        if place['id'] == place_id:
            return place
    return None

def main():
    """Main function for manual data collection"""
    # Load environment variables
    load_dotenv()
    
    # Get API credentials
    api_key = os.environ.get("BELLHOP_API_KEY")
    api_secret = os.environ.get("BELLHOP_API_SECRET")
    
    if not api_key or not api_secret:
        print("Error: API credentials not found in .env file")
        return
    
    # Welcome message
    print("\n===== Bellhop Ride Price Manual Collection Tool =====")
    
    while True:
        print("\nSelect an option:")
        print("1. Select from popular places")
        print("2. Enter custom coordinates")
        print("3. Exit")
        
        choice = input("\nEnter your choice (1-3): ")
        
        if choice == "1":
            # Display available places
            display_places()
            
            # Get pickup location
            pickup_id = input("\nEnter ID for pickup location: ")
            try:
                pickup_id = int(pickup_id)
                pickup = get_place_by_id(pickup_id)
                if not pickup:
                    print("Invalid pickup ID")
                    continue
            except ValueError:
                print("Invalid input. Please enter a number.")
                continue
            
            # Get destination location
            dest_id = input("Enter ID for destination location: ")
            try:
                dest_id = int(dest_id)
                destination = get_place_by_id(dest_id)
                if not destination:
                    print("Invalid destination ID")
                    continue
            except ValueError:
                print("Invalid input. Please enter a number.")
                continue
            
        elif choice == "2":
            # Enter custom coordinates
            try:
                pickup_lat = float(input("Enter pickup latitude: "))
                pickup_lng = float(input("Enter pickup longitude: "))
                dest_lat = float(input("Enter destination latitude: "))
                dest_lng = float(input("Enter destination longitude: "))
                
                pickup = {
                    "lat": pickup_lat, 
                    "lng": pickup_lng, 
                    "name": f"Custom ({pickup_lat}, {pickup_lng})"
                }
                destination = {
                    "lat": dest_lat, 
                    "lng": dest_lng, 
                    "name": f"Custom ({dest_lat}, {dest_lng})"
                }
            except ValueError:
                print("Invalid coordinates. Please enter valid numbers.")
                continue
            
        elif choice == "3":
            # Exit
            print("Exiting...")
            break
            
        else:
            print("Invalid choice. Please try again.")
            continue
        
                    # Fetch ride prices
        pickup_name = pickup.get('name')
        dest_name = destination.get('name')
        print(f"\nFetching ride prices from {pickup_name} to {dest_name}...")
        response = get_prices(
            api_key,
            api_secret,
            pickup["lat"],
            pickup["lng"],
            destination["lat"],
            destination["lng"]
        )
        
        if not response:
            print("Error fetching ride prices")
            continue
        
        # Display results
        display_ride_options(response)
        
        # Ask what to do with the results
        print("\nWhat would you like to do with these results?")
        print("1. Save to JSON file")
        print("2. Nothing (continue)")
        
        action = input("\nEnter your choice (1-2): ")
        
        if action == "1":
            # Save to JSON file
            if choice == "1":
                route_name = f"{pickup['name']}_to_{destination['name']}"
            else:
                route_name = "custom_route"
            save_results_to_file(response, route_name)
        
        # Ask if user wants to continue
        again = input("\nLook up another route? (y/n): ")
        if again.lower() != 'y':
            break
    
    print("\nThank you for using the Bellhop manual collection tool!")

if __name__ == "__main__":
    main()