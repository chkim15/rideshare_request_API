#!/usr/bin/env python3
"""
Simple script to check ride prices between common locations
"""
import os
from dotenv import load_dotenv
from src.api import BellhopAPI
from src.utils.coordinates import get_location, print_available_locations

def main():
    """Main function"""
    # Load environment variables
    load_dotenv()
    
    # Get API credentials
    api_key = os.environ.get("BELLHOP_API_KEY")
    api_secret = os.environ.get("BELLHOP_API_SECRET")
    
    if not api_key or not api_secret:
        print("Error: API credentials not found in .env file")
        return
    
    # Initialize API client
    api_client = BellhopAPI(api_key=api_key, api_secret=api_secret)
    
    # Show available locations
    print("\n==== Bellhop Ride Price Checker ====\n")
    print_available_locations()
    
    # Get user input for pickup and destination
    print("\nEnter location names or coordinates:")
    
    # Get pickup location
    pickup_input = input("\nPickup location (name or 'lat,lng'): ")
    pickup = None
    
    if "," in pickup_input:
        # Parse coordinates
        try:
            lat, lng = map(float, pickup_input.split(","))
            pickup = {"lat": lat, "lng": lng, "name": f"Custom ({lat}, {lng})"}
        except ValueError:
            print("Invalid coordinates format. Use 'latitude,longitude'")
            return
    else:
        # Get named location
        pickup = get_location(pickup_input)
    
    if not pickup:
        print("Invalid pickup location")
        return
    
    # Get destination location
    dest_input = input("Destination location (name or 'lat,lng'): ")
    dest = None
    
    if "," in dest_input:
        # Parse coordinates
        try:
            lat, lng = map(float, dest_input.split(","))
            dest = {"lat": lat, "lng": lng, "name": f"Custom ({lat}, {lng})"}
        except ValueError:
            print("Invalid coordinates format. Use 'latitude,longitude'")
            return
    else:
        # Get named location
        dest = get_location(dest_input)
    
    if not dest:
        print("Invalid destination location")
        return
    
    # Fetch ride prices
    print(f"\nFetching ride prices from {pickup['name']} to {dest['name']}...")
    response = api_client.get_prices(
        pickup_lat=pickup["lat"],
        pickup_lng=pickup["lng"],
        dest_lat=dest["lat"],
        dest_lng=dest["lng"]
    )
    
    if not response:
        print("Error fetching ride prices")
        return
    
    # Display results
    print("\n=== AVAILABLE RIDES ===")
    print("=" * 70)
    
    results = response.get("results", [])
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
        print("-" * 70)
        print(f"{'PRODUCT':<20} {'PRICE':<15} {'WAIT TIME':<15} {'TRIP TIME':<15}")
        print("-" * 70)
        
        for option in options:
            product = option.get("product", "Unknown")
            
            # Format price in dollars
            price_min = option.get("price_min", 0) / 100 if option.get("price_min") else 0
            price = f"${price_min:.2f}"
            
            # Format wait time
            wait_min = option.get("est_pickup_wait_time", {}).get("min", "?")
            wait_time = f"{wait_min} min"
            
            # Format trip time
            trip_seconds = option.get("est_time_after_pickup_till_dropoff", 0)
            trip_minutes = trip_seconds // 60 if trip_seconds else 0
            trip_time = f"{trip_minutes} min"
            
            print(f"{product:<20} {price:<15} {wait_time:<15} {trip_time:<15}")
    
    # Ask if user wants to save to BigQuery
    save = input("\nSave results to BigQuery? (y/n): ")
    if save.lower() == "y":
        try:
            from src.storage import BigQueryStorage
            storage = BigQueryStorage()
            storage.save(
                response,
                pickup_lat=pickup["lat"],
                pickup_lng=pickup["lng"],
                dest_lat=dest["lat"],
                dest_lng=dest["lng"]
            )
            print("Results saved to BigQuery successfully!")
        except Exception as e:
            print(f"Error saving to BigQuery: {e}")

if __name__ == "__main__":
    main()