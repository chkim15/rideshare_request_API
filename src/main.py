#!/usr/bin/env python3
"""
Bellhop to BigQuery CLI application
"""
import os
import argparse
import json
from dotenv import load_dotenv
from src.api import BellhopAPI
from src.storage import BigQueryStorage

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Fetch ride prices and save to BigQuery')
    parser.add_argument('--pickup-lat', type=float, required=True, help='Pickup latitude')
    parser.add_argument('--pickup-lng', type=float, required=True, help='Pickup longitude')
    parser.add_argument('--dest-lat', type=float, required=True, help='Destination latitude')
    parser.add_argument('--dest-lng', type=float, required=True, help='Destination longitude')
    parser.add_argument('--print-response', action='store_true', help='Print API response')
    parser.add_argument('--save', action='store_true', help='Save results to BigQuery')
    return parser.parse_args()

def display_results(response_data):
    """Display ride pricing results in a readable format"""
    if not response_data:
        print("No results available")
        return

    print("\n=== AVAILABLE RIDES ===")
    print("=" * 70)
    
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
        print("-" * 70)
        print(f"{'PRODUCT':<20} {'PRICE':<15} {'WAIT TIME':<15} {'TRIP TIME':<15}")
        print("-" * 70)
        
        for option in options:
            product = option.get("product", "Unknown")
            
            # Format price in dollars
            price_min = option.get("price_min", 0) / 100 if option.get("price_min") else 0
            price = f"${price_min:.2f}"
            
            # Format wait time
            wait_seconds = option.get("est_pickup_wait_time", {}).get("min", "?")
            wait_time = f"{wait_seconds} seconds"
            
            # Format trip time
            trip_seconds = option.get("est_time_after_pickup_till_dropoff", 0)
            trip_time = f"{trip_seconds} seconds"
            
            print(f"{product:<20} {price:<15} {wait_time:<15} {trip_time:<15}")

def main():
    """Main application function"""
    # Load environment variables
    load_dotenv()
    
    # Parse command line arguments
    args = parse_args()
    
    # Initialize API client
    api_client = BellhopAPI(
        api_key=os.environ.get("BELLHOP_API_KEY"),
        api_secret=os.environ.get("BELLHOP_API_SECRET")
    )
    
    # Fetch ride prices
    print(f"Fetching ride prices from ({args.pickup_lat}, {args.pickup_lng}) to ({args.dest_lat}, {args.dest_lng})...")
    response = api_client.get_prices(
        pickup_lat=args.pickup_lat, 
        pickup_lng=args.pickup_lng,
        dest_lat=args.dest_lat,
        dest_lng=args.dest_lng
    )
    
    # Print raw response if requested
    if args.print_response:
        print("\nRaw API response:")
        print(json.dumps(response, indent=2))
    
    # Display formatted results
    display_results(response)
    
    # Save to BigQuery if requested
    if args.save:
        storage = BigQueryStorage()
        storage.save(
            response,
            pickup_lat=args.pickup_lat,
            pickup_lng=args.pickup_lng,
            dest_lat=args.dest_lat,
            dest_lng=args.dest_lng
        )
        print("\nResults saved to BigQuery successfully!")

if __name__ == "__main__":
    main()