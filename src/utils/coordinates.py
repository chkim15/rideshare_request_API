"""
Coordinate helper utilities
"""

# Common locations to use as examples
LOCATIONS = {
    # New York City
    "times_square": {"lat": 40.758896, "lng": -73.985130, "name": "Times Square"},
    "empire_state": {"lat": 40.748817, "lng": -73.985428, "name": "Empire State Building"},
    "grand_central": {"lat": 40.7527, "lng": -73.9772, "name": "Grand Central Terminal"},
    "jfk_airport": {"lat": 40.6413, "lng": -73.7781, "name": "JFK Airport"},
    "laguardia": {"lat": 40.7769, "lng": -73.8740, "name": "LaGuardia Airport"},
    "central_park": {"lat": 40.7812, "lng": -73.9665, "name": "Central Park"},
    
    # San Francisco
    "sf_ferry_building": {"lat": 37.7955, "lng": -122.3937, "name": "SF Ferry Building"},
    "golden_gate": {"lat": 37.8199, "lng": -122.4783, "name": "Golden Gate Bridge"},
    "sfo_airport": {"lat": 37.6213, "lng": -122.3790, "name": "SFO Airport"},
    
    # Los Angeles
    "lax_airport": {"lat": 33.9416, "lng": -118.4085, "name": "LAX Airport"},
    "hollywood_sign": {"lat": 34.1341, "lng": -118.3215, "name": "Hollywood Sign"},
    "santa_monica_pier": {"lat": 34.0099, "lng": -118.4960, "name": "Santa Monica Pier"}
}

def get_location(name):
    """
    Get coordinates for a named location
    
    Args:
        name (str): Location name (key in LOCATIONS dictionary)
        
    Returns:
        dict: Location dictionary with lat, lng, and name or None if not found
    """
    return LOCATIONS.get(name.lower().replace(" ", "_"))

def print_available_locations():
    """Print all available named locations"""
    print("Available locations:")
    for key, location in LOCATIONS.items():
        print(f"  {key}: {location['name']} ({location['lat']}, {location['lng']})")