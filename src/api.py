"""
Bellhop API client module
"""
import requests

class BellhopAPI:
    """Bellhop API client for fetching ride pricing"""
    
    def __init__(self, api_key, api_secret):
        """
        Initialize the Bellhop API client
        
        Args:
            api_key (str): Bellhop API key
            api_secret (str): Bellhop API secret
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self.endpoint = "https://api.bellhop.me/api/rich-intelligent-pricing"
    
    def get_prices(self, pickup_lat, pickup_lng, dest_lat, dest_lng):
        """
        Fetch ride prices from Bellhop API
        
        Args:
            pickup_lat (float): Pickup latitude
            pickup_lng (float): Pickup longitude
            dest_lat (float): Destination latitude
            dest_lng (float): Destination longitude
        
        Returns:
            dict: API response containing ride pricing data
        """
        headers = {
            "accept": "application/json",
            "X-API-KEY": self.api_key,
            "X-API-SECRET": self.api_secret,
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
        
        try:
            response = requests.post(self.endpoint, headers=headers, json=payload)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching ride prices: {e}")
            return None