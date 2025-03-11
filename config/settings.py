import os
from pathlib import Path

# Project paths
BASE_DIR = Path(__file__).resolve().parent.parent
CREDENTIALS_DIR = os.path.join(BASE_DIR, "config", "credentials")

# Bellhop API settings
BELLHOP_API_KEY = os.environ.get("BELLHOP_API_KEY", "")
BELLHOP_API_SECRET = os.environ.get("BELLHOP_API_SECRET", "")
BELLHOP_API_URL = "https://api.bellhop.me/api/rich-intelligent-pricing"

# Google Cloud settings
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", None)
GCP_CREDENTIALS_FILE = os.environ.get("GCP_CREDENTIALS_FILE", None)
BIGQUERY_DATASET_ID = os.environ.get("BIGQUERY_DATASET_ID", "ride_pricing")
BIGQUERY_TABLE_ID = os.environ.get("BIGQUERY_TABLE_ID", "price_comparisons")

# Default locations for testing (coordinates for some NYC landmarks)
DEFAULT_LOCATIONS = {
    "times square": (40.758896, -73.985130, "Times Square, New York, NY"),
    "empire state": (40.748817, -73.985428, "Empire State Building, New York, NY"),
    "grand central": (40.7527, -73.9772, "Grand Central Terminal, New York, NY"),
    "williamsburg": (40.7081, -73.9571, "Williamsburg, Brooklyn, NY"),
    "bushwick": (40.6958, -73.9171, "Bushwick, Brooklyn, NY"),
    "jfk": (40.6413, -73.7781, "JFK Airport, Queens, NY"),
    "laguardia": (40.7769, -73.8740, "LaGuardia Airport, Queens, NY"),
    "central park": (40.7812, -73.9665, "Central Park, New York, NY"),
    "brooklyn bridge": (40.7061, -73.9969, "Brooklyn Bridge, New York, NY"),
}