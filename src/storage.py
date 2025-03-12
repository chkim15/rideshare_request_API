"""
BigQuery storage module for ride price data
"""
import json
import datetime
from google.cloud import bigquery

class BigQueryStorage:
    """BigQuery storage for ride price data"""
    
    def __init__(self, dataset_id="ride_pricing", table_id="price_comparisons"):
        """
        Initialize BigQuery storage
        
        Args:
            dataset_id (str): BigQuery dataset ID
            table_id (str): BigQuery table ID
        """
        self.client = bigquery.Client()
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.table_ref = f"{self.client.project}.{dataset_id}.{table_id}"
        
        # Ensure dataset and table exist
        self._setup()
    
    def _setup(self):
        """Set up BigQuery dataset and table if they don't exist"""
        try:
            # Try to get dataset (creates if it doesn't exist)
            try:
                self.client.get_dataset(self.dataset_id)
            except Exception:
                dataset = bigquery.Dataset(f"{self.client.project}.{self.dataset_id}")
                dataset.location = "US"
                self.client.create_dataset(dataset, exists_ok=True)
                print(f"Created dataset: {self.dataset_id}")
            
            # Try to get table (creates if it doesn't exist)
            try:
                self.client.get_table(self.table_ref)
            except Exception:
                schema = [
                    bigquery.SchemaField("request_timestamp", "TIMESTAMP"),
                    bigquery.SchemaField("pickup_lat", "FLOAT"),
                    bigquery.SchemaField("pickup_lng", "FLOAT"),
                    bigquery.SchemaField("destination_lat", "FLOAT"),
                    bigquery.SchemaField("destination_lng", "FLOAT"),
                    bigquery.SchemaField("search_id", "STRING"),
                    bigquery.SchemaField("raw_response", "STRING"),
                    bigquery.SchemaField("ride_options", "RECORD", mode="REPEATED", fields=[
                        bigquery.SchemaField("provider", "STRING"),
                        bigquery.SchemaField("product", "STRING"),
                        bigquery.SchemaField("service_level", "STRING"),
                        bigquery.SchemaField("price_min_cents", "INTEGER"),
                        bigquery.SchemaField("price_max_cents", "INTEGER"),
                        bigquery.SchemaField("currency", "STRING"),
                        bigquery.SchemaField("wait_time_min", "INTEGER"),
                        bigquery.SchemaField("wait_time_max", "INTEGER"),
                        bigquery.SchemaField("trip_time_seconds", "INTEGER"),
                        bigquery.SchemaField("distance_meters", "INTEGER"),
                        bigquery.SchemaField("surge_multiplier", "FLOAT")
                    ])
                ]
                
                table = bigquery.Table(self.table_ref, schema=schema)
                self.client.create_table(table, exists_ok=True)
                print(f"Created table: {self.table_ref}")
                
        except Exception as e:
            print(f"Error setting up BigQuery: {e}")
    
    def save(self, response_data, pickup_lat, pickup_lng, dest_lat, dest_lng):
        """
        Save ride price data to BigQuery
        
        Args:
            response_data (dict): API response data
            pickup_lat (float): Pickup latitude
            pickup_lng (float): Pickup longitude
            dest_lat (float): Destination latitude
            dest_lng (float): Destination longitude
        
        Returns:
            bool: True if successful, False otherwise
        """
        if not response_data or "results" not in response_data:
            print("Invalid response data")
            return False
        
        try:
            # Extract ride options
            ride_options = []
            for result in response_data.get("results", []):
                for price in result.get("prices", []):
                    option = {
                        "provider": price.get("provider"),
                        "product": price.get("product"),
                        "service_level": price.get("service_level"),
                        "price_min_cents": price.get("price_min"),
                        "price_max_cents": price.get("price_max"),
                        "currency": price.get("currency"),
                        "wait_time_min": price.get("est_pickup_wait_time", {}).get("min"),
                        "wait_time_max": price.get("est_pickup_wait_time", {}).get("max"),
                        "trip_time_seconds": price.get("est_time_after_pickup_till_dropoff"),
                        "distance_meters": price.get("distance_meters"),
                        "surge_multiplier": price.get("surge_multiplier")
                    }
                    ride_options.append(option)
            
            # Prepare row for insertion
            row = {
                "request_timestamp": datetime.datetime.now().isoformat(),
                "pickup_lat": pickup_lat,
                "pickup_lng": pickup_lng,
                "destination_lat": dest_lat,
                "destination_lng": dest_lng,
                "search_id": response_data.get("search_id"),
                "raw_response": json.dumps(response_data),
                "ride_options": ride_options
            }
            
            # Insert into BigQuery
            errors = self.client.insert_rows_json(self.table_ref, [row])
            
            if errors:
                print(f"Errors inserting row: {errors}")
                return False
            
            return True
            
        except Exception as e:
            print(f"Error saving to BigQuery: {e}")
            return False