# Obi to BigQuery

A tool to fetch Uber and Lyft pricing from the Obi API and store results in Google BigQuery for analysis.

## Setup

1. Clone this repository
2. Create a virtual environment: `python3 -m venv obi`
3. Activate the environment: `source obi/bin/activate`
4. Install dependencies: `pip install -r requirements.txt`
5. Configure your Google Cloud credentials
6. Run the application: `python src/main.py`

## Features

- Fetch ride pricing from multiple providers via Obi API
- Store complete pricing data in BigQuery
- Simple command-line interface for quick price checks
- Historical price tracking for future analysis