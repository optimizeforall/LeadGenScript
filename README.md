# Business Lead Generator

## Description
This Python-based tool efficiently searches for businesses across U.S. cities and states using the Google Places API. It's designed to generate leads for various business types, with customizable search options and optimized performance.

## Features
- Search for businesses in capital cities of all states
- Search in major cities across all states
- Search within a specific state
- Concurrent API requests for faster processing
- CSV output of business leads including name, phone, website, location, rating, and review count

## Installation
1. Clone the repository:
   ```
   git clone https://github.com/yourusername/business-lead-generator.git
   ```
2. Navigate to the project directory:
   ```
   cd business-lead-generator
   ```
3. Make the script executable:
   ```
   chmod +x main.py
   ```
4. Set up your Google Places API key as an environment variable:
   ```
   export GOOGLE_PLACES_API_KEY='your_api_key_here'
   ```

## Usage
Run the script with one of the following options:

1. Search in capital cities of all states:
   ```
   ./main.py --all-states "Business Type"
   ```

2. Search in major cities across all states:
   ```
   ./main.py --everything "Business Type"
   ```

3. Search within a specific state:
   ```
   ./main.py --state NY "Business Type"
   ```

Replace "Business Type" with your target business category (e.g., "Roofing", "Plumbing").

## Output
The script generates a CSV file named `leads_[BusinessType]_[Timestamp].csv` containing the collected business information.

## Note
Ensure compliance with Google Places API terms of service and respect rate limits to avoid service interruptions.