import requests
import json
import csv
import time
import concurrent.futures
from datetime import datetime, timedelta
from colorama import Fore, Style, init

# API key for Google Places API (replace with your own)
API_KEY = 'AIzaSyA-YMXLi1Er6R_-iL1VncrDUyPa3erKEU4'

def search_businesses(location):
    """
    Search for businesses using Google Places API.
    
    Args:
    location (str): Location to search in
    
    Returns:
    list: List of dictionaries containing business information
    """
    businesses = []
    next_page_token = None
    page_count = 0
    business_type = "Lighting and Holiday"
    query = f"{business_type}"
    
    while True:
        page_count += 1
        
        # Construct the URL for the Places API text search
        url = f"https://maps.googleapis.com/maps/api/place/textsearch/json?query={query}+in+{location}&key={API_KEY}"
        
        # Add page token if available for pagination
        if next_page_token:
            url += f"&pagetoken={next_page_token}"
        
        # Make the API request
        response = requests.get(url)
        results = response.json()
        
        # Check for API errors
        if 'error_message' in results:
            break
        
        # Process each business result
        for result in results['results']:
            business = {
                'name': result['name'],
                'address': result.get('formatted_address', 'N/A'),
                'rating': result.get('rating', 'N/A'),
                'phone': 'N/A',
                'website': 'N/A',
                'city': location.split(',')[0].strip(),
                'state': location.split(',')[1].strip()
            }
            
            # Get additional details (phone and website) using Place Details API
            place_id = result['place_id']
            details_url = f"https://maps.googleapis.com/maps/api/place/details/json?place_id={place_id}&fields=formatted_phone_number,website&key={API_KEY}"
            details_response = requests.get(details_url)
            details_results = details_response.json()
            
            if 'result' in details_results:
                business['phone'] = details_results['result'].get('formatted_phone_number', 'N/A')
                business['website'] = details_results['result'].get('website', 'N/A')
            
            businesses.append(business)
        
        # Check for more pages of results
        next_page_token = results.get('next_page_token')
        
        if not next_page_token:
            break
        
        # Wait before making the next request (API restriction)
        time.sleep(2)
    
    return businesses

def save_to_csv(businesses, filename):
    """
    Save the list of businesses to a CSV file.
    
    Args:
    businesses (list): List of dictionaries containing business information
    filename (str): Name of the file to save the data to
    """
    with open(filename, 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=['name', 'address', 'rating', 'phone', 'website', 'city', 'state'])
        writer.writeheader()
        writer.writerows(businesses)

from datetime import datetime
from colorama import Fore, Style, init
import concurrent.futures

def main():
    """
    Main function to run the business search and data saving process.
    """
    init(autoreset=True)  # Initialize colorama
    start_time = datetime.now()

    major_cities = [
        "New York, NY", "Los Angeles, CA", "Chicago, IL", "Houston, TX", "Phoenix, AZ",
        "Philadelphia, PA", "San Antonio, TX", "San Diego, CA", "Dallas, TX", "San Jose, CA",
        "Austin, TX", "Jacksonville, FL", "Fort Worth, TX", "Columbus, OH", "San Francisco, CA",
        "Charlotte, NC", "Indianapolis, IN", "Seattle, WA", "Denver, CO", "Washington, DC"
    ]

    all_businesses = []

    print(f"{Style.BRIGHT}{Fore.CYAN}{'City':<20} {'Leads':<8} {'Runtime':<15} {'Progress'}")
    print(f"{Style.BRIGHT}{Fore.CYAN}{'-'*55}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_city = {executor.submit(search_businesses, city): city for city in major_cities}
        
        for i, future in enumerate(concurrent.futures.as_completed(future_to_city), 1):
            city = future_to_city[future]
            try:
                businesses = future.result()
                all_businesses.extend(businesses)
                runtime = datetime.now() - start_time
                
                if len(businesses) > 20:
                    color = Fore.GREEN
                elif len(businesses) > 0:
                    color = Fore.BLUE
                else:
                    color = Fore.RED
                
                print(f"{color}{city:<20} {len(businesses):<8} {str(runtime):<15} {i}/{len(major_cities)}")
            except Exception as exc:
                print(f"{Fore.RED}{city:<20} {'ERROR':<8} {str(runtime):<15} {i}/{len(major_cities)}")

    filename = "leads.csv"
    save_to_csv(all_businesses, filename)
    
    total_runtime = datetime.now() - start_time
    print(f"\n{Style.BRIGHT}{Fore.YELLOW}Total runtime: {total_runtime}")
    print(f"{Style.BRIGHT}{Fore.YELLOW}Total businesses: {len(all_businesses)}")
    print(f"{Style.BRIGHT}{Fore.YELLOW}Data saved to: {filename}")

if __name__ == "__main__":
    main()