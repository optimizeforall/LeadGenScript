import requests
import json
import csv
import time
import concurrent.futures
import sys
from datetime import datetime
from colorama import Fore, Style, init
from geopy.geocoders import Nominatim

# API key for Google Places API (replace with your own)
API_KEY = 'AIzaSyA-YMXLi1Er6R_-iL1VncrDUyPa3erKEU4'

# Constants
BUSINESS_TYPE = "Lighting and Holiday"
MAX_RETRIES = 3
BACKOFF_TIME = 2

def get_location_coordinates(location):
    geolocator = Nominatim(user_agent="my_app")
    try:
        location_data = geolocator.geocode(location)
        if location_data:
            return f"{location_data.latitude},{location_data.longitude}"
    except Exception as e:
        print(f"Error getting coordinates for {location}: {str(e)}")
    return None

def get_phone_number(place_id):
    url = f"https://maps.googleapis.com/maps/api/place/details/json?place_id={place_id}&fields=formatted_phone_number&key={API_KEY}"
    response = requests.get(url)
    result = response.json().get('result', {})
    return result.get('formatted_phone_number')

def search_businesses(location):
    """
    Search for businesses using Google Places API.
    
    Args:
    location (str): Location to search in
    
    Returns:
    list: List of dictionaries containing business information
    int: Number of skipped businesses
    """
    businesses = []
    skipped_businesses = 0
    next_page_token = None
    page_count = 0
    query = f"{BUSINESS_TYPE}"
    
    while True:
        page_count += 1
        
        # Construct the URL for the Places API text search
        url = f"https://maps.googleapis.com/maps/api/place/textsearch/json?query={query}+in+{location}&key={API_KEY}"
        
        if next_page_token:
            url += f"&pagetoken={next_page_token}"
        
        # Make the API request with retries
        for attempt in range(MAX_RETRIES):
            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                results = response.json()
                break
            except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
                if attempt == MAX_RETRIES - 1:
                    print(f"\nError searching {location}: {str(e)}")
                    return businesses, skipped_businesses
                time.sleep(BACKOFF_TIME * (attempt + 1))
        
        if 'error_message' in results:
            print(f"\nAPI Error for {location}: {results['error_message']}")
            break
        
        for result in results['results']:
            phone_number = get_phone_number(result.get('place_id'))
            if phone_number:
                business = {
                    'NAME': result['name'],
                    'PHONE': phone_number,
                    'WEBSITE': 'N/A',
                    'CITY/STATE': f"{location.split(',')[0].strip()}, {location.split(',')[1].strip()}",
                    'RATING': result.get('rating', 'N/A'),
                    'REVIEWS': result.get('user_ratings_total', 'N/A')
                }
                
                # Get additional details (website) using Place Details API
                place_id = result['place_id']
                details_url = f"https://maps.googleapis.com/maps/api/place/details/json?place_id={place_id}&fields=website&key={API_KEY}"
                
                for attempt in range(MAX_RETRIES):
                    try:
                        details_response = requests.get(details_url, timeout=10)
                        details_response.raise_for_status()
                        details_results = details_response.json()
                        break
                    except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
                        if attempt == MAX_RETRIES - 1:
                            print(f"\nError getting details for {business['NAME']} in {location}: {str(e)}")
                            break
                        time.sleep(BACKOFF_TIME * (attempt + 1))
                if 'result' in details_results:
                    business['WEBSITE'] = details_results['result'].get('website', 'N/A')
                
                businesses.append(business)
            else:
                skipped_businesses += 1
        
        next_page_token = results.get('next_page_token')
        if not next_page_token:
            break
        
        time.sleep(2)  # Delay to avoid hitting API rate limits
    
    return businesses, skipped_businesses

def save_to_csv(businesses, filename):
    """
    Save the list of businesses to a CSV file.
    
    Args:
    businesses (list): List of dictionaries containing business information
    filename (str): Name of the file to save the data to
    """
    with open(filename, 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=['NAME', 'PHONE', 'WEBSITE', 'CITY/STATE', 'RATING', 'REVIEWS'])
        writer.writeheader()
        writer.writerows(businesses)

def main():
    """
    Main function to run the business search and data saving process.
    """
    init(autoreset=True)  # Initialize colorama
    start_time = datetime.now()

    major_cities = [
        "New York, NY", "Los Angeles, CA", "Chicago, IL", "Houston, TX", "Phoenix, AZ",
    ]

    all_businesses = []
    businesses_without_numbers = 0

    print(f"{Style.BRIGHT}{Fore.CYAN}{'City, State':<20} {'Leads':<8} {'Avg Rating':<12} {'Total Reviews':<15} {'Runtime':<15} {'Progress'}")
    print(f"{Style.BRIGHT}{Fore.CYAN}{'-'*80}")

    completed_cities = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_city = {executor.submit(search_businesses, city): city for city in major_cities}
        
        for future in concurrent.futures.as_completed(future_to_city):
            city = future_to_city[future]
            try:
                businesses, skipped_businesses = future.result()
                all_businesses.extend(businesses)
                runtime = datetime.now() - start_time
                completed_cities += 1
                
                businesses_with_numbers = [b for b in businesses if b['PHONE'] != 'N/A']
                businesses_without_numbers += skipped_businesses
                
                avg_rating = sum(float(b['RATING']) for b in businesses_with_numbers if b['RATING'] != 'N/A') / len(businesses_with_numbers) if businesses_with_numbers else 0
                total_reviews = sum(int(b['REVIEWS']) for b in businesses_with_numbers if b['REVIEWS'] != 'N/A')
                
                if len(businesses_with_numbers) > 20:
                    color = Fore.GREEN
                elif len(businesses_with_numbers) > 0:
                    color = Fore.BLUE
                else:
                    color = Fore.RED
                
                print(f"{color}{city:<20} {len(businesses_with_numbers):<8} {avg_rating:.2f}        {total_reviews:<15} {str(runtime):<15} {completed_cities}/{len(major_cities)}")
            except Exception as exc:
                print(f"{Fore.RED}{city:<20} {'ERROR':<8} {'N/A':<12} {'N/A':<15} {str(runtime):<15} {completed_cities}/{len(major_cities)}")
                print(f"Error details: {str(exc)}")

    filename = f"leads_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    save_to_csv(all_businesses, filename)
    
    total_runtime = datetime.now() - start_time
    print(f"\n{Style.BRIGHT}{Fore.YELLOW}Total runtime: {total_runtime}")
    print(f"{Style.BRIGHT}{Fore.YELLOW}Total businesses: {len(all_businesses)}")
    print(f"{Style.BRIGHT}{Fore.RED}Businesses with no numbers listed: {businesses_without_numbers}")
    print(f"{Style.BRIGHT}{Fore.YELLOW}Data saved to: {filename}")

if __name__ == "__main__":
    main()
