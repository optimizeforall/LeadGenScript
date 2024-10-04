import requests
import json
import csv
import time
import concurrent.futures
import sys
import os
from datetime import datetime
from colorama import Fore, Style, init
from geopy.geocoders import Nominatim
import us
from urllib.request import urlopen
from io import StringIO
import argparse
from geonamescache import GeonamesCache
from geonamescache.mappers import country
from threading import local
import logging

# Initialize thread-local storage for sessions
thread_local = local()

def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    return thread_local.session

# API key for Google Places API (imported from environment variable)
API_KEY = os.environ.get('GOOGLE_PLACES_API_KEY')

if not API_KEY:
    print("Error: GOOGLE_PLACES_API_KEY environment variable is not set.")
    sys.exit(1)

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

def search_businesses(location, business_type):
    """
    Search for businesses using Google Places API.
    
    Args:
    location (str): Location to search in
    business_type (str): Type of business to search for
    
    Returns:
    list: List of dictionaries containing business information
    int: Number of skipped businesses
    """
    session = get_session()
    businesses = []
    skipped_businesses = 0
    next_page_token = None
    page_count = 0
    query = f"{business_type}"
    
    while True:
        page_count += 1
        
        # Construct the URL for the Places API text search
        url = f"https://maps.googleapis.com/maps/api/place/textsearch/json?query={query}+in+{location}&key={API_KEY}"
        
        if next_page_token:
            url += f"&pagetoken={next_page_token}"
        
        # Make the API request with retries
        for attempt in range(MAX_RETRIES):
            try:
                response = session.get(url, timeout=10)
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
                        details_response = session.get(details_url, timeout=10)
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

def get_cities_by_state(state_input=None, all_states=False, everything=False, min_population=10000):
    """
    Generate a dictionary of cities grouped by state using the geonamescache library.
    
    Args:
        state_input (str): The state abbreviation or full name to search for cities.
        all_states (bool): If True, include only the capital city of each state.
        everything (bool): If True, include all cities for all states.
        min_population (int): Minimum populatioon for a city to be included.
    
    Returns:
        dict: Dictionary with states as keys and lists of "City, State" as values.
    """
    
    gc = GeonamesCache()
    us_cities = gc.get_cities()
    
    def get_state_from_input(input_str):
        if input_str:
            state = us.states.lookup(input_str)
            if not state:
                state = next((s for s in us.states.STATES if s.name.lower() == input_str.lower()), None)
            return state
        return None
    
    def get_cities_for_state(state, us_cities, min_population):
        cities = []
        # Get the capital city for the state
        capital_city = f"{state.capital}, {state.abbr}"
        cities.append(capital_city)
        return cities
    
    if everything:
        states_dict = {}
        for state in us.states.STATES:
            if not state.is_territory:
                cities = get_cities_for_state(state, us_cities, min_population)
                states_dict[state.abbr] = cities
        return states_dict
    elif all_states:
        # Include only the capital city per state
        states_dict = {}
        for state in us.states.STATES:
            if not state.is_territory:
                cities = get_cities_for_state(state, us_cities, min_population)
                states_dict[state.abbr] = cities
        return states_dict
    elif state_input:
        state = get_state_from_input(state_input)
        if not state:
            raise ValueError(f"Invalid state input: {state_input}")
        
        # Get the capital city for the specified state
        cities = get_cities_for_state(state, us_cities, min_population)
        
        print(f"Cities found for {state.name} ({state.abbr}): {cities}")

        return {state.abbr: cities}  # Return the capital city for the state
    else:
        # Default to all major cities if no specific input is provided
        return get_cities_by_state(all_states=True)

def main():
    """
    Main function to run the business search and data saving process.
    """

    parser = argparse.ArgumentParser(description="Search for businesses in US states.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--all-states", action="store_true", help="Search in one major city per all 50 states")
    group.add_argument("--everything", action="store_true", help="Search in all cities of all states")
    parser.add_argument("--state", help="Two-letter state abbreviation to search for cities")
    parser.add_argument("business_type", nargs="?", default="Lighting and Holiday", help="Type of business to search for")
    args = parser.parse_args()

    # Configure logging for better performance and debugging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    init(autoreset=True)  # Initialize colorama
    start_time = datetime.now()

    # Add the new print statements for the search queries
    print(f"\n{Style.BRIGHT}{Fore.MAGENTA}Original Search Query: {args.business_type}")
    enhanced_query = f"{args.business_type} services"  # Example of an enhanced query
    print(f"{Style.BRIGHT}{Fore.MAGENTA}Enhanced Search Query: {enhanced_query}")

    # Get cities grouped by state based on arguments
    try:
        cities_by_state = get_cities_by_state(args.state, args.all_states, args.everything)
    except ValueError as ve:
        logging.error(str(ve))
        sys.exit(1)

    all_businesses = []
    businesses_without_numbers = 0

    print(f"{Style.BRIGHT}{Fore.CYAN}{'City, State':<20} {'Leads':<8} {'Avg Rating':<12} {'Total Reviews':<15} {'Runtime':<15} {'Progress'}")
    print(f"{Style.BRIGHT}{Fore.CYAN}{'-'*80}")

    completed_cities = 0
    total_cities = sum(len(cities) for cities in cities_by_state.values())

    # Optimize ThreadPoolExecutor by increasing max_workers
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        future_to_city = {}
        for state, cities in cities_by_state.items():
            for city in cities:
                future = executor.submit(search_businesses, city, args.business_type)
                future_to_city[future] = city
        
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
                
                print(f"{color}{city:<20} {len(businesses_with_numbers):<8} {avg_rating:.2f}        {total_reviews:<15} {str(runtime):<15} {completed_cities}/{total_cities}")
            except Exception as exc:
                print(f"{Fore.RED}{city:<20} {'ERROR':<8} {'N/A':<12} {'N/A':<15} {str(runtime):<15} {completed_cities}/{total_cities}")
                print(f"Error details: {str(exc)}")

    filename = f"leads_{args.business_type.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    save_to_csv(all_businesses, filename)
    
    total_runtime = datetime.now() - start_time
    print(f"\n{Style.BRIGHT}{Fore.YELLOW}Total runtime: {total_runtime}")
    print(f"{Style.BRIGHT}{Fore.YELLOW}Total businesses: {len(all_businesses)}")
    print(f"{Style.BRIGHT}{Fore.RED}Businesses with no numbers listed: {businesses_without_numbers}")
    print(f"{Style.BRIGHT}{Fore.YELLOW}Data saved to: {filename}")

if __name__ == "__main__":
    main()