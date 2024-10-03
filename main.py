import requests
import json
import csv
import time
import concurrent.futures
import sys
import os
import argparse
from datetime import datetime
from colorama import Fore, Style, init
from geopy.geocoders import Nominatim
import us
from geonamescache import GeonamesCache
from geonamescache.mappers import country
from threading import local
import logging
from collections import defaultdict, Counter
from queue import Queue

# Initialize thread-local storage for sessions
thread_local = local()

def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    return thread_local.session  # Fixed: Removed parentheses to return the session object

def close_session():
    session = get_session()
    session.close()

# Constants
API_KEY = os.environ.get('GOOGLE_PLACES_API_KEY')
if not API_KEY:
    print("Error: GOOGLE_PLACES_API_KEY environment variable is not set.")
    sys.exit(1)

BUSINESS_TYPE_DEFAULT = "Lighting and Holiday"
MAX_RETRIES = 3
BACKOFF_TIME = 2
MIN_POPULATION = 10000
MAJOR_CITIES_LIMIT = 100  # Number of major cities to search in each state

def get_location_coordinates(location):
    geolocator = Nominatim(user_agent="my_app")
    try:
        location_data = geolocator.geocode(location)
        if location_data:
            return f"{location_data.latitude},{location_data.longitude}"
    except Exception as e:
        print(f"Error getting coordinates for {location}: {str(e)}")
    return None

def load_big_chains(filename):
    """Load big chain names from a text file."""
    try:
        with open(filename, 'r') as file:
            chains = [line.strip().lower() for line in file.readlines()]
            return chains
    except Exception as e:
        print(f"Error loading big chains: {str(e)}")
        return []

def search_businesses(location, business_type, debug=False):
    """
    Search for businesses using Google Places API.
    
    Args:
    location (str): Location to search in
    business_type (str): Type of business to search for
    
    Returns:
    list: List of dictionaries containing business information
    int: Number of skipped businesses (only duplicates, no phone, or closed)
    list: List of dictionaries containing bad lead information
    int: Number of closed businesses
    """
    session = get_session()
    businesses = []
    skipped_businesses = 0
    closed_businesses = 0  # New counter for closed businesses
    next_page_token = None
    page_count = 0
    query = f"{business_type}"
    bad_leads = []  # Initialize bad_leads list
    request_count = 0
    start_time = time.time()

    # Load big chains to exclude
    big_chains = load_big_chains('big_chain.txt')
    
    # Debugging statement to check the loaded chains
    if debug:
        print(f"Loaded big chains: {big_chains}")  # Moved to debug mode
    
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
            except requests.exceptions.RequestException as e:
                print(f"Request error on attempt {attempt + 1} for {location}: {str(e)}")
                if attempt == MAX_RETRIES - 1:
                    print(f"\nError searching {location}: {str(e)}")
                    return businesses, skipped_businesses, bad_leads, closed_businesses
                time.sleep(BACKOFF_TIME * (2 ** attempt))  # Exponential backoff
            except json.JSONDecodeError as e:
                print(f"JSON decode error on attempt {attempt + 1} for {location}: {str(e)}")
                if attempt == MAX_RETRIES - 1:
                    print(f"\nError decoding JSON response for {location}: {str(e)}")
                    return businesses, skipped_businesses, bad_leads, closed_businesses
        
        if 'error_message' in results:
            if "rate limit" in results['error_message'].lower():
                print(f"\nAPI Rate Limit Exceeded for {location}. Pausing for a while before retrying.")
                for backoff_attempt in range(MAX_RETRIES):
                    time.sleep(BACKOFF_TIME * (2 ** backoff_attempt))  # Exponential backoff for rate limit
                continue  # Retry the request
            print(f"\nAPI Error for {location}: {results['error_message']}")
            break
        
        for result in results.get('results', []):
            place_id = result.get('place_id')
            if not place_id:
                continue  # Skip if no place_id
            
            # Debugging statement to check the current business name
            if debug:
                print(f"Checking business: {result['name']}")
            
            # Filter out major chains based on the loaded list
            if any(chain in result['name'].lower() for chain in big_chains):
                skipped_businesses += 1
                bad_leads.append({
                    'Name': result['name'],
                    'Phone': 'N/A',
                    'Website': 'N/A',  # Fixed: Changed to 'N/A' to avoid using details_results before it's defined
                    'State/City': f"{location.split(',')[1].strip()}, {location.split(',')[0].strip()}",
                    'Rating': 'N/A',  # Fixed: Changed to 'N/A' to avoid using details_results before it's defined
                    'Reviews': 'N/A',  # Fixed: Changed to 'N/A' to avoid using details_results before it's defined
                    'Score': 0,
                    'Reason': 'Major chain store'
                })
                continue  # Skip to the next result
            
            # Update the fields to include business_status
            details_url = (
                f"https://maps.googleapis.com/maps/api/place/details/json?"
                f"place_id={place_id}&fields=formatted_phone_number,website,rating,user_ratings_total,business_status&key={API_KEY}"
            )
            
            for attempt in range(MAX_RETRIES):
                try:
                    details_response = session.get(details_url, timeout=10)
                    details_response.raise_for_status()
                    details_results = details_response.json().get('result', {})
                    break
                except requests.exceptions.RequestException as e:
                    print(f"Request error on attempt {attempt + 1} for details of {result.get('name')}: {str(e)}")  # Detailed error message
                    if attempt == MAX_RETRIES - 1:
                        print(f"\nError getting details for {result.get('name')} in {location}: {str(e)}")
                        details_results = {}
                    else:
                        time.sleep(BACKOFF_TIME * (attempt + 1))
                except json.JSONDecodeError as e:
                    print(f"JSON decode error on attempt {attempt + 1} for details of {result.get('name')}: {str(e)}")  # JSON error message
                    if attempt == MAX_RETRIES - 1:
                        print(f"\nError decoding JSON response for details of {result.get('name')} in {location}: {str(e)}")
                        details_results = {}
            
            # Exclude closed businesses
            business_status = details_results.get('business_status', 'OPERATIONAL')
            if business_status != 'OPERATIONAL':
                closed_businesses += 1
                bad_leads.append({
                    'Name': result['name'],
                    'Phone': details_results.get('formatted_phone_number', 'N/A'),
                    'Website': details_results.get('website', 'N/A'),
                    'State/City': f"{location.split(',')[1].strip()}, {location.split(',')[0].strip()}",
                    'Rating': details_results.get('rating', 'N/A'),
                    'Reviews': details_results.get('user_ratings_total', 'N/A'),
                    'Score': 0,
                    'Reason': 'Business closed or not operational'
                })
                continue  # Skip to the next result
            
            phone_number = details_results.get('formatted_phone_number')
            website = details_results.get('website', 'N/A')
            rating = details_results.get('rating', 0)
            reviews = details_results.get('user_ratings_total', 0)
            score = float(rating) * int(reviews) if rating != 'N/A' and reviews != 'N/A' else 0
            
            if not phone_number:
                skipped_businesses += 1
                bad_leads.append({
                    'Name': result['name'],
                    'Phone': 'N/A',
                    'Website': website,
                    'State/City': f"{location.split(',')[1].strip()}, {location.split(',')[0].strip()}",
                    'Rating': rating,
                    'Reviews': reviews,
                    'Score': score,
                    'Reason': 'No phone number'
                })
                continue  # Skip to the next result
            
            business = {
                'Name': result['name'],
                'Phone': phone_number,
                'Website': website,
                'State/City': f"{location.split(',')[1].strip()}, {location.split(',')[0].strip()}",
                'Rating': rating,
                'Reviews': reviews,
                'Score': score
            }
            businesses.append(business)
        
        next_page_token = results.get('next_page_token')
        if not next_page_token:
            break
        
        # Increment request count
        request_count += 1

        # Rate limiting: Check if we need to sleep
        if request_count >= 100:  # 100 requests per second for Places API
            elapsed_time = time.time() - start_time
            if elapsed_time < 1:  # If less than 1 second has passed
                time.sleep(1 - elapsed_time)  # Sleep for the remaining time
            # Reset the counter and start time
            request_count = 0
            start_time = time.time()
        
        # time.sleep(1)  # Add a delay of 1 second between requests

    
    return businesses, skipped_businesses, bad_leads, closed_businesses  # Include bad_leads in return

def save_to_csv(businesses, filename):
    """
    Save the list of businesses to a CSV file.
    
    Args:
    businesses (list): List of dictionaries containing business information
    filename (str): Name of the file to save the data to
    """
    with open(filename, 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=['Name', 'Phone', 'Website', 'State/City', 'Rating', 'Reviews', 'Score'])
        writer.writeheader()
        writer.writerows(businesses)

def get_state_from_input(input_str):
    if input_str:
        state = us.states.lookup(input_str)
        if not state:
            state = next((s for s in us.states.STATES if s.name.lower() == input_str.lower()), None)
        return state
    return None

def get_cities_for_state(state, us_cities, min_population, limit=None):
    cities = [
        f"{city_data['name']}, {state.abbr}"
        for city_data in us_cities.values()
        if (city_data['countrycode'] == 'US' and
            city_data['admin1code'] == state.abbr and
            city_data['population'] >= min_population)
    ]
    cities.sort(
        key=lambda x: next(
            city_data['population'] for city_data in us_cities.values()
            if f"{city_data['name']}, {city_data['admin1code']}" == x
        ),
        reverse=True
    )
    return cities[:limit] if limit else cities  # Return all cities if limit is None

def get_cities_by_state(state_input=None, all_states=False, everything=False):
    gc = GeonamesCache()
    us_cities = gc.get_cities()
    
    if everything:
        all_cities = {}
        for state in us.states.STATES:
            if not state.is_territory:
                cities = get_cities_for_state(state, us_cities, MIN_POPULATION, MAJOR_CITIES_LIMIT)
                all_cities[state.abbr] = cities
                print(f"Cities found for {state.name} ({state.abbr}): {len(cities)}")  # Debugging statement
        return all_cities
    elif all_states:
        return {
            state.abbr: [f"{state.capital}, {state.abbr}"]
            for state in us.states.STATES if not state.is_territory
        }
    elif state_input:
        state = get_state_from_input(state_input)
        if not state:
            raise ValueError(f"Invalid state input: {state_input}")
        cities = get_cities_for_state(state, us_cities, MIN_POPULATION)
        print(f"Cities found for {state.name} ({state.abbr}): {cities}")
        return {state.abbr: cities}
    else:
        return {
            state.abbr: [f"{state.capital}, {state.abbr}"]
            for state in us.states.STATES if not state.is_territory
        }

def save_bad_leads_to_csv(bad_leads, filename):
    """
    Save the list of bad leads to a CSV file.
    
    Args:
    bad_leads (list): List of dictionaries containing bad lead information
    filename (str): Name of the file to save the data to
    """
    with open(filename, 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=['Name', 'Phone', 'Website', 'State/City', 'Rating', 'Reviews', 'Score', 'Reason'])
        writer.writeheader()
        writer.writerows(bad_leads)

def main():
    """
    Main function to run the business search and data saving process.
    """
    parser = argparse.ArgumentParser(description="Search for businesses in US states.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--all-states", action="store_true", help="Search in one major city per all 50 states")
    group.add_argument("--everything", action="store_true", help="Search in all cities of all states")
    parser.add_argument("--state", help="Two-letter state abbreviation to search for cities")
    parser.add_argument("--debug", action="store_true", help="Enable debug output")  # New debug argument
    parser.add_argument("business_type", nargs="?", default="Lighting and Holiday", help="Type of business to search for")
    args = parser.parse_args()

    # Initialize colorama
    init(autoreset=True)  
    start_time = datetime.now()

    # Describe the search operation in green
    if args.all_states:
        print(f"{Style.BRIGHT}{Fore.GREEN}Searching all 50 US state capitals for '{args.business_type}' businesses.{Style.RESET_ALL}")
    elif args.everything:
        print(f"{Style.BRIGHT}{Fore.GREEN}Searching major cities in all 50 US states for '{args.business_type}' businesses.{Style.RESET_ALL}")
    elif args.state:
        state = us.states.lookup(args.state)
        if not state:
            print(f"{Fore.RED}Error: Invalid state abbreviation '{args.state}'.{Style.RESET_ALL}")
            sys.exit(1)
        print(f"{Style.BRIGHT}{Fore.GREEN}Searching all cities in {state.name} for '{args.business_type}' businesses.{Style.RESET_ALL}")
    else:
        print(f"{Style.BRIGHT}{Fore.GREEN}Searching one major city per state for '{args.business_type}' businesses.{Style.RESET_ALL}")

    # Add a brief legend for the color system
    print(f"\n{Style.BRIGHT}Legend: {Fore.GREEN}20+ leads, {Fore.BLUE}6-19 leads, {Fore.YELLOW}1-5 leads, {Fore.RED}0 leads or error{Style.RESET_ALL}\n")

    # Get cities grouped by state based on arguments
    try:
        cities_by_state = get_cities_by_state(args.state, args.all_states, args.everything)
        total_cities = sum(len(cities) for cities in cities_by_state.values())
        print(f"{Style.BRIGHT}Total cities to search: {total_cities}{Style.RESET_ALL}")

        # Determine the number of max workers based on the total number of cities
        max_workers = 20 if total_cities < 200 else 10
        print(f"{Style.BRIGHT}Using {max_workers} worker threads{Style.RESET_ALL}")
    except ValueError as ve:
        print(f"{Fore.RED}Error: {str(ve)}{Style.RESET_ALL}")
        sys.exit(1)

    all_businesses = []
    all_bad_leads = []  # Initialize all_bad_leads list
    businesses_without_numbers = 0
    total_closed_businesses = 0
    total_large_chains_removed = 0  # New counter for large chains removed
    seen_businesses = defaultdict(int)
    reasons_counter = Counter()

    # Add bold headers
    print(f"{Style.BRIGHT}{'State/City':<20} {'Leads':<8} {'Avg Rating':<12} {'Total Reviews':<15} {'Runtime':<15} {'Progress'}{Style.RESET_ALL}")
    print(f"{'-'*80}")

    completed_cities = 0

    # Use the determined max_workers value
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_city = {}
        for state, cities in cities_by_state.items():
            for city in cities:
                future = executor.submit(search_businesses, city, args.business_type, args.debug)
                future_to_city[future] = city
        
        for future in concurrent.futures.as_completed(future_to_city):
            city = future_to_city[future]
            try:
                businesses, skipped_businesses, bad_leads, closed_businesses = future.result()  # Unpack bad_leads

                businesses_without_numbers += skipped_businesses
                total_closed_businesses += closed_businesses
                total_large_chains_removed += skipped_businesses  # Count large chains removed

                all_bad_leads.extend(bad_leads)  # Aggregate bad leads

                for business in businesses:
                    business_key = (business['Name'], business['Phone'])
                    if seen_businesses[business_key] == 0:
                        all_businesses.append(business)
                    else:
                        # Add duplicate reason
                        all_bad_leads.append({**business, 'Reason': 'Duplicate'})
                        reasons_counter['Duplicate'] += 1
                    seen_businesses[business_key] += 1

                runtime = datetime.now() - start_time
                completed_cities += 1
                
                businesses_with_numbers = [b for b in businesses if b['Phone'] != 'N/A']
                
                avg_rating = sum(float(b['Rating']) for b in businesses_with_numbers if b['Rating'] != 'N/A') / len(businesses_with_numbers) if businesses_with_numbers else 0
                total_reviews = sum(int(b['Reviews']) for b in businesses_with_numbers if b['Reviews'] != 'N/A')
                
                if len(businesses_with_numbers) > 20:
                    color = Fore.GREEN
                elif len(businesses_with_numbers) > 5:
                    color = Fore.BLUE
                elif len(businesses_with_numbers) > 0:
                    color = Fore.YELLOW
                else:
                    color = Fore.RED
                
                print(f"{color}{city:<20} {len(businesses_with_numbers):<8} {avg_rating:.2f}        {total_reviews:<15} {str(runtime):<15} {completed_cities}/{total_cities}{Style.RESET_ALL}")
            except Exception as exc:
                runtime = 'N/A'  # Initialized runtime to 'N/A' to avoid UnboundLocalError
                print(f"{Fore.RED}{city:<20} {'ERROR':<8} {'N/A':<12} {'N/A':<15} {runtime:<15} {completed_cities}/{total_cities}{Style.RESET_ALL}")

    # After collecting all businesses, print statistics
    total_businesses = sum(seen_businesses.values())  # Total businesses processed
    unique_businesses = len(seen_businesses)  # Unique businesses
    duplicates = total_businesses - unique_businesses  # Duplicates
    total_excluded = duplicates + businesses_without_numbers + total_closed_businesses + total_large_chains_removed

    # Adjust the percentage calculation
    # Ensure that the denominator does not include closed businesses if they are already counted
    if total_businesses > 0:  # Avoid division by zero
        percentage_removed = (total_excluded / total_businesses) * 100
    else:
        percentage_removed = 0  # Handle case where no businesses were found

    print(f"\n{Style.BRIGHT}Total businesses found: {total_businesses + total_closed_businesses}{Style.RESET_ALL}")
    print(f"{Style.BRIGHT}Unique businesses: {unique_businesses}{Style.RESET_ALL}")
    print(f"{Fore.RED}Duplicates removed: {duplicates}{Style.RESET_ALL}")
    print(f"{Fore.RED}Businesses with no numbers listed: {businesses_without_numbers}{Style.RESET_ALL}")
    print(f"{Fore.RED}Closed businesses: {total_closed_businesses}{Style.RESET_ALL}")
    print(f"{Fore.RED}Large chains removed: {total_large_chains_removed}{Style.RESET_ALL}")  # Output for large chains removed
    print(f"{Fore.RED}Total leads removed: {total_excluded}{Style.RESET_ALL}")  # Total leads removed
    print(f"{Fore.RED}Percentage leads removed: {percentage_removed:.2f}%{Style.RESET_ALL}")

    # Generate filenames
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    if args.all_states:
        filename_prefix = "leads-all-states-capitals"
    elif args.everything:
        filename_prefix = "leads-all-states-major-cities"
    elif args.state:
        filename_prefix = f"leads-{args.state.lower()}-all-cities"
    else:
        filename_prefix = "leads"

    filename = f"{filename_prefix}-{args.business_type.replace(' ', '_')}-{timestamp}.csv"
    bad_leads_filename = f"bad_leads-{args.business_type.replace(' ', '_')}-{timestamp}.csv"

    save_to_csv(all_businesses, filename)
    save_bad_leads_to_csv(all_bad_leads, bad_leads_filename)  # Save all_bad_leads

    total_runtime = datetime.now() - start_time
    print(f"\n{Style.BRIGHT}Total runtime: {total_runtime}{Style.RESET_ALL}")
    print(f"{Style.BRIGHT}Total businesses: {len(all_businesses)}{Style.RESET_ALL}")
    print(f"{Style.BRIGHT}Good leads saved to: {filename}{Style.RESET_ALL}")
    print(f"{Style.BRIGHT}Bad leads saved to: {bad_leads_filename}{Style.RESET_ALL}")  # Added bad leads file

if __name__ == "__main__":
    try:
        main()
    finally:
        close_session()