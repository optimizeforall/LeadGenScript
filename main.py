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
from openai import OpenAI
import threading

client = OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))

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

# Near the top of the file, after the other imports

if not os.environ.get('OPENAI_API_KEY'):
    print("Error: OPENAI_API_KEY environment variable is not set.")
    sys.exit(1)

parser = argparse.ArgumentParser()
parser.add_argument('--debug', action='store_true', help='Enable debug mode')
args = parser.parse_args()

# Replace the existing logging configuration with this
if args.debug:
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
else:
    logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')

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

def search_businesses(location, business_type, enhanced_query):
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
    query = enhanced_query if enhanced_query else business_type

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
        for geoname_id, city_info in us_cities.items():
            if city_info['countrycode'] == 'US' and city_info['admin1code'] == state.abbr:
                if city_info['population'] >= min_population:
                    city_name = f"{city_info['name']}, {state.abbr}"
                    cities.append(city_name)

        # Ensure the capital city is included
        capital_city = f"{state.capital}, {state.abbr}"
        if capital_city not in cities:
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

        cities = get_cities_for_state(state, us_cities, min_population)

        # Only display the number of cities found, not the entire list
        print(f"Number of cities found for {state.name} ({state.abbr}): {len(cities)}")

        return {state.abbr: cities}
    else:
        return get_cities_by_state(all_states=True)

def generate_enhanced_query(original_query):
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",  # Using the specified model
        messages=[
            {"role": "system", "content": "You are a helpful assistant that generates targeted search queries for homeowner services using the Google Places API."},
            {"role": "user", "content": f"Generate a specific, targeted search query for homeowner services related to: '{original_query}'. The query should be 3-4 words long and aimed at improving search results within the bounds of the Places API. Respond only with the enhanced query."}
        ]
        )
        enhanced_query = response.choices[0].message.content.strip()
        
        
        return enhanced_query
    except Exception as e:
        print(f"Error generating enhanced query: {str(e)}")
        return original_query

# Add this function at the beginning of your file
def print_legend():
    print(f"Legend: {Fore.RED}0 leads {Fore.YELLOW}1-4 leads {Fore.BLUE}5-14 leads {Fore.GREEN}15+ leads{Style.RESET_ALL}")

# Update the display_progress function
def display_progress(city, result, start_time, completed_cities, total_cities):
    runtime = datetime.now() - start_time
    
    if result['status'] == 'Completed':
        data = result['data']
        if data['leads'] >= 15:
            color = Fore.GREEN
        elif data['leads'] >= 5:
            color = Fore.BLUE
        elif data['leads'] > 0:
            color = Fore.YELLOW
        else:
            color = Fore.RED
        print(f"{color}{city:<20} {data['leads']:<8} {data['avg_rating']:.2f}        {data['total_reviews']:<15} {str(runtime):<15} {completed_cities}/{total_cities}")
    elif result['status'] == 'Error':
        print(f"{Fore.RED}{city:<20} {'ERROR':<8} {'N/A':<12} {'N/A':<15} {str(runtime):<15} {completed_cities}/{total_cities}")

    sys.stdout.flush()

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

    init(autoreset=True)  # Initialize colorama
    start_time = datetime.now()

    # Get cities grouped by state based on arguments
    try:
        cities_by_state = get_cities_by_state(args.state, args.all_states, args.everything)
    except ValueError as ve:
        logging.error(str(ve))
        sys.exit(1)

    total_cities = sum(len(cities) for cities in cities_by_state.values())
    states = list(cities_by_state.keys())

    # Calculate estimated runtime in minutes
    estimated_seconds = total_cities * 2
    estimated_minutes = estimated_seconds // 60
    estimated_seconds_remainder = estimated_seconds % 60

    # Print the introduction
    print(f"\n{Fore.GREEN}Starting business search process:")
    print(f"{Fore.GREEN}- Searching for: {args.business_type}")
    print(f"{Fore.GREEN}- Total cities to search: {total_cities}")
    print(f"{Fore.GREEN}- States included: {', '.join(states)}")
    print(f"{Fore.GREEN}- Estimated runtime: {estimated_minutes} minutes {estimated_seconds_remainder} seconds\n")

    # Print the legend
    print_legend()

    # Generate the enhanced query using GPT-4 Mini
    print(f"\n{Style.BRIGHT}{Fore.MAGENTA}Original Search Query: {args.business_type}")
    enhanced_query = generate_enhanced_query(args.business_type)
    print(f"{Style.BRIGHT}{Fore.MAGENTA}Enhanced Search Query: {enhanced_query}")

    print(f"\n{Style.BRIGHT}{Fore.CYAN}{'City, State':<20} {'Leads':<8} {'Avg Rating':<12} {'Total Reviews':<15} {'Runtime':<15} {'Progress'}")
    print(f"{Style.BRIGHT}{Fore.CYAN}{'-'*80}")

    all_businesses = []
    businesses_without_numbers = 0

    completed_cities = 0
    total_cities = sum(len(cities) for cities in cities_by_state.values())

    # Create a dictionary to store results for each city
    city_results = {}

    # Optimize ThreadPoolExecutor by increasing max_workers
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        future_to_city = {}
        for state, cities in cities_by_state.items():
            for city in cities:
                future = executor.submit(search_businesses, city, args.business_type, enhanced_query)
                future_to_city[future] = city
                # Initialize results for each city
                city_results[city] = {'status': 'Pending', 'data': None}

        for future in concurrent.futures.as_completed(future_to_city):
            city = future_to_city[future]
            try:
                businesses, skipped_businesses = future.result()
                all_businesses.extend(businesses)
                completed_cities += 1

                businesses_with_numbers = [b for b in businesses if b['PHONE'] != 'N/A']
                businesses_without_numbers += skipped_businesses

                avg_rating = sum(float(b['RATING']) for b in businesses_with_numbers if b['RATING'] != 'N/A') / len(businesses_with_numbers) if businesses_with_numbers else 0
                total_reviews = sum(int(b['REVIEWS']) for b in businesses_with_numbers if b['REVIEWS'] != 'N/A')

                city_results[city] = {
                    'status': 'Completed',
                    'data': {
                        'leads': len(businesses_with_numbers),
                        'avg_rating': avg_rating,
                        'total_reviews': total_reviews
                    }
                }
                # Display progress for this city immediately
                display_progress(city, city_results[city], start_time, completed_cities, total_cities)
            except Exception as exc:
                city_results[city] = {'status': 'Error', 'data': str(exc)}
                # Display error for this city immediately
                display_progress(city, city_results[city], start_time, completed_cities, total_cities)

    # Final display of results
    display_progress(city_results, start_time, completed_cities, total_cities)

    # Update the filename to be either 'leads.csv' or 'bad-leads.csv'
    good_leads_filename = "leads.csv"
    bad_leads_filename = "bad-leads.csv"

    # Separate good leads (with phone numbers) from bad leads (without phone numbers)
    good_leads = [b for b in all_businesses if b['PHONE'] != 'N/A']
    bad_leads = [b for b in all_businesses if b['PHONE'] == 'N/A']

    # Save good leads
    save_to_csv(good_leads, good_leads_filename)

    # Save bad leads
    save_to_csv(bad_leads, bad_leads_filename)

    total_runtime = datetime.now() - start_time
    print(f"\n{Style.BRIGHT}{Fore.YELLOW}Total runtime: {total_runtime}")
    print(f"{Style.BRIGHT}{Fore.YELLOW}Total businesses: {len(all_businesses)}")
    print(f"{Style.BRIGHT}{Fore.GREEN}Businesses with phone numbers: {len(good_leads)}")
    print(f"{Style.BRIGHT}{Fore.RED}Businesses without phone numbers: {len(bad_leads)}")
    print(f"{Style.BRIGHT}{Fore.YELLOW}Good leads saved to: {good_leads_filename}")
    print(f"{Style.BRIGHT}{Fore.YELLOW}Bad leads saved to: {bad_leads_filename}")

if __name__ == "__main__":
    main()