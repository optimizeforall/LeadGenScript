import requests
import json
import csv
import time
import concurrent.futures
import sys
import os
from datetime import datetime
from colorama import Fore, Back, Style, init
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
import asyncio
import aiohttp
from config import (
    GOOGLE_PLACES_API_KEY,
    OPENAI_API_KEY,
    BUSINESS_TYPE,
    MAX_RETRIES,
    BACKOFF_TIME
)
from data_handler import save_to_csv as save_to_csv_handler

client = OpenAI(api_key=OPENAI_API_KEY)

# Initialize thread-local storage for sessions
thread_local = local()

def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    return thread_local.session

# API key for Google Places API (imported from environment variable)
API_KEY = GOOGLE_PLACES_API_KEY

if not API_KEY:
    print("Error: GOOGLE_PLACES_API_KEY environment variable is not set.")
    sys.exit(1)

if not OPENAI_API_KEY:
    print("Error: OPENAI_API_KEY environment variable is not set.")
    sys.exit(1)

parser = argparse.ArgumentParser(description="Search for businesses in US states.")
parser.add_argument("--all-states", action="store_true", help="Search in major cities of all states")
parser.add_argument("--state", help="Two-letter state abbreviation to search for cities")
parser.add_argument("-n", "--number", type=int, default=100, help="Number of cities to process (default: 100)")
parser.add_argument("business_type", nargs="?", default="Lighting and Holiday", help="Type of business to search for")
parser.add_argument('--debug', action='store_true', help='Enable debug mode')
parser.add_argument('-k', '--keywords', help="Additional comma-separated keywords to extend the search")
args = parser.parse_args()

# Initialize colorama
init(autoreset=True)

# Replace the existing logging configuration with this
if args.debug:
    class ColoredFormatter(logging.Formatter):
        COLORS = {
            'DEBUG': Fore.CYAN,
            'INFO': Fore.GREEN,
            'WARNING': Fore.YELLOW,
            'ERROR': Fore.RED,
            'CRITICAL': Fore.RED + Back.WHITE
        }

        def format(self, record):
            color = self.COLORS.get(record.levelname, '')
            message = super().format(record)
            return f"{color}{message}{Style.RESET_ALL}"

    handler = logging.StreamHandler()
    handler.setFormatter(ColoredFormatter('%(asctime)s - %(levelname)s - %(message)s'))
    logging.basicConfig(level=logging.DEBUG, handlers=[handler])
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

async def get_place_details(session, place_id):
    url = f"https://maps.googleapis.com/maps/api/place/details/json?place_id={place_id}&fields=formatted_phone_number,website&key={API_KEY}"
    async with session.get(url) as response:
        result = await response.json()
        details = result.get('result', {})
        return details.get('formatted_phone_number'), details.get('website')

# Initialize a set to track unique businesses based on (name, phone)
unique_businesses = set()

# Update the is_duplicate function
def is_duplicate(business):
    identifier = (business['NAME'].strip().lower(), business['PHONE'].strip())
    if identifier in unique_businesses:
        return True
    unique_businesses.add(identifier)
    return False

# Update the search_businesses function
async def search_businesses(session, location, business_type, enhanced_query, keywords):
    businesses = []
    bad_leads = []
    skipped_businesses = 0
    duplicates = 0
    url = f"https://maps.googleapis.com/maps/api/place/textsearch/json?query={enhanced_query}+in+{location}&key={API_KEY}"

    async with session.get(url) as response:
        results = await response.json()

    if 'error_message' in results:
        print(f"\nAPI Error for {location}: {results['error_message']}")
        return businesses, bad_leads, skipped_businesses, duplicates

    total_results = len(results.get('results', []))
    valid_leads = 0
    invalid_leads = 0

    for result in results.get('results', []):
        business_name = result['name'].lower()
        if any(keyword in business_name for keyword in keywords):
            phone_number, website = await get_place_details(session, result.get('place_id'))
            city, state = location.split(',')
            business = {
                'NAME': result['name'],
                'PHONE': phone_number if phone_number else 'N/A',
                'WEBSITE': website if website else 'N/A',
                'STATE/CITY': f"{state.strip()}, {city.strip()}",
                'RATING': result.get('rating', 'N/A'),
                'REVIEWS': result.get('user_ratings_total', 'N/A'),
                'REASON': ''
            }
            
            if is_duplicate(business):
                business['REASON'] = 'duplicate'
                bad_leads.append(business)
                duplicates += 1
                invalid_leads += 1
            elif not phone_number:
                business['REASON'] = 'no_number'
                bad_leads.append(business)
                skipped_businesses += 1
                invalid_leads += 1
            else:
                businesses.append(business)
                valid_leads += 1
        else:
            city, state = location.split(',')
            bad_lead = {
                'NAME': result['name'],
                'PHONE': 'N/A',
                'WEBSITE': 'N/A',
                'STATE/CITY': f"{state.strip()}, {city.strip()}",
                'RATING': result.get('rating', 'N/A'),
                'REVIEWS': result.get('user_ratings_total', 'N/A'),
                'REASON': 'no_keyword'
            }
            bad_leads.append(bad_lead)
            invalid_leads += 1

    logging.debug(f"{Fore.MAGENTA}Location: {location}{Style.RESET_ALL}")
    logging.debug(f"{Fore.CYAN}Total results: {total_results}{Style.RESET_ALL}")
    logging.debug(f"{Fore.GREEN}Valid leads: {valid_leads}{Style.RESET_ALL}")
    logging.debug(f"{Fore.RED}Invalid leads: {invalid_leads}{Style.RESET_ALL}")
    logging.debug(f"{Fore.YELLOW}Duplicates: {duplicates}{Style.RESET_ALL}")

    return businesses, bad_leads, skipped_businesses, invalid_leads, duplicates

# Update the process_city function
async def process_city(session, city, business_type, enhanced_query, keywords):
    start_time_city = time.time()
    businesses, bad_leads, skipped_businesses, invalid_leads, duplicates = await search_businesses(session, city, business_type, enhanced_query, keywords)
    logging.debug(f"Time taken for {city}: {time.time() - start_time_city:.2f} seconds")
    return city, businesses, bad_leads, skipped_businesses, invalid_leads, duplicates

# Update the save_to_csv function
def save_to_csv(businesses, filename):
    with open(filename, 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=['NAME', 'PHONE', 'WEBSITE', 'STATE/CITY', 'RATING', 'REVIEWS', 'REASON'])
        writer.writeheader()
        writer.writerows(businesses)

def get_cities_by_state(state_input=None, all_states=False, num_cities=100, min_population=10000):
    logging.debug(f"Starting get_cities_by_state with state_input={state_input}, all_states={all_states}, num_cities={num_cities}")
    start_time = time.time()
    
    gc = GeonamesCache()
    us_cities = gc.get_cities()
    
    logging.debug(f"Loaded GeonamesCache in {time.time() - start_time:.2f} seconds")

    def get_state_from_input(input_str):
        if input_str:
            state = us.states.lookup(input_str)
            if not state:
                state = next((s for s in us.states.STATES if s.name.lower() == input_str.lower()), None)
            return state
        return None

    def get_cities_for_state(state, us_cities, min_population, num_cities):
        cities = []
        for geoname_id, city_info in us_cities.items():
            if city_info['countrycode'] == 'US' and city_info['admin1code'] == state.abbr:
                if city_info['population'] >= min_population:
                    city_name = f"{city_info['name']}, {state.abbr}"
                    cities.append((city_name, city_info['population']))
        
        cities.sort(key=lambda x: x[1], reverse=True)
        top_cities = [city[0] for city in cities[:num_cities]]
        
        capital_city = f"{state.capital}, {state.abbr}"
        if capital_city not in top_cities:
            top_cities.insert(0, capital_city)
        
        return top_cities[:num_cities]

    if all_states:
        states_dict = {}
        for state in us.states.STATES:
            if not state.is_territory:
                cities = get_cities_for_state(state, us_cities, min_population, num_cities)
                states_dict[state.abbr] = cities
        logging.debug(f"Processed all states in {time.time() - start_time:.2f} seconds")
        return states_dict
    elif state_input:
        state = get_state_from_input(state_input)
        if not state:
            raise ValueError(f"Invalid state input: {state_input}")

        cities = get_cities_for_state(state, us_cities, min_population, num_cities)
        logging.debug(f"Processed single state {state.abbr} in {time.time() - start_time:.2f} seconds")
        print(f"Number of cities found for {state.name} ({state.abbr}): {len(cities)}")
        return {state.abbr: cities}
    else:
        logging.debug("No state input or all_states flag, defaulting to all states")
        return get_cities_by_state(all_states=True, num_cities=num_cities)

def generate_enhanced_query_and_keywords(original_query, additional_keywords=None):
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a helpful assistant that generates concise and relevant enhanced search queries for residential services."},
                {"role": "user", "content": f"""Generate a concise enhanced search query for the business type: '{original_query}', focusing on residential services for homeowners. The enhanced query should be 2-3 words long, specific, and targeted to improve search results. Avoid unnecessary adjectives or commercial terms.

Also, provide a list of 10-15 relevant keywords or partial keywords, prioritizing residential-related terms. Include 'exterior' and 'contract' (to cover terms like contracting, contractor) in the keywords. Partial keywords are encouraged to match variations (e.g., 'illumin' for illuminate, illumination, etc.). Use 'light' instead of 'lights' to match both singular and plural forms.

Respond with the enhanced query on one line, followed by the keywords list on the next line, separated by commas. Example response format:
Enhanced Query: Residential Roofing
Keywords: roof, repair, install, exterior, contract, shingl, homeown, resident, light, ..."""}
            ]
        )
        result = response.choices[0].message.content.strip().split('\n')
        enhanced_query = result[0].split(': ')[1].strip()
        keywords = result[1].split(': ')[1].strip().split(', ')
        
        # Add additional keywords if provided
        if additional_keywords:
            keywords.extend([kw.strip() for kw in additional_keywords.split(',')])
        
        # Ensure each word in the enhanced query is in keywords
        keywords.extend(word for word in enhanced_query.split() if word not in keywords)

        keywords = list(set([word.lower() for word in keywords]))  # Remove duplicates and convert to lowercase

        return enhanced_query, keywords
    except Exception as e:
        print(f"Error generating enhanced query and keywords: {str(e)}")
        return original_query, []

# Add this function at the beginning of your file
def print_legend():
    print(f"\nLegend: {Fore.RED}0 leads {Fore.YELLOW}1-4 leads {Fore.BLUE}5-14 leads {Fore.GREEN}15+ leads{Style.RESET_ALL}")

# Update the display_progress function
def display_progress(city, result, start_time, completed_cities, total_cities):
    runtime = (datetime.now() - start_time).total_seconds()
    
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
        print(f"{color}{city:<20} {data['leads']:<8} {data['invalid_leads']:<15} {data['duplicates']:<10} {runtime:.2f}s{' ':<8} {completed_cities}/{total_cities}")
    elif result['status'] == 'Error':
        print(f"{Fore.RED}{city:<20} {'ERROR':<8} {'N/A':<15} {'N/A':<10} {runtime:.2f}s{' ':<8} {completed_cities}/{total_cities}")

    sys.stdout.flush()

# Add this function to ensure the directory exists
def ensure_directory(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

def deduplicate_businesses(businesses):
    deduped = []
    seen = set()
    for business in businesses:
        identifier = (business['NAME'].strip().lower(), business['PHONE'].strip())
        if identifier not in seen:
            deduped.append(business)
            seen.add(identifier)
    return deduped

async def main_async():
    """
    Main function to run the business search and data saving process.
    """

    init(autoreset=True)  # Initialize colorama
    start_time = datetime.now()

    # Get cities grouped by state based on arguments
    try:
        start_time_cities = time.time()
        cities_by_state = get_cities_by_state(args.state, args.all_states, args.number)
        logging.debug(f"Total time to get cities: {time.time() - start_time_cities:.2f} seconds")
    except ValueError as ve:
        logging.error(str(ve))
        sys.exit(1)

    total_cities = sum(len(cities) for cities in cities_by_state.values())
    states = list(cities_by_state.keys())
    
    # Print the introduction
    print(f"\n{Fore.GREEN}Starting business search process:")
    print(f"{Fore.GREEN}- Searching for: {args.business_type}")
    print(f"{Fore.GREEN}- Total cities to search: {total_cities}")
    print(f"{Fore.GREEN}- States included: {', '.join(states)}")

    # Print the legend
    print_legend()

    # Generate the enhanced query using GPT-4 Mini
    print(f"\n{Style.BRIGHT}{Fore.MAGENTA}Original Search Query: {args.business_type}")
    enhanced_query, keywords = generate_enhanced_query_and_keywords(args.business_type, args.keywords)
    logging.info(f"Enhanced query: {enhanced_query}")
    logging.info(f"Keywords: {keywords}")
    print(f"{Style.BRIGHT}{Fore.MAGENTA}Enhanced Search Query: {enhanced_query}")
    print(f"{Style.BRIGHT}{Fore.MAGENTA}Keywords: {', '.join(keywords)}")

    print(f"\n{Style.BRIGHT}{Fore.CYAN}{'City, State':<20} {'Leads':<8} {'Invalid Leads':<15} {'Dups':<10} {'Runtime':<15} {'Progress'}")
    print(f"{Style.BRIGHT}{Fore.CYAN}{'-'*85}")

    all_businesses = []
    all_bad_leads = []
    businesses_without_numbers = 0
    total_duplicates = 0

    completed_cities = 0
    total_cities = sum(len(cities) for cities in cities_by_state.values())

    async with aiohttp.ClientSession() as session:
        tasks = []
        for state, cities in cities_by_state.items():
            for city in cities:
                tasks.append(process_city(session, city, args.business_type, enhanced_query, keywords))

        for completed_task in asyncio.as_completed(tasks):
            try:
                city, businesses, bad_leads, skipped_businesses, invalid_leads, duplicates = await completed_task
                all_businesses.extend(businesses)
                all_bad_leads.extend(bad_leads)
                businesses_without_numbers += skipped_businesses
                total_duplicates += duplicates
                completed_cities += 1

                # Calculate averages and display progress
                businesses_with_numbers = [b for b in businesses if b['PHONE'] != 'N/A']
                avg_rating = sum(float(b['RATING']) for b in businesses_with_numbers if b['RATING'] != 'N/A') / len(businesses_with_numbers) if businesses_with_numbers else 0
                total_reviews = sum(int(b['REVIEWS']) for b in businesses_with_numbers if b['REVIEWS'] != 'N/A')

                display_progress(city, {
                    'status': 'Completed',
                    'data': {
                        'leads': len(businesses_with_numbers),
                        'invalid_leads': invalid_leads,
                        'duplicates': duplicates
                    }
                }, start_time, completed_cities, total_cities)
            except Exception as e:
                logging.error(f"Error processing task: {str(e)}")

    print("\nProcessing completed. Preparing final results...")

    # Replace the existing counts with deduplicated counts
    # Deduplicate the businesses
    all_businesses = deduplicate_businesses(all_businesses)
    all_bad_leads = deduplicate_businesses(all_bad_leads)

    # Recalculate counts based on deduplicated data
    didnt_match_keywords = sum(1 for lead in all_bad_leads if lead.get('REASON') == "no_keyword")
    total_duplicates = sum(1 for lead in all_bad_leads if lead.get('REASON') == "duplicate")
    no_phone_number = sum(1 for lead in all_bad_leads if lead.get('REASON') == "no_number")
    total_leads = len(all_businesses) + len(all_bad_leads)

    try:
        # Create the ../leads directory if it doesn't exist
        leads_directory = os.path.join(os.path.dirname(os.path.dirname(__file__)), "leads")
        ensure_directory(leads_directory)

        # Update the filenames to include the full path and timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        good_leads_filename = os.path.join(leads_directory, f"leads_{timestamp}.csv")
        bad_leads_filename = os.path.join(leads_directory, f"bad-leads_{timestamp}.csv")

        # Update the saving part
        print(f"Saving {len(all_businesses)} good leads...")
        save_to_csv_handler(all_businesses, good_leads_filename)
        
        print(f"Saving {len(all_bad_leads)} bad leads...")
        save_to_csv_handler(all_bad_leads, bad_leads_filename)

        # Update image paths in the data (if needed)
        for business in all_businesses + all_bad_leads:
            if 'IMAGE' in business and business['IMAGE']:
                old_path = business['IMAGE']
                new_path = old_path.replace('imgs/', '../imgs/')
                business['IMAGE'] = new_path

        total_runtime = datetime.now() - start_time
        total_leads = len(all_businesses) + len(all_bad_leads)
        
        print(f"\n{Style.BRIGHT}{Fore.YELLOW}Total runtime: {total_runtime}")
        print(f"{Style.BRIGHT}{Fore.YELLOW}Total leads: {total_leads}")
        print(f"{Style.BRIGHT}{Fore.RED}Total no numbers: {no_phone_number}")
        print(f"{Style.BRIGHT}{Fore.RED}Total duplicates found: {total_duplicates}")
        print(f"{Style.BRIGHT}{Fore.RED}Didn't match keywords: {didnt_match_keywords}")
        print(f"{Style.BRIGHT}{Fore.RED}Total invalid leads: {len(all_bad_leads)}")
        print(f"{Style.BRIGHT}{Fore.GREEN}Valid leads: {len(all_businesses)}")
        print(f"{Style.BRIGHT}{Fore.YELLOW}Good leads saved as: {os.path.basename(good_leads_filename)}")
        print(f"{Style.BRIGHT}{Fore.YELLOW}Bad leads saved as: {os.path.basename(bad_leads_filename)}")
        print(f"{Style.BRIGHT}{Fore.YELLOW}Leads saved in directory: {leads_directory}")

        print("\nScript completed successfully.")
    except Exception as e:
        print(f"\n{Fore.RED}An error occurred while finalizing results: {str(e)}")
        logging.exception("Error in finalizing results")

if __name__ == "__main__":
    try:
        asyncio.run(main_async())
    except Exception as e:
        print(f"\n{Fore.RED}An unexpected error occurred: {str(e)}")
        logging.exception("Unexpected error in main execution")