import sys
import os
import argparse
import logging
from datetime import datetime
import concurrent.futures
from colorama import Fore, Style, init
import openai
from lead_processor import (get_cities_by_state, search_businesses, save_to_csv)

# Set up OpenAI API key
openai.api_key = os.environ.get('OPENAI_API_KEY')

if not openai.api_key:
    print("Error: OPENAI_API_KEY environment variable is not set.")
    sys.exit(1)

def enhance_query(query):
    """
    Use OpenAI's GPT-4 model to enhance the user's query.
    """
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are an AI assistant helping to enhance search queries for finding businesses on Google Maps. The goal is to find relevant businesses based on the user's input. Please provide a 3-5 word enhanced query."},
                {"role": "user", "content": f"Enhance this query for finding businesses on Google Maps: '{query}'. Return only the enhanced query, nothing else."}
            ]
        )
        enhanced_query = response.choices[0].message['content'].strip()
        return enhanced_query
    except Exception as e:
        logging.error(f"Error enhancing query: {str(e)}")
        return query  # Return original query if enhancement fails

def main():
    parser = argparse.ArgumentParser(description="Search for businesses in US states.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--all-states", action="store_true", help="Search in one major city per all 50 states")
    group.add_argument("--everything", action="store_true", help="Search in all cities of all states")
    parser.add_argument("--state", help="Two-letter state abbreviation to search for cities")
    parser.add_argument("business_type", nargs="?", default="Lighting and Holiday", help="Type of business to search for")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    init(autoreset=True)  # Initialize colorama
    start_time = datetime.now()

    # Enhance the user's query
    enhanced_query = enhance_query(args.business_type)
    print(f"\n{Style.BRIGHT}{Fore.YELLOW}Original query: {args.business_type}")
    print(f"{Style.BRIGHT}{Fore.GREEN}Enhanced query: {enhanced_query}\n")

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

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        future_to_city = {executor.submit(search_businesses, city, enhanced_query): city 
                          for state, cities in cities_by_state.items() 
                          for city in cities}
        
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
                
                color = Fore.GREEN if len(businesses_with_numbers) > 20 else Fore.BLUE if len(businesses_with_numbers) > 0 else Fore.RED
                
                print(f"{color}{city:<20} {len(businesses_with_numbers):<8} {avg_rating:.2f}        {total_reviews:<15} {str(runtime):<15} {completed_cities}/{total_cities}")
            except Exception as exc:
                print(f"{Fore.RED}{city:<20} {'ERROR':<8} {'N/A':<12} {'N/A':<15} {str(runtime):<15} {completed_cities}/{total_cities}")
                print(f"Error details: {str(exc)}")

    filename = f"leads_{enhanced_query.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    save_to_csv(all_businesses, filename)
    
    total_runtime = datetime.now() - start_time
    print(f"\n{Style.BRIGHT}{Fore.YELLOW}Total runtime: {total_runtime}")
    print(f"{Style.BRIGHT}{Fore.YELLOW}Total businesses: {len(all_businesses)}")
    print(f"{Style.BRIGHT}{Fore.RED}Businesses with no numbers listed: {businesses_without_numbers}")
    print(f"{Style.BRIGHT}{Fore.YELLOW}Data saved to: {filename}")

if __name__ == "__main__":
    main()