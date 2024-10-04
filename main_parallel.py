import requests
import json
import csv
import time
from datetime import datetime, timedelta
import concurrent.futures
import os
import sys
from colorama import init, Fore, Style

# Initialize colorama
init(autoreset=True)

# API key for Google Places API (replace with your own)
API_KEY = 'AIzaSyA-YMXLi1Er6R_-iL1VncrDUyPa3erKEU4'

def search_businesses(query, location):
    # ... (keep the existing function as is) ...

def save_to_csv(businesses, filename):
    # ... (keep the existing function as is) ...

def process_city(city, business_type):
    """
    Process a single city and return the results.
    
    Args:
    city (str): City to search in
    business_type (str): Type of business to search for
    
    Returns:
    list: List of businesses found in the city
    """
    query = f"{business_type}"
    businesses = search_businesses(query, city)
    return businesses

def main():
    """
    Main function to run the business search and data saving process.
    """
    start_time = datetime.now()

    business_type = "Lighting and Holiday"
    test_mode = False

    major_cities = [
        "New York, NY", "Los Angeles, CA", "Chicago, IL", "Houston, TX", "Phoenix, AZ",
        "Philadelphia, PA", "San Antonio, TX", "San Diego, CA", "Dallas, TX", "San Jose, CA",
        "Austin, TX", "Jacksonville, FL", "Fort Worth, TX", "Columbus, OH", "San Francisco, CA",
        "Charlotte, NC", "Indianapolis, IN", "Seattle, WA", "Denver, CO", "Washington, DC"
    ]

    all_businesses = []
    total_cities = len(major_cities)

    # Use ThreadPoolExecutor for parallel execution
    with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        future_to_city = {executor.submit(process_city, city, business_type): city for city in major_cities}
        
        for i, future in enumerate(concurrent.futures.as_completed(future_to_city), 1):
            city = future_to_city[future]
            try:
                businesses = future.result()
                all_businesses.extend(businesses)
                progress = f"{Fore.GREEN}[{i}/{total_cities}]"
                city_info = f"{Fore.BLUE}{city}: {Fore.YELLOW}{len(businesses)}"
                total_info = f"{Fore.MAGENTA}Total: {Fore.YELLOW}{len(all_businesses)}"
                sys.stdout.write(f"\r{progress} {city_info} | {total_info}{' '*20}")
                sys.stdout.flush()
            except Exception as exc:
                sys.stdout.write(f"\r{Fore.RED}Error in {city}: {exc}{' '*40}\n")
                sys.stdout.flush()

    filename = f"US_cities_{business_type.replace(' ', '_')}_leads_parallel.csv"
    save_to_csv(all_businesses, filename)
    
    end_time = datetime.now()
    runtime = end_time - start_time
    
    sys.stdout.write("\n\n")  # Move to a new line after the progress updates
    print(f"{Fore.GREEN}Search completed!")
    print(f"{Fore.CYAN}Total businesses found: {Fore.YELLOW}{len(all_businesses)}")
    print(f"{Fore.CYAN}Data saved to: {Fore.YELLOW}{filename}")
    print(f"{Fore.CYAN}Total runtime: {Fore.YELLOW}{runtime}")

if __name__ == "__main__":
    main()