from colorama import Fore, Style
from datetime import datetime

def get_location_coordinates(location):
    # This function is not implemented in the current version
    # You may want to implement it using a geocoding service if needed
    return None

def is_duplicate(business, existing_businesses):
    return any(
        b['NAME'].lower() == business['NAME'].lower() and
        b['CITY/STATE'].lower() == business['CITY/STATE'].lower()
        for b in existing_businesses
    )

def save_to_csv(businesses, filename):
    # This function is now handled by write_output_data in src/data_handler.py
    # You can remove this function or keep it as a wrapper if needed
    from src.data_handler import write_output_data
    write_output_data(businesses, filename)

def print_legend():
    print(f"Legend: {Fore.RED}0 leads {Fore.YELLOW}1-4 leads {Fore.BLUE}5-14 leads {Fore.GREEN}15+ leads{Style.RESET_ALL}")

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

    import sys
    sys.stdout.flush()