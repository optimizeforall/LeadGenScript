import csv

def save_to_csv(data, filename):
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        if data:  # Check if data is not empty
            writer = csv.DictWriter(csvfile, fieldnames=data[0].keys())
            writer.writeheader()
            for row in data:
                writer.writerow(row)
    
    print(f"Data saved to {filename}")

# Add this line to make the function available for import
__all__ = ['save_to_csv']