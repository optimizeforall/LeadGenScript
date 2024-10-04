# Business Lead Generator

## Description
This Python tool searches for businesses across U.S. cities using the Google Places API. It's designed to generate leads for various business types, making it useful for sales and marketing professionals.

## Prerequisites
- Python 3.7 or higher
- A Google Places API key

## Setup
1. Clone the repository:
   ```
   git clone https://github.com/yourusername/business-lead-generator.git
   cd business-lead-generator
   ```

2. Install required packages:
   ```
   pip install -r requirements.txt
   ```

3. Set up your Google Places API key:
   - Create a file named `.env` in the project root
   - Add the following line to the file:
     ```
     GOOGLE_PLACES_API_KEY=your_api_key_here
     ```
   Replace `your_api_key_here` with your actual Google Places API key.

## Usage
Run the script using one of these commands:

1. Search in all states:
   ```
   python main.py --all-states "Business Type"
   ```

2. Search in a specific state:
   ```
   python main.py --state NY "Business Type"
   ```

Replace "Business Type" with your target business category (e.g., "Roofing", "Plumbing").

## Output
The script generates two CSV files:
- `leads.csv`: Contains valid business leads
- `bad-leads.csv`: Contains invalid or duplicate leads

## Notes
- Ensure you have sufficient API quota before running large searches.
- Respect Google's terms of service and API usage limits.

## Troubleshooting
If you encounter any issues, check the following:
- Ensure your API key is correctly set in the `.env` file
- Verify that all required packages are installed
- Check your internet connection

For more detailed error messages, run the script with the `--debug` flag: