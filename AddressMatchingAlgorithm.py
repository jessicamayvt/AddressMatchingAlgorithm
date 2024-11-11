import pandas as pd
from rapidfuzz import fuzz, process
from multiprocessing import Pool, cpu_count
import h3
import time

start_time = time.time()

# Define file paths
TRIBE_ADDRESS_DATE_FILE_PATH = "confidential/npt_customer_address_examples.csv"
BSL_FABRIC_FILE_PATH = "confidential/FCC_Active_BSLFORMAT.csv"
CLEANED_FILE_PATH = "confidential/cleaned_dataFORMAT.csv"

# Load the CSV files into DataFrames
tribe_address_data = pd.read_csv(TRIBE_ADDRESS_DATE_FILE_PATH)
fabric_address_data = pd.read_csv(BSL_FABRIC_FILE_PATH)
cleaned_data = pd.read_csv(CLEANED_FILE_PATH)

# Rename columns for consistency in both datasets
tribe_address_data = tribe_address_data.rename(columns={'GPS LAT': 'latitude', 'GPS LONG': 'longitude'})

#####          COORDINATE MATCHING          ##########
# Helper function to validate latitude and longitude values
def is_valid_lat_lon(lat, lon):
    """Check if latitude and longitude are valid."""
    return -90 <= lat <= 90 and -180 <= lon <= 180

# Generate H3 index for each row based on latitude and longitude
def generate_h3_index(lat, long, resolution=9):
    """Generate H3 index if lat and long are valid, else return None."""
    if is_valid_lat_lon(lat, long):
        return h3.latlng_to_cell(lat, long, resolution)
    return None

# Calculate approximate matches based on H3 indexing
def calculate_match_based_on_coordinates(tribe_df, fabric_df, resolution=9):
    # Drop rows with missing or invalid coordinates
    tribe_df = tribe_df.dropna(subset=['latitude', 'longitude']).copy()
    fabric_df = fabric_df.dropna(subset=['latitude', 'longitude']).copy()

    # Generate H3 indices for both DataFrames
    tribe_df['h3_index'] = tribe_df.apply(lambda row: generate_h3_index(row['latitude'], row['longitude'], resolution), axis=1)
    fabric_df['h3_index'] = fabric_df.apply(lambda row: generate_h3_index(row['latitude'], row['longitude'], resolution), axis=1)

    matching_coordinates = pd.merge(
        tribe_df[['latitude', 'longitude', 'h3_index']],
        fabric_df[['latitude', 'longitude', 'h3_index']],
        on='h3_index',
        how='inner',
        suffixes=('_tribe', '_fabric')
    )

    # Group by each unique tribe entry's latitude and longitude to keep the closest match
    unique_tribe_matches = matching_coordinates.groupby(['latitude_tribe', 'longitude_tribe']).first().reset_index()

    # Print matching coordinates
    print("Unique Matching Coordinates for each tribe entry:")
    print(unique_tribe_matches[['latitude_tribe', 'longitude_tribe', 'latitude_fabric', 'longitude_fabric']])

    # Return the number of unique matches
    return unique_tribe_matches.shape[0]  # Number of unique matches based on each entry in tribe_df

######       ADDRESS MATCHING       #######
# Filter to only include 'id' state entries in fabric address data
id_fabric_address_data = fabric_address_data[fabric_address_data['state'].str.lower() == 'id'].copy()

#Create address hash for quick exact matching
def create_address_hash_tribes(row):
    street = str(row.get('Street 1', '')).lower() if pd.notna(row.get('Street 1')) else ''
    town = str(row.get('Town', '')).lower() if pd.notna(row.get('Town')) else ''
    state = str(row.get('State', '')).lower() if pd.notna(row.get('State')) else ''
    return f"{street}-{town}-{state}"

#Create address hash for quick exact matching
def create_address_hash_fabric(row):
    street = str(row.get('address_primary', '')).lower() if pd.notna(row.get('address_primary')) else ''
    town = str(row.get('city', '')).lower() if pd.notna(row.get('city')) else ''
    state = str(row.get('state', '')).lower() if pd.notna(row.get('state')) else ''
    return f"{street}-{town}-{state}"


# Filter cleaned data for Idaho entries and generate hashes
cleaned_data_idaho = cleaned_data[cleaned_data['State'].str.lower() == 'id'].copy()
cleaned_data_idaho['address_hash'] = cleaned_data_idaho.apply(create_address_hash_tribes, axis=1)

# Create Idaho BSL address hashes
id_fabric_address_data.loc[:, 'address_hash'] = id_fabric_address_data.apply(create_address_hash_fabric, axis=1)
id_fabric_address_set = set(id_fabric_address_data['address_hash'])

#Create Non-cleaned data address hashes
tribe_data = tribe_address_data[tribe_address_data['State'].str.lower() == 'idaho'].copy()
tribe_data['address_hash'] = tribe_data.apply(create_address_hash_tribes, axis=1)

# Mark exact matches in cleaned data
cleaned_data_idaho['exact_match'] = cleaned_data_idaho['address_hash'].isin(id_fabric_address_set)
tribe_data['exact_match'] = tribe_data['address_hash'].isin(id_fabric_address_set)

# Separate exact matches from non-exact matches for cleaned data 
exact_matches = cleaned_data_idaho[cleaned_data_idaho['exact_match']]
non_exact_matches = cleaned_data_idaho[~cleaned_data_idaho['exact_match']]

#Separate Exact Matches from Non-Exact Matches for Regular Data
regular_exact_matches = tribe_data[tribe_data['exact_match']]
regular_non_exact_matches = tribe_data[~tribe_data['exact_match']]

#Fuzzy matching function to user in parallel processing
def fuzzy_match(chunk, fabric_data):
    results = []
    for subscriber in chunk: 
        best_match = process.extractOne(subscriber, fabric_data, scorer=fuzz.ratio)
        if best_match and best_match[1] >= 80:
            results.append((subscriber, best_match[0], best_match[1]))
    return results

# Method for chunking the subscriber data
def chunk_data(data, chunk_size):
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]

# Main method that uses multiprocessing to map chunked subscriber data to fuzzy matching
def main(subscriber_data, fabric_data, chunk_size=100):
    """Coordinates the chunking and multiprocessing of fuzzy matching."""
    # Convert data to lists if necessary
    subscriber_data = subscriber_data.tolist() if isinstance(subscriber_data, pd.Series) else subscriber_data
    fabric_data = fabric_data.tolist() if isinstance(fabric_data, pd.Series) else fabric_data
    
    # Create data chunks
    chunks = list(chunk_data(subscriber_data, chunk_size))
    
    # Initialize multiprocessing pool
    with Pool(cpu_count()) as pool:
        # Perform parallel fuzzy matching
        results = pool.starmap(fuzzy_match, [(chunk, fabric_data) for chunk in chunks])
    
    # Flatten the list of results
    flattened_results = [item for sublist in results for item in sublist]
    
    return flattened_results

# Example usage
if __name__ == "__main__":
    
    # Use non-exact match addresses for fuzzy matching for cleaned data 
    non_exact_addresses = non_exact_matches['address_hash']
    fabric_addresses = id_fabric_address_data['address_hash']

    #Use non-exact match addresses for fuzzy matching regular data
    regular_non_exact_addresses = regular_non_exact_matches['address_hash']

    # Run the main method
    cleaned_match_results = main(non_exact_addresses, fabric_addresses)
    regular_match_results = main(regular_non_exact_addresses, fabric_addresses)

    # Calculate approximate coordinate matches using H3 indexing
    approximate_matches = calculate_match_based_on_coordinates(tribe_address_data, fabric_address_data)
    print(f"Total Number of Approximate Matches based on Latitude and Longitude with tolerance: {approximate_matches}")

   # Print the results for non-cleaned data
    print(f"Non-Cleaned Data - Total Number of Fuzzy Matches: {len(regular_match_results)}")
    print(f"Non-Cleaned Data - Number of Exact Matches: {len(regular_exact_matches)}")

    # Print the results for cleaned data
    print(f"\nCleaned Data - Total Number of Fuzzy Matches: {len(cleaned_match_results)}")
    print(f"Cleaned Data - Number of Exact Matches: {len(exact_matches)}")

    # Print all results into a txt file 
    CLEANED_MATCH_RESULTS_TEXT_FILE = "cleaned_match_results.txt"
    with open(CLEANED_MATCH_RESULTS_TEXT_FILE, 'w') as file:
        for result in cleaned_match_results:
            print(f"Subscriber: {result[0]}, Matched Fabric: {result[1]}, Score: {result[2]}", file=file)

    UNCLEANED_MATCH_RESULTS_TEXT_FILE = "uncleaned_match_results.txt"
    with open(UNCLEANED_MATCH_RESULTS_TEXT_FILE, 'w') as file:
        for result in regular_match_results:
            print(f"Subscriber: {result[0]}, Matched Fabric: {result[1]}, Score: {result[2]}", file=file)
    
    print("--- %s seconds ---" % (time.time() - start_time))