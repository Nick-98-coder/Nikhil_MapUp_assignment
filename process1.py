import pandas as pd
import os
import argparse

def process_parquet(input_file, output_folder):
    df = pd.read_parquet(input_file)

    # Convert timestamp to datetime without converting to ISO format
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')

    # Calculate time difference between consecutive data points
    time_diff = df['timestamp'].diff()

    # Identify trip boundaries where time difference is more than 7 hours
    trip_boundaries = time_diff > pd.Timedelta(hours=7)

    # Create a new trip number for each trip boundary
    trip_numbers = trip_boundaries.cumsum()

    # Create the output folder if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)

    # Group by trip number and write to CSV files
    for trip_number, group in df.groupby(trip_numbers):
        # Get the unit value for the trip from the 'unit' column
        unit_value = group['unit'].iloc[0]

        # Generate CSV filename based on the pattern {unit}_{trip_number}.csv
        csv_filename = os.path.join(output_folder, f"{unit_value}_{trip_number}.csv")

        # Format the timestamp column back to the original RFC 3339 format
        group['timestamp'] = group['timestamp'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

        # Write the group to the CSV file without including the 'unit' column
        group.drop(columns=['unit']).to_csv(csv_filename, index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process a Parquet file and generate CSV files.')
    parser.add_argument('--to_process', type=str, help='Path to the Parquet file to be processed.')
    parser.add_argument('--output_dir', type=str, help='The folder to store the resulting CSV files.')
    args = parser.parse_args()

    input_path = args.to_process
    output_folder = args.output_dir

    process_parquet(input_path, output_folder)



#excecutable command-line arguments from my PC
#python process1.py --to_process C:\Users\Nick\Documents\assignment1\evaluation_data\input\raw_data.parquet --output_dir C:\Users\Nick\Documents\assignment1\evaluation_data\output\process2