import requests
import json
import os
   

# Get user input for the directory
directory = input("Enter the jsonpath: ")

# Ensure the directory exists, create it if necessary
if not os.path.exists(directory):
    os.makedirs(directory)

# Get a list of CSV files from the specified directory
file_path = input("Enter the directory containing CSV files: ")
csv_files = [f for f in os.listdir(file_path) if f.lower().endswith('.csv')]

url = "https://apis.tollguru.com/toll/v2/gps-tracks-csv-upload"
headers = {'x-api-key': 'FdnPN6qJ8QjhgjmL6hNFfqpjq363LqhM', 'Content-Type': 'text/csv'}

# Process each CSV file and create JSON files
for csv_file in csv_files:
    csv_file_path = os.path.join(file_path, csv_file)

    # Extract the base_file_name from the CSV file name
    base_file_name = os.path.splitext(os.path.basename(csv_file_path))[0]

    with open(csv_file_path, 'rb') as file:
        response = requests.post(url, headers=headers, data=file)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Parse the JSON response
            response_json = response.json()
            json_file_path = os.path.join(directory, f"{base_file_name}.json")
            with open(file_path, 'w') as json_file:
                json.dump(response_json, json_file, indent=4)
                print(f"JSON data {csv_file} has been written to {json_file_path}")
        else:
            print(f"Request for {csv_file} failed with status code {response.status_code}: {response.text}")
