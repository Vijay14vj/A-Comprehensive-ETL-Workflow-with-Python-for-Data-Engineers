import os
import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime
import subprocess
import zipfile
import requests

# Logging function
def log(message):
    with open("log_file.txt", "a") as log_file:
        log_file.write(f"{datetime.now()}: {message}\n")

# Function to download the dataset using requests (alternative to wget)
def download_data(url, destination):
    try:
        print(f"Downloading dataset from {url}...")
        response = requests.get(url)
        response.raise_for_status()  # Check for errors in the request
        with open(destination, "wb") as file:
            file.write(response.content)
        log(f"Dataset downloaded from {url}")
        print("Download completed successfully.")
    except requests.exceptions.RequestException as e:
        log(f"Error downloading the dataset: {str(e)}")
        print(f"Error downloading the dataset: {str(e)}")

# Function to unzip the downloaded file
def unzip_file(zip_file, destination_folder):
    try:
        print(f"Unzipping file {zip_file} to {destination_folder}...")
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall(destination_folder)
        log(f"Dataset unzipped to {destination_folder}")
        print("Unzipping completed successfully.")
    except Exception as e:
        log(f"Error unzipping the file: {str(e)}")
        print(f"Error unzipping the file: {str(e)}")

# Extract functions
def extract_csv(file_path):
    try:
        df = pd.read_csv(file_path)
        print(f"Extracted CSV data from {file_path}:")
        print(df.head())
        return df
    except Exception as e:
        log(f"Error extracting CSV from {file_path}: {str(e)}")
        print(f"Error extracting CSV from {file_path}: {str(e)}")
        return pd.DataFrame()

def extract_json(file_path):
    try:
        df = pd.read_json(file_path, lines=True)
        print(f"Extracted JSON data from {file_path}:")
        print(df.head())
        return df
    except Exception as e:
        log(f"Error extracting JSON from {file_path}: {str(e)}")
        print(f"Error extracting JSON from {file_path}: {str(e)}")
        return pd.DataFrame()

def extract_xml(file_path):
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
        data = []
        columns = [child.tag for child in root[0]]
        for element in root:
            data.append({child.tag: child.text for child in element})
        df = pd.DataFrame(data, columns=columns)
        print(f"Extracted XML data from {file_path}:")
        print(df.head())
        return df
    except Exception as e:
        log(f"Error extracting XML from {file_path}: {str(e)}")
        print(f"Error extracting XML from {file_path}: {str(e)}")
        return pd.DataFrame()

def extract_data(file_path):
    if file_path.endswith(".csv"):
        return extract_csv(file_path)
    elif file_path.endswith(".json"):
        return extract_json(file_path)
    elif file_path.endswith(".xml"):
        return extract_xml(file_path)
    else:
        log(f"Unsupported file format: {file_path}")
        print(f"Unsupported file format: {file_path}")
        return pd.DataFrame()

# Transform function
def transform_data(df):
    print("Data before transformation:")
    print(df.head())
    if "height" in df.columns:
        df["height"] = (df["height"].astype(float) * 0.0254).round(2)  # Convert to meters and round to 2 decimals
    if "weight" in df.columns:
        df["weight"] = (df["weight"].astype(float) * 0.453592).round(2)  # Convert to kg and round to 2 decimals
    print("Data after transformation:")
    print(df.head())
    return df

# Load function
def load_data(df, output_path):
    if df.empty:
        print("Warning: No data to save!")
        log("No data to save!")
    else:
        try:
            df.to_csv(output_path, index=False)
            print(f"Data saved to {output_path}")
        except Exception as e:
            log(f"Error saving data to {output_path}: {str(e)}")
            print(f"Error saving data to {output_path}: {str(e)}")

# Main ETL process
def etl_process(input_folder, output_file):
    log("ETL process started")
    all_data = pd.DataFrame()

    # Extract phase
    log("Extraction started")
    for file_path in glob.glob(os.path.join(input_folder, "*.*")):
        log(f"Processing file: {file_path}")
        print(f"Processing file: {file_path}")
        data = extract_data(file_path)
        if not data.empty:
            all_data = pd.concat([all_data, data], ignore_index=True)
    log("Extraction completed")

    # Debug extracted data
    print("Combined data after extraction:")
    print(all_data.head())

    # Transform phase
    log("Transformation started")
    all_data = transform_data(all_data)
    log("Transformation completed")

    # Debug transformed data
    print("Combined data after transformation:")
    print(all_data.head())

    # Load phase
    log("Loading started")
    load_data(all_data, output_file)
    log("Loading completed")

    log("ETL process completed")

# Execution
if __name__ == "__main__":
    # Step 1: Download the dataset
    dataset_url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/source.zip"
    download_path = "C:/Users/vijay/OneDrive/Desktop/ETL work flow with python/source.zip"
    download_data(dataset_url, download_path)

    # Step 2: Unzip the downloaded file
    unzip_folder = "C:/Users/vijay/OneDrive/Desktop/ETL work flow with python/unzipped_folder"
    unzip_file(download_path, unzip_folder)

    # Step 3: Run the ETL process
    input_folder = unzip_folder
    output_file = "C:/Users/vijay/OneDrive/Desktop/ETL work flow with python/transformed_data.csv"
    etl_process(input_folder, output_file)
