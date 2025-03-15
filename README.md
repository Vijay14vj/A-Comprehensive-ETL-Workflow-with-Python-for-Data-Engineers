# A Comprehensive ETL Workflow with Python for Data Engineers

## Introduction
This project demonstrates an end-to-end Extract, Transform, Load (ETL) workflow using Python. The goal is to extract data from multiple formats (CSV, JSON, XML), transform it by standardizing units, and load the processed data into a structured format suitable for further analysis.

## Objectives
By the end of this project, you will be able to:
- Extract data from CSV, JSON, and XML files.
- Transform extracted data into a desired format, including unit conversions.
- Load transformed data into a structured format (CSV) for further use.
- Log ETL operations for monitoring and debugging purposes.

## Dataset
The dataset for this project can be downloaded using the following command:
```sh
wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/source.zip
```
After downloading, extract the data using:
```sh
unzip source.zip -d ./unzipped_folder
```

## Technologies Used
- **Python**
- **Pandas** for data handling
- **glob** for file operations
- **xml.etree.ElementTree** for parsing XML
- **datetime** for logging

## Project Structure
```
├── data/                      # Raw data files (CSV, JSON, XML)
├── logs/                      # Log files for tracking ETL execution
├── output/                    # Processed output files
├── etl.py                     # Main ETL script
├── requirements.txt            # Required Python packages
└── README.md                   # Project documentation
```

## ETL Process Breakdown
### 1. Extract Data
- Extract data from CSV, JSON, and XML files using dedicated functions.
- Combine extracted data into a Pandas DataFrame.

### 2. Transform Data
- Convert height from inches to meters.
- Convert weight from pounds to kilograms.
- Ensure data consistency for further analysis.

### 3. Load Data
- Save the transformed data into a CSV file.
- Store the final dataset in a structured format.

### 4. Logging
- Maintain a log file (`log_file.txt`) to track each phase of ETL execution.
- Capture timestamps for extraction, transformation, and loading processes.

## How to Run the Project
1. Install dependencies:
   ```sh
   pip install -r requirements.txt
   ```
2. Execute the ETL pipeline:
   ```sh
   python etl.py
   ```
3. Check logs for execution details:
   ```sh
   cat logs/log_file.txt
   ```

## Expected Output
- A processed CSV file containing cleaned and transformed data.
- Log files documenting the execution of the ETL pipeline.

## Contribution Guidelines
If you'd like to contribute:
- Fork the repository.
- Create a new branch.
- Make your changes and submit a pull request.

## References
- [Python Official Documentation](https://www.python.org/doc/)
- [PEP 8 - Python Coding Standards](https://www.python.org/dev/peps/pep-0008/)


