import glob
import pandas as pd
from datetime import datetime

# Extract json files
def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process)
    return dataframe

columns = ['Name', 'Market Cap (US$ Billion)']
def extract():
    extracted_data = pd.DataFrame(columns=['Name', 'Market Cap (US$ Billion)'])

    # Process JSON file
    for jsonfile in glob.glob("*.json"):
        extracted_data = extracted_data.append(extract_from_json(jsonfile), ignore_index=True)

    return extracted_data

# Load the file as a DF, find the exchange rate for GBP and store it in a variable
#log("Extract phase started")
extracted_data = extract()
#log("Extract phase ended")
extracted_data

def transform(data):
    data['Market Cap (US$ Billion)'] = round(0.75*data['Market Cap(US$ Billion)'], 3)
    data.rename(columns={'Market Cap (US$ Billion)': 'Market Cap(GBP Billion)'}, inplace=True)
    return data

transformed_data = transform(extracted_data)
transformed_data

def load(target_file, data_to_load):
    data_to_load.to_csv(target_file, index=False)

# log your data
def log(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()  # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("logfile.txt", "a") as f:
        f.write(timestamp + ',' + message + '\n')

# run ETL process
log("ETL Job Started")
log("ETL phase Started")

# Call function
extracted_data = extract()
extracted_data.head()

# log the data
log("Transform phase Ended")

# Call the load function
load('market_cap.csv', transformed_data)

log("Load phase ended")