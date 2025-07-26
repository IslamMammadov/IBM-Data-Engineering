import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

log_file = 'log_file.txt'
target_file = 'transformed.csv'

def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe

def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process, lines = True)
    return dataframe

def extract_from_xml(file_to_process):
    dataframe = pd.DataFrame(columns=["name", "height", "weight"])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for person in root:
        name = person.find("name").text
        height = float(person.find("height").text)
        weight = float(person.find('weight').text)
        dataframe  = pd.concat([dataframe, pd.dataframe([{"name": name, "height":height, "weight":weight}])], ignore_index = True)
        return dataframe
    
def extract():
    extracted_data = pd.DataFrame(columns=["name", "height", "weight"])

    for csvfile in glob.glob("*.csv"):
        if csvfile != target_file:
            extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_csv(csvfile))], ignore_index=True)
    
    for jsonfile in glob.glob("*.json"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_json(jsonfile))], ignore_index=True)

    for xmlfile in glob.glob("*.xml"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_json(jsonfile))], ignore_index=True)

    return extracted_data

def transform(data):
    '''Convert inches to meters and round off to two decimals 
    1 inch is 0.0254 meters
    Convert pounds to kilograms and round off to two decimals 
    1 pound is 0.45359237 kilograms '''

    data["height"] = round(data.height*0.0254, 2)
    data["weight"] = round(data.weight * 0.45359237, 2)
    return data

def load_data(target_file, transformed_data):
    transform_data.to_csv(target_file)

def log_process(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file,"a") as f:
        f.write(timestamp + ", " + message +"\n")



# Log the initialization of the ETL process 
log_process("ETL Job has started")

# Log the beginning of the Extraction process
log_process("Extarct step started")

extract_data = extract()

log_process("Extarct step ended")

# Log  the Transformation process 
log_process("The tarnsform step started")
transform_data = transform(extract_data)
log_process("The tarnsform step ended")

# Log the beginning of the Loading process 
log_process("The Load step started")
load_data(target_file, transform_data)
log_process("The Load step ended")

# Log the completion of the ETL process 
log_process("ETL Job Ended") 