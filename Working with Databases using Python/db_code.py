import pandas as pd
import sqlite3

#URLS to download csv files
# https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/INSTRUCTOR.csv
# https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/Departments.csv
#Database initiation
conn = sqlite3.connect("STAFF.db")

#Create and Load the table
table_name = "INSTRUCTOR"
attribute_list = ['ID', 'FNAME', 'LNAME', 'CITY', 'CCODE']
file_path = "/workspaces/IBM-Data-Engineering/Working with Databases using Python/INSTRUCTOR.csv"

df = pd.read_csv(file_path, names=attribute_list)
df.to_sql(table_name, conn, if_exists= 'replace')
print("Table is ready")

#Running basic queries on data
query = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query, conn)

print(query)
print("###################")
print(query_output)

#Add new records to the database

data_dict = {
    "ID":[100],
    'FNAME' : ['John'],
    'LNAME' : ['Doe'],
    'CITY' : ['Paris'],
    'CCODE' : ['FR']

}
data_append = pd.DataFrame(data_dict)
data_append.to_sql(table_name, conn, if_exists="append", index=False)
print('Data appended successfully')

query = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query, conn)

print(query)
print("###################")
print(query_output)
#####################################################################
#Another Example

table_name = "Departments"
attribute_list = ["DEPT_ID", "DEP_NAME","MANAGER_ID", "LOC_ID"]
file_path = file_path = "/workspaces/IBM-Data-Engineering/Working with Databases using Python/Departments.csv"

df = pd.read_csv(file_path, names = attribute_list)
df.to_sql(table_name, conn, if_exists = 'replace')
query = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query, conn)
print(query)
print("###################")
print(query_output)
print("#####################")

data_dict = {
    "DEPT_ID":[9], 
    "DEP_NAME":["Quality Assurance"],
    "MANAGER_ID": [30010],
    "LOC_ID": ["L0010"]
}
data_append = pd.DataFrame(data_dict)
data_append.to_sql(table_name, conn, if_exists="append")

query = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query, conn)

print(query)
print("###################")
print(query_output)

#Close database connection
conn.close()