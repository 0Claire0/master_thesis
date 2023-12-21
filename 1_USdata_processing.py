import csv
import os

# Specify the folder path and file name
folder_path = 'C:\\Users\\clair\\MasterThesis'  # Example folder path on Windows
file_name = 'US.txt'

# Construct the full file path
file_path = os.path.join(folder_path, file_name)

# Read data from the text file with UTF-8 encoding
with open(file_path, 'r', encoding='utf-8') as txt_file:
    lines = txt_file.readlines()

# Identify the separator used in the text file
separator = '\t'  # Assuming values are separated by tabs

# Extract only the desired columns (0-based indices)
desired_columns = [0, 1, 4, 5, 7, 8, 10]  # Example: geonameid, name, latitude, longitude, country code, admin1 code
rows = [line.strip().split(separator) for line in lines]

# Select only the desired columns
selected_rows = [[row[i] for i in desired_columns] for row in rows]

# Add the header row with specified column names
header = ['geonameid', 'name', 'latitude', 'longitude', 'feature code', 'country code', 'admin1 code']

# Specify data types for each column
column_data_types = [str, str, float, float, str, str, str]

# Write data to CSV file with utf-8-sig encoding
output_file_path = os.path.join(folder_path, 'USdata_processed.csv')
with open(output_file_path, 'w', newline='', encoding='utf-8-sig') as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(header)  # Write header to CSV

    # Write selected rows to CSV with specified data types
    for row in selected_rows:
        typed_row = [dtype(value) for dtype, value in zip(column_data_types, row)]
        csv_writer.writerow(typed_row)

print(f"CSV file has been created at: {output_file_path}")


