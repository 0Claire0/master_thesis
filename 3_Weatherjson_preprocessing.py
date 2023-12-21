import json
import pandas as pd
import os
from openpyxl import load_workbook
import pyarrow.parquet as pq

# Set Pandas display options to show all rows and columns
pd.set_option('display.max_rows', None)  # Show all rows
pd.set_option('display.max_columns', None)  # Show all columns


def return_station_df(data):
	meta_dict = {}
	data_list = []

	for id_key, id_data in data.items():
		for station_data in id_data['STATION']:
			idnum = station_data['ID']
			stid = station_data['STID']
			mnet_id = station_data['MNET_ID']
			lon = float(station_data['LONGITUDE'])
			lat = float(station_data['LATITUDE'])

			# Store metadata in the dictionary using station ID as the key
			if stid not in meta_dict:
				meta_dict[stid] = {"STATION_NUM": idnum, "MNET_ID": mnet_id, "LONGITUDE": lon, "LATITUDE": lat}

			datetime_values = station_data['OBSERVATIONS'].get('date_time', [])

			# Specify the additional variables to extract
			variables_to_extract = [
				'air_temp', 'pressure', 'wind_speed', 'solar_radiation', 'precip_accum', 'snow_accum', 'water_temp',
				'dew_point_temperature', 'relative_humidity', 'wind_direction', 'wind_gust', 'altimeter', 'pressure',
				'soil_temp', 'water_temp', 'precip_storm', 'road_temp', 'road_freezing_temp', 'visibility', 'ceiling',
				'soil_temp_ir', 'soil_moisture', 'snow_accum_manual', 'evapotranspiration', 'surface_temp',
				'net_radiation_sw', 'net_radiation_lw', 'sonic_air_temp', 'sonic_vertical_vel',
				'sonic_zonal_wind_stdev', 'sonic_vertical_wind_stdev', 'sonic_air_temp_stdev', 'vertical_heat_flux',
				'friction_velocity', 'vertical_moisture_flux', 'ozone_concentration', 'electric_conductivity',
				'incoming_radiation_uv', 'NH3_concentration', 'NO2y_concentration', 'NO2_concentration',
				'NOx_concentration',
				'NOy_concentration', 'NO_concentration', 'outgoing_radiation_lw', 'outgoing_radiation_uv',
				'particulate_concentration', 'PM_10_concentration', 'SO2_concentration'
			]

			# Specify the variables for which you want to keep the maximum value
			variables_to_keep_max = ['precip_accum', 'snow_accum', 'snow_accum_manual']

			# Calculate hourly values for each variable
			for i in range(len(datetime_values)):
				datetime_value = pd.to_datetime(datetime_values[i], format='%Y-%m-%dT%H:%M:%SZ').replace(minute=0,
																										 second=0,
																										 microsecond=0)

				# Initialize the data structure for this datetime if not present
				if datetime_value not in hourly_data:
					hourly_data[datetime_value] = {variable: [] for variable in variables_to_extract}

				# Append the values for each variable
				for variable in variables_to_extract:
					variable_values = station_data['OBSERVATIONS'].get(f'{variable}_set_1', [])
					value = variable_values[i] if i < len(variable_values) else None
					hourly_data[datetime_value][variable].append(value)

			# Calculate either averages or maximum values for each variable for each hour
			for hour, values in hourly_data.items():
				data_row = [idnum, stid, hour]
				for variable in variables_to_extract:
					if variable in variables_to_keep_max:
						max_value = max(filter(None, values[variable])) if values[variable] and any(
							values[variable]) else None
						data_row.append(max_value)
					else:
						avg_value = sum(filter(None, values[variable])) / len(values[variable]) if values[variable][
																									   0] is not None else None
						data_row.append(avg_value)

				data_list.append(data_row)

	# Create DataFrame from the dictionary
	columns = ["STATION_NUM", "STATION_ID", "DATETIME"] + [f"{variable}_set_1" for variable in variables_to_extract]
	data_df = pd.DataFrame(data_list, columns=columns)

	# Convert DATETIME column to datetime type
	data_df['DATETIME'] = pd.to_datetime(data_df['DATETIME'])

	# Extract year, month, day, and hour from the DATETIME feature
	data_df['YEAR'] = data_df['DATETIME'].dt.year
	data_df['MONTH'] = data_df['DATETIME'].dt.month
	data_df['DAY'] = data_df['DATETIME'].dt.day
	data_df['HOUR'] = data_df['DATETIME'].dt.hour

	# Set STATION_NUM as the index
	data_df.set_index("STATION_NUM", inplace=True)

	return data_df


def clean_empty_stations(json_data):
    cleaned_data = {}
    for key, value in json_data.items():
        # Check if 'STATION' list is not empty
        if 'STATION' in value and value['STATION']:
            observations = value['STATION'][0]['OBSERVATIONS']
            if any(observations.get(column) is not None and column != 'date_time' for column in observations.keys()):
                cleaned_data[key] = value
    return cleaned_data


# Directory containing the JSON files
json_folder = 'C:\\Users\\clair\\MasterThesis\\Extracted_weather_data'

# List all JSON files in the folder
json_files = [os.path.join(json_folder, file) for file in os.listdir(json_folder) if file.endswith('.json')]

# Group JSON files based on the location name
file_groups = {}
for json_file in json_files:
	# Extract the location name as a group identifier
	group_identifier = os.path.basename(json_file).split('_')[
		0]  # Assuming the location name is the first part of the filename

	# Append the file to the corresponding group
	if group_identifier not in file_groups:
		file_groups[group_identifier] = [json_file]
	else:
		file_groups[group_identifier].append(json_file)

# Process each group separately
for group_identifier, group_files in file_groups.items():
	# Process each JSON file in the group and concatenate the results into a single DataFrame
	data_dfs = []
	for json_file in group_files:
		hourly_data = {}  # Initialize hourly_data for each file
		try:
			with open(json_file, 'r') as json_file:
				json_data = json.load(json_file)
		except json.JSONDecodeError as e:
			print(f"Error reading JSON file {json_file}: {e}")
			continue  # Skip to the next file in case of an error

		# Clean stations with no observations
		cleaned_json_data = clean_empty_stations(json_data)

		data_df = return_station_df(cleaned_json_data)
		if not data_df.empty:  # Check if the DataFrame is not empty before appending
			data_dfs.append(data_df)

	# Check if there's at least one non-empty DataFrame before concatenating
	if data_dfs:
		# Concatenate DataFrame into a single DataFrame for the group
		final_data_df = pd.concat(data_dfs)

		# Flatten multi-level columns in final_data_df
		final_data_df.reset_index(inplace=True)  # Reset all levels of the index

		# Concatenate group identifier with column names
		final_data_df.columns = [f'{group_identifier}_{col}' if col not in ['YEAR', 'MONTH', 'DAY', 'HOUR'] else col
								 for col in final_data_df.columns.values]

		# Ensure unique column names
		final_data_df = final_data_df.loc[:, ~final_data_df.columns.duplicated()]

		# Save the DataFrame to a Parquet file
		output_parquet_path = f'C:\\Users\\clair\\MasterThesis\\Processed_weather_data\\Weatherdata_processed_{group_identifier}.parquet'
		final_data_df.to_parquet(output_parquet_path, index=False)
	else:
		print("No non-empty DataFrames to concatenate.")
