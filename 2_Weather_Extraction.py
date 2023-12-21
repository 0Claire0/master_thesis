import requests
import datetime
import pandas as pd
import json
import pytz
import os
import configparser

# Create a ConfigParser object
config = configparser.ConfigParser()

# Read the configuration file
config.read('config.ini')

# Access the API token
API_TOKEN = config['API']['API_TOKEN']


# Read CSV file
data = pd.read_csv('USdata_processed.csv')

# Function to get coordinates based on name and admin1 code
def get_coordinates(name, admin1_code):
	location_data = data[(data['name'] == name) & (data['admin1 code'] == admin1_code)]
	if not location_data.empty:
		latitude = location_data['latitude'].values[0]
		longitude = location_data['longitude'].values[0]
		return latitude, longitude
	else:
		return None


# Function to get meteorological data using Synoptic Data API
def get_weather_data(latitude, longitude, start_time, end_time):
	radius_param = f'{latitude},{longitude},6.3'

	base_url = 'https://api.synopticdata.com/v2/stations/timeseries'
	response = requests.get(
		f'{base_url}?radius={radius_param}&start={start_time}&end={end_time}&vars=air_temp,pressure,wind_speed,solar_radiation,precip_accum,snow_accum,water_temp,dew_point_temperature,relative_humidity,wind_direction,wind_gust,altimeter,pressure,soil_temp,water_temp,precip_storm,road_temp,road_freezing_temp,visibility,ceiling,soil_temp_ir,soil_moisture,snow_accum_manual,evapotranspiration,surface_temp,net_radiation_sw,net_radiation_lw,sonic_air_temp,sonic_vertical_vel,sonic_zonal_wind_stdev,sonic_vertical_wind_stdev,sonic_air_temp_stdev,vertical_heat_flux,friction_velocity,vertical_moisture_flux,ozone_concentration,electric_conductivity,incoming_radiation_uv,NH3_concentration,NO2y_concentration,NO2_concentration,NOx_concentration,NOy_concentration,NO_concentration,outgoing_radiation_lw,outgoing_radiation_uv,particulate_concentration,PM_10_concentration,SO2_concentration&token={API_TOKEN}')

	if response.status_code == 200:
		try:
			data = response.json()
			return data
		except json.JSONDecodeError:
			print("Error: Unable to parse JSON response.")
			print(response.content)  # Print the response content for debugging purposes
			return None
	else:
		print(f"Error: Unable to fetch weather data. Status code: {response.status_code}")
		print(response.content)  # Print the response content for debugging purposes
		return None


# Get user input for location name and admin1 code
name_input = input("Enter location name: ")
admin1_code_input = input("Enter admin1 code: ")

# Get coordinates based on user input
coordinates = get_coordinates(name_input, admin1_code_input)

if coordinates:
    latitude, longitude = coordinates
    current_year = datetime.datetime.now().year

    for year in range(current_year - 4, current_year + 1):
        weather_data_all = {}  # Dictionary to store weather data for all days of June

        for day in range(1, 31):  # Days in June
            # Inside the loop where you are iterating through days in June
            start_time = datetime.datetime(year, 6, day, 8, 0, 0, tzinfo=pytz.utc)
            end_time = datetime.datetime(year, 6, day, 18, 0, 0, tzinfo=pytz.utc)

            # Convert start_time and end_time to UTC formatted strings
            start_time_formatted = start_time.strftime('%Y%m%d%H%M')
            end_time_formatted = end_time.strftime('%Y%m%d%H%M')

            weather_data = get_weather_data(latitude, longitude, start_time_formatted, end_time_formatted)

            if weather_data:
                # Store weather data for the day in the dictionary
                weather_data_all[f'{year}_06_{day}'] = weather_data
            else:
                print(f"Weather data not available for {name_input} in {year}-06-{day}.")

        # Save the compiled weather data as a JSON file for the month
        output_folder = 'C:\\Users\\clair\\MasterThesis\\Extracted_weather_data'  # Specify the folder path where you want to save the JSON files
        output_file_name = os.path.join(output_folder, f'{name_input}_{year}_06_weather_data.json')
        with open(output_file_name, 'w') as json_file:
            json.dump(weather_data_all, json_file, indent=4)
        print(f"Weather data for {name_input} in {year}-06 saved as {output_file_name}.")

else:
    print("Location not found.")