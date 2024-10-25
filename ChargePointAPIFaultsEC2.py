from zeep import Client
from zeep.wsse.username import UsernameToken
from datetime import datetime, timedelta
from supabase import create_client, Client as Supa_Client
import numpy as np
import pandas as pd
import requests
import re
import os

'''
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
'''

def check_existing_session(sessionID, table_name):
    """
    Check if a session already exists in the Supabase table.

    Args:
        sessionID: The session ID to check.
        table_name: The name of the Supabase table.

    Returns:
        bool: True if the session exists, False otherwise.
    """
    try:
        response = supabase.table(table_name).select('sessionID').eq('sessionID', sessionID).execute()
        return len(response.data) > 0
    except Exception as e:
        print(f"Error checking existing session: {e}")
        return False

def delete_existing_session(sessionID, table_name):
    """
    Delete an existing session from the Supabase table.

    Args:
        sessionID: The session ID to delete.
        table_name: The name of the Supabase table.

    Returns:
        None
    """
    try:
        supabase.table(table_name).delete().eq('sessionID', sessionID).execute()
    except Exception as e:
        print(f"Error deleting existing session: {e}")

def upload_data_no_duplicates(data, table_name, iso=True):
    """
    Uploads a list of dictionaries containing charging session data to Supabase.

    Args:
        data: A list of dictionaries with charging session information.
        table_name: The name of the Supabase table.
        iso: Boolean indicating if timestamps should be converted to ISO format.

    Returns:
        None
    """
    try:
        if iso:
            # Convert timestamps to ISO format
            for item in data:
                item["startTime"] = item["startTime"].isoformat()
                item["endTime"] = item["endTime"].isoformat()

        for item in data:
            sessionID = item['sessionID']
            if check_existing_session(sessionID, table_name):
                delete_existing_session(sessionID, table_name)
            # Insert the data into the table
            response = supabase.table(table_name).insert(item).execute()
        print(f"Successfully inserted {len(data)} rows into {table_name}!")

    except Exception as e:
        print(f"Error uploading data: {e}")

def upload_data(data, table_name, iso=True):
    """
    Uploads a list of dictionaries containing charging session data to Supabase.

    Args:
        data: A list of dictionaries with charging session information.
        table_name: The name of the Supabase table.
        iso: Boolean indicating if timestamps should be converted to ISO format.

    Returns:
        None
    """
    try:
        if iso:
            # Convert timestamps to ISO format
            for item in data:
                item["startTime"] = item["startTime"].isoformat()
                item["endTime"] = item["endTime"].isoformat()

        clear_table(table_name)

        # Insert the data into the table
        response = supabase.table(table_name).insert(data).execute()
        print(f"Successfully inserted {len(data)} rows into {table_name}!")

    except Exception as e:
        print(f"Error uploading data: {e}")

def upload_dataframe(df, table_name):
    """
    Uploads a DataFrame to Supabase.

    Args:
        df: A pandas DataFrame with the data to upload.
        table_name: The name of the Supabase table.

    Returns:
        None
    """
    try:
        data = df.to_dict(orient='records')
        upload_data(data, table_name, iso=False)
    except Exception as e:
        print(f"Error uploading DataFrame: {e}")

def upload_dataframe_no_duplicates(df, table_name):
    """
    Uploads a DataFrame to Supabase.

    Args:
        df: A pandas DataFrame with the data to upload.
        table_name: The name of the Supabase table.

    Returns:
        None
    """
    try:
        # Replace NaN values with None in userID column
        df['userID'] = df['userID'].replace({np.nan: None})

        data = df.to_dict(orient='records')
        upload_data_no_duplicates(data, table_name, iso=False)
    except Exception as e:
        print(f"Error uploading DataFrame: {e}")

def upload_unique_dataframe(df, table_name):
    """
    Uploads a DataFrame with unique session IDs to Supabase.

    Args:
        df: A pandas DataFrame with the data to upload.
        table_name: The name of the Supabase table.

    Returns:
        None
    """
    try:
        # Replace NaN values with None in userID column

        # Drop duplicates based on 'sessionID' column
        unique_df = df.drop_duplicates(subset='sessionID')

        # Convert DataFrame to list of dictionaries
        data = unique_df.to_dict(orient='records')

        # Delete all rows from the table
        clear_table(table_name)

        # Upload the new data
        response = supabase.table(table_name).insert(data).execute()
        print(f"Successfully inserted {len(data)} rows into {table_name}!")
    except Exception as e:
        print(f"Error uploading unique DataFrame: {e}")

def clear_table(table_name):
    """
    Delete all rows from the specified Supabase table based on the presence of 'id' or 'post_id' key.

    Args:
        table_name: The name of the Supabase table.

    Returns:
        None
    """
    try:
        # Fetch a sample row to check for column names
        response = supabase.table(table_name).select('*').limit(1).execute()
        if not response.data:
            print(f"No data found in {table_name}.")
            return

        # Check for presence of 'id' or 'post_id' keys
        column_names = response.data[0].keys()
        if 'id' in column_names:
            supabase.table(table_name).delete().neq('id', 0).execute()
            print(f"All rows with 'id' key deleted from {table_name}!")
        elif 'post_id' in column_names:
            supabase.table(table_name).delete().neq('post_id', '').execute()
            print(f"All rows with 'post_id' key deleted from {table_name}!")
        else:
            print(f"Neither 'id' nor 'post_id' keys found in {table_name}. Cannot perform deletion.")

    except Exception as e:
        print(f"Error deleting rows: {e}")

def more_records(response_data, response_list, tStart, tEnd):

    try:
        while response_data["MoreFlag"] == 1:
            start_record = len(response_list) + 1

            searchQuery = {
                'fromTimeStamp': tStart,
                'toTimeStamp': tEnd,
                'startRecord': start_record
            }

            response_data = client.service.getChargingSessionData(searchQuery)

            for session in response_data.ChargingSessionData:
                response_list.append(session)

    except KeyError:
         while response_data["moreFlag"] == 1:
            start_record = len(response_list) + 1

            searchQuery = {
                'fromTimeStamp': tStart,
                'toTimeStamp': tEnd,
                'startRecord': start_record
            }

            response_data = client.service.getChargingSessionData(searchQuery)

            for session in response_data.ChargingSessionData:
                response_list.append(session)

    return response_list

def more_records_stations(response_data, response_list):

    try:
        while response_data["MoreFlag"] == 1:
            start_record = len(response_list) + 1

            searchQuery = {
                'startRecord': start_record
            }

            response_data = client.service.getStations(searchQuery)

            for station in response_data.stationData:
                response_list.append(station)

    except KeyError:
         while response_data["moreFlag"] == 1:
            start_record = len(response_list) + 1

            searchQuery = {
                'startRecord': start_record
            }

            response_data = client.service.getStations(searchQuery)

            for station in response_data.stationData:
                response_list.append(station)

    return response_list

def check_faulty_num(df, post_id):

    faulty_count = 0

    for index, row in df.iterrows():
        if row['post_id'] == post_id:
            if row['Energy'] < min_energy_dispensed:
                faulty_count += 1
            elif row['power_delivery_rate'] < min_charge_rate:
                faulty_count += 1
            elif row['charging_duration_hours']*3600 < min_duration.total_seconds():
                faulty_count += 1

    return faulty_count

def parse_duration(duration_str):
    hours, minutes, seconds = map(int, duration_str.split(':'))
    return timedelta(hours=hours, minutes=minutes, seconds=seconds)


# ChargePoint API params
api_key = os.environ.get("API_KEY")
api_pw = os.environ.get("API_PW")
wsdl_url = os.environ.get("WSDL_URL")
supa_url = os.environ.get("SUPA_URL")
supa_key = os.environ.get("SUPA_KEY")

# Customer ID - Different for each customer
organization_id = "3f5bb216-1977-437e-a263-26c68cdc9229"

client = Client(wsdl_url, wsse=UsernameToken(api_key, api_pw))

# Fault Thresholds
min_charge_rate = 3 #kW
min_duration = timedelta(minutes=3) #min
min_energy_dispensed = 1 #kWh

# Get Stations Query
searchQuery = {}

station_data = client.service.getStations(searchQuery)

station_data_list = station_data.stationData

station_data_list = more_records_stations(station_data, station_data_list)


# Get Charging Sessions Query
tEnd = datetime.now()
tStart = tEnd - timedelta(days=30)

usageSearchQuery = {
    'fromTimeStamp': tStart,
    'toTimeStamp': tEnd,
}
session_data = client.service.getChargingSessionData(usageSearchQuery)

# Initialize supabase client instance
supabase: Supa_Client = create_client(supa_url, supa_key)


session_list = session_data.ChargingSessionData

session_list = more_records(session_data, session_list, tStart, tEnd)

outputDict = {}
outputRecords = []

for charging_session in session_list:
    stationID = charging_session['stationID']
    stationName = charging_session['stationName']
    portNumber = charging_session['portNumber']
    Address = charging_session['Address']
    City = charging_session['City']
    State = charging_session['State']
    Country = charging_session['Country']
    postalCode = charging_session['postalCode']
    sessionID = charging_session['sessionID']
    Energy = charging_session['Energy']
    startTime = charging_session['startTime']
    endTime = charging_session['endTime']
    userID = charging_session['userID']
    recordNumber = charging_session['recordNumber']
    credentialID = charging_session['credentialID']
    vehicleMake = charging_session["vehicleMake"]
    vehicleModel = charging_session["vehicleModel"]
    vehicleModelYear = charging_session["vehicleModelYear"]

    if vehicleMake or vehicleModel or vehicleModelYear is None:
        vehicle = "Unknown"
    else:
        vehicle = f"{vehicleModelYear} {vehicleMake} {vehicleModel}"

    startSOC = charging_session["startBatteryPercentage"]
    endSOC = charging_session["stopBatteryPercentage"]

    if startSOC + endSOC == 0:
        startSOC = "Unknown"
        endSOC = "Unknown"

    totalChargingDuration = charging_session["totalChargingDuration"]
    charging_duration_hours = parse_duration(totalChargingDuration)
    charging_duration_hours = int(charging_duration_hours.total_seconds()) / 3600

    totalSessionDuration = charging_session["totalSessionDuration"]
    time_delta_TCD = parse_duration(totalChargingDuration)
    time_delta_TSD = parse_duration(totalSessionDuration)

    idleTime = time_delta_TSD - time_delta_TCD
    total_seconds_idleTime = int(idleTime.total_seconds())
    idle_hours, idle_remainder = divmod(total_seconds_idleTime, 3600)
    idle_minutes, idle_seconds = divmod(idle_remainder, 60)

    if total_seconds_idleTime < 60:
        idleTime_str = "00:00:00"

    else:
        idleTime_str = f"{idle_hours:02}:{idle_minutes:02}:{idle_seconds:02}"

    if charging_duration_hours == 0:
        power_delivery_rate = 0
    else:
        power_delivery_rate = Energy / charging_duration_hours

    recordObj = {
        'stationID': stationID,
        'stationName': stationName,
        'portNumber': portNumber,
        'Address': Address,
        'City': City,
        'State': State,
        'Country': Country,
        'postalCode': postalCode,
        'sessionID': sessionID,
        'Energy': Energy,
        'startTime': startTime,
        'endTime': endTime,
        'userID': userID,
        'recordNumber': recordNumber,
        'credentialID': credentialID,
        'organization_id': organization_id,
        'vehicle': vehicle,
        'startSOC': startSOC,
        'endSOC': endSOC,
        'totalChargingDuration': totalChargingDuration,
        'totalSessionDuration': totalSessionDuration,
        'idleTime': idleTime_str,
        'charging_duration_hours': charging_duration_hours,
        'power_delivery_rate': power_delivery_rate
    }

    outputRecords.append(recordObj)

reversed_outputRecords = outputRecords[::-1]


# Upload session data
upload_data(reversed_outputRecords, 'chargepoint_sessions')

#Upload to perm database
response = supabase.table('chargepoint_sessions_internal').select('*').execute()
for record in reversed_outputRecords:
    duplicate = False
    for row in response.data:
        if str(record["sessionID"]) == str(row["sessionID"]):
            duplicate = True

    if not duplicate:
        supabase.table('chargepoint_sessions_internal').insert(record).execute()

print("\n")
print("**********Finished updating internal sessions table.***********")
print("\n")


########################################################### Faulty Sessions #####################################################

# Fetch from supabase
response_window = supabase.table('chargepoint_sessions').select('*').execute() #Fetch record from last 30 days
df = pd.DataFrame.from_dict(response_window.data)


# Calculate power delivery rate
df['power_delivery_rate'] = np.where(df['charging_duration_hours'] == 0, 0, df['Energy'] / df['charging_duration_hours'])


# Map number of ports to dataframe
station_dict = {station.stationID: station.numPorts for station in station_data_list}
df['number_of_ports'] = df['stationID'].map(station_dict)
## Map
## Additional
## Station params


# Check for sessions with no energy dispensed
df_error_no_discharge = df[df.Energy < min_energy_dispensed].copy()
df_error_no_discharge['message'] = df_error_no_discharge.apply(
    lambda x: f"Low energy: {round(x['Energy'], 2)}kWh by {x['sessionID']} at {x['startTime']}",
    axis=1
)
df_error_no_discharge['fault_type'] = 'No energy dispensed'

# Check for sessions with charging rate below 3 kW
df_error_low_power = df[df['power_delivery_rate'] < min_charge_rate].copy()
df_error_low_power['message'] = df_error_low_power.apply(
    lambda x: f"Low power delivery rate: {round(x['power_delivery_rate'], 2)} kW by {x['sessionID']} at {x['startTime']}",
    axis=1
)
df_error_low_power['fault_type'] = 'Low power delivery rate'

# Check for sessions below 3 mins
df_error_short_duration = df[pd.to_datetime(df['endTime']) - pd.to_datetime(df['startTime']) < min_duration].copy()
df_error_short_duration['message'] = df_error_short_duration.apply(
    lambda x: f"Short duration ({pd.to_datetime(x['endTime']) - pd.to_datetime(x['startTime'])}) by {x['sessionID']} at {x['startTime']}",
    axis=1
)
df_error_short_duration['fault_type'] = 'Short duration'

# Combine all errors
df_errors = pd.concat([df_error_no_discharge, df_error_low_power, df_error_short_duration])

df_errors['userID'] = df_errors['userID'].replace({np.nan: None})

df_errors['organization_id'] = organization_id  # Add customer_id here

df_errors_unique = df_errors.drop_duplicates(subset='sessionID')
upload_unique_dataframe(df_errors_unique.sort_values(by='startTime', ascending=False), 'chargepoint_faulty_sessions')



# Upload to perm database
df_errors_unique_noid = df_errors_unique.drop(columns=['id'])  # Drop 'id' column
faulty_records = df_errors_unique_noid.sort_values(by='startTime', ascending=False).to_dict(orient='records')

# Insert unique records into the table
for record in faulty_records:
    # Ensure 'id' is not included
    record_to_insert = {k: v for k, v in record.items() if k != 'id'}

    # Upsert record based on sessionID to handle duplicates automatically
    supabase.table('chargepoint_faulty_sessions_internal').upsert(record_to_insert, on_conflict=['sessionID']).execute()

print("\n**********Finished updating internal faulty sessions table.***********\n")


########################################################### Faulty Post Table ############################################################


#Session faults (no/low energy, short duration)

cutoff_48hr = datetime.utcnow() - timedelta(hours=48)
df_temp = df.copy()
df_temp['startTime'] = pd.to_datetime(df_temp['startTime']).dt.tz_convert(None)
df_48hr = df_temp[df_temp['startTime'] > cutoff_48hr]


df_faulty_stations = df_errors_unique.groupby(['stationID', 'portNumber']).agg({
    'stationName': 'first',
    'Address': 'first',
    'City': 'first',
    'State': 'first',
    'Country': 'first',
    'fault_type': lambda x: ', '.join(x.unique()),  # Combine unique fault types
    'message': lambda x: '; '.join(x)  # Combine all messages
}).reset_index()

df_faulty_stations['post_id'] = df_faulty_stations['stationID'].astype(str) + '_' + df_faulty_stations['portNumber'].astype(str)
columns = ['post_id'] + [col for col in df_faulty_stations.columns if col != 'post_id']
df_faulty_stations = df_faulty_stations[columns]

df_faulty_stations['organization_id'] = organization_id  # Add customer_id here

#Site faults (unused posts)

df_missing_posts = df_48hr.groupby('stationID').agg({
    'stationName': 'first',
    'Address': 'first',
    'City': 'first',
    'State': 'first',
    'Country': 'first',
    'portNumber': lambda x: list(x.unique()),
    'number_of_ports': 'first'
}).reset_index()

df_missing_posts.rename(columns={'portNumber': 'port_list'}, inplace=True)
df_missing_posts = df_missing_posts[df_missing_posts['port_list'].apply(len) < df_missing_posts['number_of_ports']]

df_missing_posts['missing_port_ids'] = None

for index, row in df_missing_posts.iterrows():
    missing_ports = []

    for port in range(1, row['number_of_ports'] + 1):
        if port not in row['port_list']:
            missing_ports.append(port)

    df_missing_posts.at[index, 'missing_port_ids'] = missing_ports


expanded_rows = []

for index, row in df_missing_posts.iterrows():
    for port in row['missing_port_ids']:
        new_row = {
            'stationID': row['stationID'],
            'portNumber': port,
            'stationName': row['stationName'],
            'Address': row['Address'],
            'City': row['City'],
            'State': row['State'],
            'Country': row['Country'],
            'fault_type': 'No charging session',
            'message': f"Port {port} of station {row['stationID']} did not record a session in the defined timeframe",
            'organization_id': organization_id  # Add customer_id here
        }
        expanded_rows.append(new_row)

df_missing_ports_expanded = pd.DataFrame(expanded_rows)


df_missing_ports_expanded['post_id'] = df_missing_ports_expanded['stationID'].astype(str) + '_' + df_missing_ports_expanded['portNumber'].astype(str)
columns = ['post_id'] + [col for col in df_missing_ports_expanded.columns if col != 'post_id']
df_missing_ports_expanded = df_missing_ports_expanded[columns]

df_combined_faulty_posts = pd.concat([df_faulty_stations, df_missing_ports_expanded], ignore_index=True)

## Df merge for serial# and other station params

station_param_df = pd.DataFrame.from_dict(station_data_list)

station_data_list_df = []
for station in station_data_list:
    recordObj = { 'stationID': station['stationID'],
                  'stationManufacturer': station['stationManufacturer'],
                  'stationModel': station['stationModel'],
                  'stationSerialNum': station['stationSerialNum'],
    }

    station_data_list_df.append(recordObj)

station_param_df = pd.DataFrame.from_dict(station_data_list_df)

df_combined_faulty_posts = pd.merge(df_combined_faulty_posts, station_param_df, on='stationID', how='left')
df_combined_faulty_posts['portNumber'] = df_combined_faulty_posts['portNumber'].astype(str)


###########Add Posts from Alarms
alarm_data = supabase.table('chargepoint_alarms').select('*').execute()
alarm_data_df = pd.DataFrame.from_dict(alarm_data.data)
alarm_data_df ['post_id'] = alarm_data_df['stationID'].astype(str) + "_" + alarm_data_df['portNumber'].astype(str)

alarm_data_posts = alarm_data_df.groupby('stationID').apply(
    lambda x: list(zip(x['alarmType'], x['alarmTime']))
).reset_index(name='alarms_list')

# Extract additional columns (taking the first row for each post_id)
additional_columns = alarm_data_df.groupby('stationID').agg({
    'stationName': 'first',
    'portNumber': 'first',
    'alarmType': lambda x: list(x.unique()),
    'stationManufacturer': 'first',
    'stationModel': 'first',
    'stationSerialNum': 'first'
}).reset_index()

# Merge the grouped alarms_list with the additional columns
alarm_data_posts = pd.merge(alarm_data_posts, additional_columns, on='stationID')
alarm_data_posts['fault_type'] = None

alarms_faulty_list = []
# Iterate through the rows of the alarm_data_posts dataframe
for _, row in alarm_data_posts.iterrows():
    alarms_list = row['alarms_list']
    alarm_type = row['alarmType']  # Changed to alarmType

    # Initialize an empty set to store unique fault types
    fault_types = set()
    messages = []

    # Convert alarmTime strings to datetime for comparison, handling the 'T' separator
    alarms_with_time = []
    for alarm, time in alarms_list:
        try:
            # Handle the format with 'T'
            alarms_with_time.append((alarm, datetime.strptime(time, '%Y-%m-%dT%H:%M:%S')))
        except ValueError:
            # If it doesn't match, raise an error or handle appropriately
            print(f"Error parsing time: {time}")

    # 1. Boot Loop check (3+ "Boot Up" or "Bootup Due to POWER ON" alarms in 48 hours)
    boot_alarms = [time for alarm, time in alarms_with_time if alarm in ['Boot Up', 'Bootup Due to POWER ON']]

    # Sort boot_alarms by time to facilitate sliding window logic
    boot_alarms.sort()

    # Check for any 48-hour window where there are 3 or more boot alarms
    for i in range(len(boot_alarms) - 2):  # We need at least 3 alarms, so stop at len(boot_alarms) - 2
        if boot_alarms[i + 2] - boot_alarms[i] <= timedelta(hours=48):
            fault_types.add('Boot Loop')
            messages.append(f"Charger stuck in a boot loop")
            break  # Exit the loop since we've already identified the boot loop

    # 2. Circuit Sharing Balance check (3+ "Circuit Sharing Load Increased" or "Circuit Sharing Load Decreased" in a week)
    circuit_sharing_alarms = [time for alarm, time in alarms_with_time if alarm in ['Circuit Sharing Load Increased', 'Circuit Sharing Load Decreased']]
    if len(circuit_sharing_alarms) >= 3:
        circuit_sharing_alarms.sort()
        if circuit_sharing_alarms[-1] - circuit_sharing_alarms[0] <= timedelta(weeks=1):
            fault_types.add('Circuit Sharing Balance')
            messages.append(f"Circuit sharing load changed {len(circuit_sharing_alarms)} times in the last month")

    # 3. Powered Off check (3+ "Powered Off" alarms in 48 hours)
    powered_off_alarms = [time for alarm, time in alarms_with_time if alarm == 'Powered Off']

    # Sort powered_off_alarms by time to facilitate sliding window logic
    powered_off_alarms.sort()

    # Check for any 48-hour window where there are 3 or more powered off alarms
    for i in range(len(powered_off_alarms) - 2):  # We need at least 3 alarms, so stop at len(powered_off_alarms) - 2
        if powered_off_alarms[i + 2] - powered_off_alarms[i] <= timedelta(hours=48):
            fault_types.add('Powered Off')
            messages.append(f"Station powered off {len(powered_off_alarms)} times in 48 hours")
            break  # Exit the loop since we've already identified the powered off condition

    # 4. Earth Fault station out of service check
    if 'Earth Fault station out of service' in alarm_type:
        fault_types.add('Earth Fault station out of service')
        messages.append('Earth Fault station out of service')

    # 5. Capacitor faulted check
    if 'Last gasp capacitor is dead' in alarm_type:
        fault_types.add('Capacitor faulted')
        messages.append('Last gasp capacitor is dead')

    # 6. Relay Stuck Closed check
    if 'Relay Stuck Closed' in alarm_type:
        fault_types.add('Relay Stuck Closed')
        messages.append('Relay Stuck Closed')

    # 7. Unreachable check (contains "Unreachable" but does not contain "Reachable")
    if 'Unreachable' in alarm_type and 'Reachable' not in alarm_type:
        fault_types.add('Unreachable')
        messages.append('Charger unreachable')

    # 8. If ACB inoperational or unreachable or communication lost
    if ('ACB not Operational' in alarm_type) or ('ACB unreachable' in alarm_type) or ('Communication to ACB Lost' in alarm_type):
        fault_types.add('Air Circuit Breaker Fault')
        messages.append('Air Circuit Breaker (ACB) faulted')

    # 9. If cable lost etherner comm
    if 'Cable lost Ethernet communication with system' in alarm_type:
        fault_types.add('Ethernet Communication Lost')
        messages.append('Ethernet communication lost')

    #10. General hardware error 
    if 'Charging is stopped due to missing or non-functional hardware' in alarm_type:
        fault_types.add('Interrupted Due to Hardware Fault')
        messages.append('Charging stopped due to missing or non-functional hardware')


    # If any fault types were found, combine them and append the row to alarms_faulty_list
    if fault_types:
        row['fault_type'] = ', '.join(fault_types)  # Combine fault types as a comma-separated string
        row['message'] = ' | '.join(messages)  # Combine messages as a string
        alarms_faulty_list.append(row)

alarms_faulty_df = pd.DataFrame.from_dict(alarms_faulty_list)
alarms_faulty_df.drop(columns=['alarms_list', 'alarmType'], inplace=True)
alarms_faulty_df["post_id"] = alarms_faulty_df["stationID"].astype(str) + "_" + alarms_faulty_df["portNumber"].astype(str)
alarms_faulty_df['State'] = "N/A"
alarms_faulty_df['Country'] = "N/A"
alarms_faulty_df['Address'] = "N/A"
alarms_faulty_df['City'] = "N/A"
alarms_faulty_df['organization_id'] = organization_id



#Add CoralEV
coral_data = supabase.table('epic_faulty_posts').select('*').execute()
coral_data_df = pd.DataFrame.from_dict(coral_data.data)
coral_data_df["post_id"] = coral_data_df["stationID"].astype(str) + "_" + coral_data_df["portNumber"].astype(str)

coral_data_df.drop(columns=['id', 'created_at'], inplace=True)

combined_df_final = pd.concat([df_combined_faulty_posts, coral_data_df], ignore_index=True)


combined_df_final = pd.concat([combined_df_final, alarms_faulty_df], ignore_index=True)

#Filter out stations with successful sessions
now = datetime.utcnow()
cutoff = now - timedelta(days=7)

sessions_last_7_days = supabase.table('chargepoint_sessions') \
    .select('*') \
    .gte('startTime', cutoff.isoformat()) \
    .execute().data


epic_sessions_last_7_days = supabase.table('epic_sessions') \
    .select('*') \
    .gte('start_timestamp', cutoff.isoformat()) \
    .execute().data


df_last_7_days = pd.DataFrame.from_dict(sessions_last_7_days)
df_last_7_days_epic = pd.DataFrame.from_dict(epic_sessions_last_7_days)

df_last_7_days_epic['start_timestamp'] = pd.to_datetime(df_last_7_days_epic['start_timestamp'])
df_last_7_days_epic['timestampz'] = df_last_7_days_epic['start_timestamp'].dt.tz_localize('UTC')
df_last_7_days_epic['timestampz'] = df_last_7_days_epic['timestampz'].dt.strftime('%Y-%m-%d %H:%M:%S%z')
df_last_7_days_epic['timestampz'] = df_last_7_days_epic['timestampz'].str.replace(r'(\+00)(00)', r'\1', regex=True)


df_last_7_days_epic.drop(columns=['id', 'created_at', 'cost', 'revenue', 'profit', 'peak_power' ], inplace=True)
df_last_7_days.drop(columns=['id', 'created_at'], inplace=True)
df_last_7_days_epic.rename(columns={'postID': 'stationID', 'connected_port': 'portNumber'}, inplace=True)



df_last_7_days = pd.concat([df_last_7_days, df_last_7_days_epic], ignore_index=True)
df_last_7_days['post_id'] = df_last_7_days['stationID'].astype(str) + '_' + df_last_7_days['portNumber'].astype(str)
df_last_7_days['power_delivery_rate'] = df_last_7_days['Energy'] / df_last_7_days['charging_duration_hours']


combined_df_final = combined_df_final.groupby('post_id').agg({
    'portNumber': 'first',
    'stationID': 'first',
    'stationName': 'first',
    'Address': 'first',
    'City': 'first',
    'State': 'first',
    'Country': 'first',
    'fault_type': lambda x: ', '.join(x.unique()),  # Combine unique fault types
    'message': lambda x: '; '.join(x),  # Combine all messages
    'organization_id': 'first',
    'stationManufacturer': 'first',
    'stationModel': 'first',
    'stationSerialNum': 'first'
}).reset_index()


combined_df_final['sessions_count_last_7_days'] = 0

for index, row in combined_df_final.iterrows():

    session_count = df_last_7_days[df_last_7_days['post_id'] == row['post_id']].shape[0]

    combined_df_final.at[index, 'sessions_count_last_7_days'] = session_count


for index, row in combined_df_final.iterrows():
    number_faulty = check_faulty_num(df_last_7_days, row['post_id'])
    combined_df_final.at[index, 'faulty_sessions_last_7_days'] = number_faulty

combined_df_final['faulty_sessions_last_7_days'] = combined_df_final['faulty_sessions_last_7_days'].astype(int)


combined_df_final['faulty_session_ratio'] = np.where(
    combined_df_final['sessions_count_last_7_days'] == 0,
    1,
    round(combined_df_final['faulty_sessions_last_7_days'] / combined_df_final['sessions_count_last_7_days'], 2)
)

# Set severity
# Modify severity_condition_critical to include rows where sessions_count_last_7_days equals 0
severity_condition_critical = (
    (combined_df_final['fault_type'].isin(['Faulted', 'Offline', 'Earth Fault station out of service', 'Capacitor faulted', 'Interrupted Due to Hardware Fault', 'Air Circuit Breaker Fault'])) |
    ((combined_df_final['faulty_session_ratio'] >= 0.25) & (combined_df_final['sessions_count_last_7_days'] >2))|
    (combined_df_final['sessions_count_last_7_days'] == 0)
)

# Condition for "Warning" if sessions_count_last_7_days is > 0 and <= 2
severity_condition_warning_low_sessions = (
    (combined_df_final['sessions_count_last_7_days'] > 0) &
    (combined_df_final['sessions_count_last_7_days'] <= 2)
)

# Condition for "Low Concern" if faulty_session_ratio is less than 0.1
severity_condition_low_concern = (
    combined_df_final['faulty_session_ratio'] < 0.1
)

# Use np.select to set the severity column based on conditions
conditions = [severity_condition_critical, severity_condition_warning_low_sessions, severity_condition_low_concern]
choices = ['Critical', 'Warning', 'Low Concern']

# Set the default to "Warning" if none of the conditions are met
combined_df_final['severity'] = np.select(conditions, choices, default='Warning')




#Change displayed categorization
# Define conditions for the fault_displayed column
fault_condition_critical = combined_df_final["fault_type"].isin(['Faulted', 'Offline', 'Earth Fault station out of service', 'Capacitor faulted', 'Air Circuit Breaker Fault', 'Interrupted Due to Hardware Fault'])
sessions_condition_low = combined_df_final["sessions_count_last_7_days"] <= 2

# Define the choices for each condition
fault_displayed_critical = combined_df_final["fault_type"]

fault_displayed_low_sessions = combined_df_final["sessions_count_last_7_days"].apply(
    lambda x: f"{x} sessions recorded in last week following alert"
)

fault_displayed_alarm_ratio = combined_df_final["faulty_session_ratio"].apply(
    lambda x: f"{np.ceil(x * 100):.0f}% of sessions triggered Alarms"
)

# Use np.select to apply the conditions and assign values to the new column
combined_df_final["fault_displayed"] = np.select(
    [fault_condition_critical, sessions_condition_low],
    [fault_displayed_critical, fault_displayed_low_sessions],
    default=fault_displayed_alarm_ratio
)


combined_df_final['portNumber'] = combined_df_final['portNumber'].fillna('N/A')


upload_dataframe(combined_df_final, "chargepoint_faulty_posts")

#Upload to perm database
faulty_posts_records = df_combined_faulty_posts.to_dict(orient='records')
faulty_posts_response = supabase.table('chargepoint_faulty_posts_internal').select('*').execute()
for record in faulty_posts_records:
    duplicate = False
    for row in faulty_posts_response.data:
        if record["stationID"] == row["stationID"]:
            duplicate = True

    if not duplicate:
        supabase.table('chargepoint_faulty_posts_internal').insert(record).execute()

print("**********Finished updating internal faulty posts table.***********")
print("\n")


















