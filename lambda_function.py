import requests
import json
import time
from collections import defaultdict
import boto3
from io import StringIO
import csv

# Params to control rapid fetch requests
max_retries = 3
timeout = 10
backoff_factor = 0.5

# Helper functions
# 1. API request fetching w/ params above
def fetch_url(url):
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=timeout)
            # response.raise_for_status()  # Raise an error for bad status codes
            return response
        except requests.exceptions.Timeout:
            print(f"Attempt {attempt + 1} of {max_retries} timed out. Retrying...")
        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt + 1} of {max_retries} failed with error: {e}")
            break  # For other types of exceptions, break out of the loop
        
        time.sleep(backoff_factor * (2 ** attempt))  # Exponential backoff
    return None

# 2. Format seasons_played variable
def convert_season(season):
    if season == 'Rookie':
      return 1
    else:
      ans = ''
      for i in season:
        if i.isdigit():
          ans += i
        else:
          break
      return int(ans)

# Format player height variable / ft. in. --> in.
def convert_height(height):
    parts = height.split()
    feet = int(parts[0].replace("'", ""))
    inches = int(parts[1].replace('"', ""))
    total_inches = feet * 12 + inches
    return total_inches

# Format player weight
def convert_weight(weight):
    return int(weight.split()[0])

# Grab all pitchers from each team through depth chart
# Ex. URL: https://www.espn.com/mlb/team/depth/_/name/sf
def players_from_depth_chart(active_pitchers=set()):
    # List of team abbreviations used in URLs
    teams = ['ari', 'atl', 'bal', 'bos', 'chc', 'chw', 'cin', 'cle', 'col', 'det', 'hou',
         'kc', 'laa', 'lad', 'mia', 'mil', 'min', 'nym', 'nyy', 'oak', 'phi', 'pit',
         'sd', 'sea', 'stl', 'sf', 'tb', 'tex', 'tor', 'wsh']
    
    # Fetch every team roster to grab all designated pitchers for the current year
    for team in teams:
        url = f'https://site.web.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/{team}/depthcharts?region=us&lang=en'

        response = fetch_url(url)
        if response.status_code == 200:
            data = json.loads(response.text)
        else:
            print(f"Failed to retrieve the individual page. Status code: {response.status_code}")
            return None

        athletes = data['depthchart'][0]['positions']['rp']['athletes'] + data['depthchart'][0]['positions']['p']['athletes']
        for player in athletes:
            active_pitchers.add((player['id'], player['displayName']))
    
    # Returns a list of all eligible active pitchers
    return active_pitchers

# Grab individual player info from id gathered in players_from_depth_chart
def get_player_data(player_id, player_info):
    url = f"http://site.api.espn.com/apis/common/v3/sports/baseball/mlb/athletes/{player_id}"
    response = fetch_url(url)

    # JSON response was analyzed and the following fields were determined viable
    if response.status_code == 200:
        print(f"Successfully fetched data for player {player_id}")
        player_data = response.json()
        player_info[player_id].append(int(player_data.get('athlete').get('id')))
        player_info[player_id].append(player_data.get('athlete').get('firstName'))
        player_info[player_id].append(player_data.get('athlete').get('lastName'))
        player_info[player_id].append(player_data.get('athlete').get('debutYear'))
        player_info[player_id].append(int(player_data.get('athlete').get('jersey')))
        player_info[player_id].append(player_data.get('athlete').get('position').get('displayName'))
        player_info[player_id].append(player_data.get('athlete').get('position').get('abbreviation'))
        player_info[player_id].append(int(player_data.get('athlete').get('team').get('id')))
        player_info[player_id].append(player_data.get('athlete').get('team').get('abbreviation'))
        player_info[player_id].append(player_data.get('athlete').get('team').get('displayName'))
        player_info[player_id].append(int(player_data.get('athlete').get('team').get('isAllStar') == True))
        player_info[player_id].append(convert_height(player_data.get('athlete').get('displayHeight')))
        player_info[player_id].append(convert_weight(player_data.get('athlete').get('displayWeight')))
        player_info[player_id].append(player_data.get('athlete').get('displayDOB').replace('/', '-'))
        player_info[player_id].append(player_data.get('athlete').get('age'))
        player_info[player_id].append(convert_season(player_data.get('athlete').get('displayExperience')))
        return player_data
    else:
        print(f"Failed to fetch data: {response.status_code}")
        return None

# AWS main function
def lambda_handler(event, context):
    # Write data to the following S3 csv
    bucket_name = 'mlb-pitcher-data'
    file_name = 'dim_mlb_pitcher_df.csv'

    active_pitchers = players_from_depth_chart()
    player_info = defaultdict(list)
    for player in active_pitchers:
        get_player_data(player[0], player_info)

    data = player_info.values()
    header = [
        'id', 
        'first_name', 
        'last_name', 
        'debut_year', 
        'jersey', 
        'position_name', 
        'position_abbreviation', 
        'team_id', 
        'team_abbreviation', 
        'team_name', 
        'is_all_star', 
        'height_inches', 
        'weight_lbs', 
        'date_of_birth', 
        'age', 
        'seasons_played'
    ]

    csv_buffer = StringIO()
    csv_writer = csv.writer(csv_buffer)
    csv_writer.writerow(header)
    csv_writer.writerows(data)

    s3 = boto3.client('s3') 
    s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=file_name)
    
    return {
        'statusCode': 200,
        'body': 'CSV file uploaded successfully to S3'
    }