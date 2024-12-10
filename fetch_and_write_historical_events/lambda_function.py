# Retrieve all baseball sport event id

import requests
from datetime import datetime, timedelta
import json
import boto3
import time

SPORT = 'baseball_mlb'
API_KEY = ''

# 2023 MLB regular season schedule
SEASON_START_DATE = datetime(2023, 3, 30)
SEASON_END_DATE = datetime(2023, 10, 1)

def fetch_and_write(bucket_name, s3):

    current_date = SEASON_START_DATE

    while current_date <= SEASON_END_DATE:
        time.sleep(0.5)
        DATE = current_date.isoformat() + "Z"
        url = f'https://api.the-odds-api.com/v4/historical/sports/{SPORT}/events?apiKey={API_KEY}&date={DATE}'
        response = requests.get(url)
        if response.status_code == 200:
            print(f"Successfully fetched data for date: {DATE}")
            data = response.json()
        else:
            print(f"Failed to fetch data for date: {DATE}")
            print(f"Status code: {response.status_code}")
            print(f"Response content: {response.text}")

        # Save the data to a JSON file
        DATE = current_date.strftime('%Y-%m-%d')
        s3.put_object(
            Bucket=bucket_name,
            Key='game-eventId' + '/' + f'{DATE}.json',
            Body=json.dumps(data, indent=4)
        )

        # Move to the next day
        current_date += timedelta(days=1)

# AWS main function
def lambda_handler(event, context):
    # Write data to the following S3 csv
    bucket_name = 'mlb-odds-api-data'

    s3 = boto3.client('s3') 

    fetch_and_write(bucket_name, s3)
    
    return {
        'statusCode': 200,
        'body': 'JSON files uploaded successfully to S3'
    }
