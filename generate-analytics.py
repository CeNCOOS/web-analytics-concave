import pandas as pd
import os
from googleapiclient.discovery import build
from google.oauth2 import service_account
import logging

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
BASE_DIR = '/home/pdaniel/web-analytics/'


def initialize_analyticsreporting():
    """Establish a connection with the google reporting API V4"""
    credentials = service_account.Credentials.from_service_account_file(KEY_FILE_LOCATION)
    analytics = build('analyticsreporting', 'v4', credentials=credentials, cache_discovery=False)
    return analytics

def print_response(response):
    list = []
    # get report data
    for report in response.get('reports', []):
    # set column headers
        columnHeader = report.get('columnHeader', {})
        dimensionHeaders = columnHeader.get('dimensions', [])
        metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
        rows = report.get('data', {}).get('rows', [])

    for row in rows:
        # create dict for each row
        dict = {}
        dimensions = row.get('dimensions', [])
        dateRangeValues = row.get('metrics', [])

        # fill dict with dimension header (key) and dimension value (value)
        for header, dimension in zip(dimensionHeaders, dimensions):
            dict[header] = dimension

        # fill dict with metric header (key) and metric value (value)
        for i, values in enumerate(dateRangeValues):
            for metric, value in zip(metricHeaders, values.get('values')):
            #set int as int, float a float
                if ',' in value or '.' in value:
                    dict[metric.get('name')] = float(value)
                else:
                    dict[metric.get('name')] = int(value)

        list.append(dict)

    df = pd.DataFrame(list)
    return df


def get_report(analytics,metric):
    return analytics.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': VIEW_ID,
          'dimensions': [{'name': 'ga:date'}],
          'metrics': [{'expression': metric}],
          'dateRanges': [{'startDate': '30daysAgo', 'endDate': 'today'}]
        }]
      }
  ).execute()

def write_dataframe(df):
    out_df = df[['date','sessions','users']].copy(deep=True)
    out_df['date'] = pd.to_datetime(out_df['date'])
    out_df['sessions'] = out_df['sessions'].astype(str)
    out_df['users'] = out_df['users'].astype(str)
    out_df.to_csv(os.path.join(BASE_DIR,FILE_NAME),index=False)

def build_dataframe():
    metrics = [
        "ga:users",
        "ga:newUsers",
        "ga:percentNewSessions",
        "ga:sessions",
        "ga:bounces",
        "ga:sessionDuration",
        "ga:entrances",
        "ga:pageviews",
        "ga:pageviewsPerSession"
    ]
    for i, metric in enumerate(metrics):
        """ Build a dataframe for each varaible """
        response = get_report(initialize_analyticsreporting(),metric) # read data from a JSON format
        if i == 0:
            df = print_response(response)
        else:
            df[metric] = print_response(response)[metric]
            
    for col in df.columns:
        new_name = col.split(":")[-1]
        df.rename(columns={col:new_name},inplace=True)
    write_dataframe(df)


def copy_file_to_webserver():
    """Copy images from model runs to webserver where they can be viewed publically."""
    try:
        os.system('scp -i /etc/ssh/keys/pdaniel/scp_rsa {} skyrocket8.mbari.org:/var/www/html/data/web-analytics/ '.format(os.path.join(BASE_DIR,FILE_NAME)))
    except:
        logging.debug('Unabled to Copy Analytics File to Skyrocket')


if __name__ == "__main__":
    KEY_FILE_LOCATION = os.path.join(BASE_DIR,'keys/cencoos-web-analytics-5303bfd7dcbb.json')
    VIEW_ID = '10796414'
    FILE_NAME = 'analytics-data.csv'
    build_dataframe()
    copy_file_to_webserver()

    # Run again for Data Services
    KEY_FILE_LOCATION = os.path.join(BASE_DIR,'keys/resounding-axe-293817-00844a9aafb0.json')
    VIEW_ID = "180542384"
    FILE_NAME = 'portal-analytics-data.csv'
    build_dataframe()
    copy_file_to_webserver()