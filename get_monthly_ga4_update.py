#!/usr/bin/env python

# Copyright 2021 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Google Analytics Data API sample application demonstrating the batch creation
of multiple reports.
See https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/properties/batchRunReports
for more information.
"""
# [START analyticsdata_run_batch_report]
# Starting point using Google's code to develop our own reporting code
import datetime as dt
import pandas as pd
import numpy as np

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    BatchRunReportsRequest, # which report request do we run?
    DateRange,
    Dimension,
    Metric,
    RunReportRequest, # this is the other request?
)
#import pdb
#
# see if old analytics file is there and load it:
mypath='/home/flbahr/webdata/'
myfile='ga4_monthly_analytics_new.csv'
mynow=dt.datetime.now()
try:
        olddataframe=pd.read_csv(mypath+myfile)
        lastmonth=olddataframe['month'].iloc[-1]
        # need to worry about january and year change
        if mynow.month < lastmonth:
                mystart=dt.datetime(mynow.year-1,lastmonth,1)
        else:
                mystart=dt.datetime(mynow.year,lastmonth,1)
        nodataframe=0
except:
        mystart=mynow-dt.timedelta(days=31)
        nodataframe=1
#
# get last value and use that for start point
#
#mynow=dt.datetime.now()
#mystart=mynow-dt.timedelta(days=31)
syear=str(mystart.year)
smon=str(mystart.month)
sday=str(mystart.day)
#
# CeNCOOS property ID
property_id=311670892
# create google data api client?
# set path to credentials
credentials_json_path="/home/flbahr/CeNCOOS-GA4-031d7df9dd8c.json" # put in correct json file name
#client=BetaAnalyticsDataClient().from_service_json(credentials_json_path)
client=BetaAnalyticsDataClient()
# example code from google
#dimension=[Dimension(name="city")], # example don't want this
#metrics=[Metric(name="activeUsers")],
#date_ranges=[DateRange(start_date="YYYY-MM-DD", end_date="today")],
mydaterange=[DateRange(start_date=syear+"-"+smon+"-"+sday, end_date="today")]
mydimensions=[Dimension(name="month"),
              Dimension(name="year")] # this will output only a month data
#mydimensions=[Dimension(name="date")] # this will output daily data
mymetrics=[
Metric(name="totalUsers"),
Metric(name="newUsers"),
Metric(name="sessions"),
Metric(name="bounceRate"),
Metric(name="averageSessionDuration"),
Metric(name="active28DayUsers"),
Metric(name="activeUsers"),
Metric(name="ScreenPageViewsPerSession"),
#Metric(name="year"),
#Metric(name="entrances"), # entrances doesn't exist in ga4
Metric(name="screenPageViews")]
#
# Make the request from Google
# use the metrics above and the daterange above
#
requests=RunReportRequest(
    property=f"properties/{property_id}",
    dimensions=mydimensions,
    metrics=mymetrics,
    date_ranges=mydaterange,
    )
response=client.run_report(requests)
dim_len=len(response.dimension_headers)
metric_len=len(response.metric_headers)
all_data=[]
for row in response.rows:
	row_data={}
	for i in range(0,dim_len):
		row_data.update({response.dimension_headers[i].name: row.dimension_values[i].value})
	for i in range(0, metric_len):
		row_data.update({response.metric_headers[i].name: row.metric_values[i].value})
	all_data.append(row_data)

newdataframe=pd.DataFrame(all_data)
x=newdataframe.sort_values('month').copy(deep=True)
spv=x['screenPageViews']
sess=x['sessions']
nuser=x['newUsers']
totu=x['totalUsers']
# sessions does not equate to number of users....
notnewu=totu.astype(int)-nuser.astype(int) # number of returing users
pctnu=nuser.astype(int)/sess.astype(int)
avgsess_per_user=sess.astype(int)/totu.astype(int)
avg_not_newsessions=avgsess_per_user*notnewu
newsess=sess.astype(int)-avg_not_newsessions
avg_percent_newsessions=newsess/sess.astype(int)
#
# okay need to append to old data frame and possibly replace the last element
#
#pdb.set_trace()
out_df=x[['month','year','sessions','totalUsers']].copy(deep=True)
#out_df['month']=pd.to_datetime(out_df['date'])
out_df['sessions']=out_df['sessions'].astype(str)
out_df['totalUsers']=out_df['totalUsers'].astype(str)
# this isn't what we really want...
# need to find location of last month in olddataframe and update
# need to append the rest of out_df to olddataframe
#
#
#thelastmonth=mystart.month
#thelastyear=mystart.year
# okay we need to find the index in the new dataframe of the last month in the old dataframe
thelastmonth=olddataframe['month'].iloc[-1]
thelastyear=olddataframe['year'].iloc[-1]
# okay now find the index of this location in the new dataframe
therowineed=x[(x['month'].astype(int)==thelastmonth)&(x['year'].astype(int)==thelastyear)].index.to_numpy()
therow=therowineed[0]
# now replace the last values in the olddataframe with the new values
olddataframe['sessions'].iloc[-1]=x['sessions'][therow]
olddataframe['totalUsers'].iloc[-1]=x['totalUsers'][therow]
#
# rows to drop
#
rows_to_remove=np.arange(0,therow+1)
out_df=out_df.drop(rows_to_remove)
# now merge dataframes
olddataframe=pd.concat([olddataframe,out_df],ignore_index=True)
# now print out to a csv file!
#olddataframe.to_csv('/home/flbahr/test/ga4_monthly_analytics_update.csv',index=False)
olddataframe.to_csv('/home/flbahr/test/ga4_monthly_analytics_new.csv',index=False)

