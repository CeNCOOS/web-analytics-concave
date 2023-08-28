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
import pdb
#
# data file path
mypath='/home/flbahr/webdata/'
# data file name
myfile='ga4_daily_analytics.csv'
mynow=dt.datetime.now()
try:
        olddataframe=pd.read_csv(mypath+myfile)
        lastdate=pd.to_datetime(olddataframe['date'].iloc[-1],format='%Y-%m-%d')
        mystart=lastdate
        nodataframe=0
except:
        mystart=mynow-dt.timedelta(days=32)
        nodataframe=1
#pdb.set_trace()
mystart=mynow-dt.timedelta(days=31)
syear=str(mystart.year)
smon=str(mystart.month)
sday=str(mystart.day)
# CeNCOOS property ID
property_id=your_property_id
# create google data api client?
# set path to credentials
credentials_json_path="your credentials file" # put in correct json file name
#client=BetaAnalyticsDataClient().from_service_json(credentials_json_path)
client=BetaAnalyticsDataClient()
# example code from google
#dimension=[Dimension(name="city")], # example don't want this
#metrics=[Metric(name="activeUsers")],
#date_ranges=[DateRange(start_date="YYYY-MM-DD", end_date="today")],
mydaterange=[DateRange(start_date=syear+"-"+smon+"-"+sday, end_date="today")]
mydimensions=[Dimension(name="date")]
mymetrics=[
Metric(name="totalUsers"),
Metric(name="newUsers"),
Metric(name="sessions"),
Metric(name="bounceRate"),
Metric(name="averageSessionDuration"),
#Metric(name="sessionsPerUser"), # not returning! Sessions/Active users
Metric(name="screenPageViewsPerSession"),
Metric(name="screenPageViews")]
# Metric(name="active28DayUsers"),
#Metric(name="entrances"), # entrances doesn't exist in ga4
# need to figure out what date range we want here
#date_ranges=[DateRange(start_date="YYYY-MM-DD", end_date="today")],

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
#
# code that we need to figure out how to run in the virtualenv
#
#
# set up the data frame
df=pd.DataFrame(all_data)
x=df.sort_values('date').copy()
spv=x['screenPageViews']
sess=x['sessions']
nuser=x['newUsers']
totu=x['totalUsers']
x['date']=pd.to_datetime(x['date'])
# sessions does not equate to number of users....
notnewu=totu.astype(int)-nuser.astype(int) # number of returing users
pctnu=nuser.astype(int)/sess.astype(int)
avgsess_per_user=sess.astype(int)/totu.astype(int)
avg_not_newsessions=avgsess_per_user*notnewu
newsess=sess.astype(int)-avg_not_newsessions
avg_percent_newsessions=newsess/sess.astype(int)
#
out_df=x[['date','sessions','totalUsers']].copy(deep=True)
out_df['date']=pd.to_datetime(out_df['date'])
out_df['sessions']=out_df['sessions'].astype(str)
out_df['totalUsers']=out_df['totalUsers'].astype(str)
#pdb.set_trace()
x=x.reset_index()
thelastdate=olddataframe['date'].iloc[-1]
therowineed=x[(x['date']==thelastdate)].index.to_numpy()
therow=therowineed[0]
#
# New code to eliminate warning message from substituting values
olddataframe.iloc[-1,1]=x['sessions'][therow] # Note last row is -1 and column 1 is "sessions"
olddataframe.iloc[-1,2]=x['totalUsers'][therow] # Note last row is -1 and column 2 is "totalUsers"
#olddataframe['sessions'].iloc[-1]=x['sessions'][therow]
#olddataframe['totalUsers'].iloc[-1]=x['totalUsers'][therow]
#
#pdb.set_trace()
rows_to_remove=np.arange(0,therow+1)
out_df=out_df.reset_index()
#pdb.set_trace()
out_df=out_df.drop(rows_to_remove)
#
#pdb.set_trace()
olddataframe=pd.concat([olddataframe,out_df],ignore_index=True)
olddataframe.drop(columns='index')
olddataframe.to_csv('/home/flbahr/webdata/ga4_daily_analytics.csv',index=False)
# we want to create some special values that are not in the standard metrics
# sessions/newUsers is the old percentNewSessions?
# screenPageViews/sessions is the old pageviewsPerSession
#
newframe=olddataframe
newframe['date']=pd.to_datetime(newframe['date'],format='%Y-%m-%d')
test=dt.datetime.now()-dt.timedelta(days=31)
newframe=newframe[newframe.date > test]
newframe=newframe.rename(columns={'totalUsers':'users'})
newframe=newframe.drop(columns='index')
newframe=newframe[:-1]
newframe.to_csv('/home/flbahr/webdata/ga4_daily_forwebplot.csv',index=False)

