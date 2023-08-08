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
myfile='ga4_monthly_analytics_mpa.csv'
mynow=dt.datetime.now()
try:
        olddataframe=pd.read_csv(mypath+myfile)
        lastdate=olddataframe['month'].iloc[-1]
        if mynow.month < lastmonth:
                mystart=dt.datetime(mynow.year-1,lastmonth,1)
        else:
                mystart=dt.datetime(mynow.year,lastmonth,1)
        nodataframe=0
except:
        mystart=mynow-dt.timedelta(days=32)
        nodataframe=1
nodataframe=0
#pdb.set_trace()
mystart=mynow-dt.timedelta(days=32)
syear=str(mystart.year)
smon=str(mystart.month)
sday=str(mystart.day)
# CeNCOOS property ID
# Axiom Property ID
property_id=your ID goes here
# create google data api client?
# set path to credentials
credentials_json_path="your credential file goes here"
client=BetaAnalyticsDataClient().from_service_account_json(credentials_json_path)
# example code from google
#dimension=[Dimension(name="city")], # example don't want this
#metrics=[Metric(name="activeUsers")],
#date_ranges=[DateRange(start_date="YYYY-MM-DD", end_date="today")],
mydaterange=[DateRange(start_date=syear+"-"+smon+"-"+sday, end_date="today")]
mydimensions=[Dimension(name="month"),
              Dimension(name="year")]
mymetrics=[
Metric(name="totalUsers"),
Metric(name="newUsers"),
Metric(name="sessions"),
Metric(name="bounceRate"),
Metric(name="averageSessionDuration"),
Metric(name="active28DayUsers"),
Metric(name="activeUsers"),
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
import numpy as np
import pandas as pd
#
# set up the data frame
df=pd.DataFrame(all_data)
x=df.sort_values('month').copy(deep=True)
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
out_df=x[['month','year','sessions','totalUsers']].copy(deep=True)
out_df['sessions']=out_df['sessions'].astype(str)
out_df['totalUsers']=out_df['totalUsers'].astype(str)
# we want to create some special values that are not in the standard metrics
# sessions/newUsers is the old percentNewSessions?
# screenPageViews/sessions is the old pageviewsPerSession
#
x=x.reset_index()
if nodataframe==1:
        thelastmonth=[]
        thelastyear=[]
        therowineed=0
        therow=-1
try:
        thelastmonth=olddataframe['month'].iloc[-1]
        thelastyear=olddataframe['year'].iloc[-1]
        therowineed=x[(x['month'].astype(int)==thelastmonth)&(x['year'].astype(int)==thelastyear)].index.to_numpy()
        therow=therowineed[0]
        olddataframe.iloc[-1,2]=x['sessions'][therow] # sessions is column 2 and -1 is last row (columns start with 0)
        olddataframe.iloc[-1,3]=x['totalUsers'][therow] # totalUsers is column 3 and -1 is last row
        rows_to_remove=np.arange(0,therow+1)
        out_df=out_df.reset_index()
        out_df=out_df.drop(rows_to_remove)
        olddataframe=pd.concat([olddataframe,out_df],ignore_index=True)
except:
        olddataframe=out_df
        olddataframe=olddataframe.drop(columns='index')
# now merge dataframes
olddataframe=pd.concat([olddataframe,out_df],ignore_index=True)
# now print out to a csv file!
olddataframe.to_csv('/home/flbahr/webdata/ga4_monthly_analytics_mpa.csv',index=False)

daysinmonth=np.array([31,28,31,30,31,30,31,31,30,31,30,31])
# create output for web page in HighCharts format 
newdf=olddataframe.copy(deep=True)
months=newdf['month'].astype(int)-1
dpm=daysinmonth[months]
newdf['days']=dpm
newdf['month']=newdf['month'].astype(str)
newdf['month']=newdf['month'].str.zfill(2)
newdf['year']=newdf['year'].astype(str)
newdf['days']=newdf['days'].astype(str)
newdf['date']=newdf['year']+'-'+newdf['month']+'-15'

newdf=newdf.drop(columns=['month','year'])
newdf=newdf[['date','sessions','totalUsers']]
newdf=newdf.rename(columns={'totalUsers':'users'})
newdf=newdf[:-1]
newdf.to_csv('/home/flbahr/webdata/ga4_monthly_mpa_forwebplot.csv',index=False)


