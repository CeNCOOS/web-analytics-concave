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
#import xarray as xr
import pdb
# google analytics ga4 python modules
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    BatchRunReportsRequest, # which report request do we run?
    DateRange,
    Dimension,
    Filter,
    FilterExpression,
    Metric,
    RunReportRequest, # this is the other request?
)
# set data output paths
mypath='/home/flbahr/test/'
# set start and end dates
mynow=dt.datetime.now()
mystart=mynow-dt.timedelta(days=31)
syear=str(mystart.year)
smon=str(mystart.month)
sday=str(mystart.day)
# CeNCOOS property ID
property_id=your_property_id
# create google data api client?
# set path to credentials
credentials_json_path="path_to_your_credentials_file" # put in correct json file name
#client=BetaAnalyticsDataClient().from_service_json(credentials_json_path)
client=BetaAnalyticsDataClient()
# example code from google
#dimension=[Dimension(name="city")], # example don't want this
#metrics=[Metric(name="activeUsers")],
#date_ranges=[DateRange(start_date="YYYY-MM-DD", end_date="today")],
mydaterange=[DateRange(start_date=syear+"-"+smon+"-"+sday, end_date="today")]
# need different Dimensions with daily vs monthly output
#mydimensions=[Dimension(name="date")] # use this line for daily, the following 2 for monthly
mydimensions=[Dimension(name="month"),
              Dimension(name="year")] # this will output only a month data
# test code for a single page
#mydfilter=FilterExpression(
#    filter=Filter(
#        field_name="pagePath",
#        string_filter=Filter.StringFilter(value="/cuti-and-beuti-upwelling-indicies"),
#        )
#    )
# set up the metrics we want to get back
mymetrics=[
Metric(name="totalUsers"),
Metric(name="newUsers"),
Metric(name="sessions"),
Metric(name="bounceRate"),
Metric(name="averageSessionDuration"),
Metric(name="active28DayUsers"),
Metric(name="activeUsers"),
Metric(name="ScreenPageViewsPerSession"),
#Metric(name="year"), # since year is dimension we can't use it here
#Metric(name="entrances"), # entrances doesn't exist in ga4
Metric(name="screenPageViews")]
#
# List the pages we want to get analytics for
#
thepages=[
    "/cuti-and-beuti-upwelling-indicies/",
    "/data-by-location/humboldt-bay/",
    "/data-by-location/bodega-bay/",
    "/data-by-location/san-francisco-bay/",
    "/data-by-location/monterey-bay/",
    "/data-by-location/morro-bay/",
    "/morro-bay-oyster-dashboard/",
    "/oyster-dashboard/",
    "/focus-areas/oah/",
    "/focus-areas/weather-climate/",
    "/focus-areas/habs/",
    "/focus-areas/biology-ecosystems/",
    "/focus-areas/tech-innovation/",
    "/data-services/",
    "/observations/sensor-platforms/hf-radar/",
    "/observations/what-we-observe/",
    "/observations/sensor-platforms/shore-stations/",
    "/observations/sensor-platforms/gliders/",
    "/line66-7-glider/",
    "/line-56-7-pt-arena/",
    "/trinidad-glider/",
    "/spray-29-naval-postgraduate-school-testbed-glider/",
    "/spray-34-mbari-nps-glider/",
    "/observations/sensor-platforms/buoys/",
    "/observations/sensor-platforms/buoys/m1-buoy-data-plots-last-7-days/",
    "/observations/sensor-platforms/buoys/ocean-acidification-buoy-1/",
    "/observations/sensor-platforms/buoys/ocean-acidification-buoy-2/",
    "/information-solutions/san-francisco-baycurrents-app/",
    "/observations/models-forecasts/",
    "/pt-reyes-wave-climatology/",
    "/marine-heat-wave/",
    "/information-solutions/",
    "/information-solutions/recent-waves/",
    "/imaging-flow-cytobots/",
    "/observations/satellites/recent-sst-and-chlorophyll-a/"]

# filenames for the output (sans extension)
thefilesout=[
    "cutibeuti",
    "humboldt",
    "bodega",
    "sanfrancisco",
    "monterey",
    "morro",
    "morro_oyster",
    "humboldt_oyster",
    "oah",
    "climate",
    "habs",
    "ecosystems",
    "tech",
    "dataservices",
    "hfradar",
    "whatweobserve",
    "shorestations",
    "gliders",
    "line67",
    "line57",
    "trinidad",
    "spray29",
    "spray34",
    "buoys",
    "m1_sevendays",
    "oa1",
    "oa2",
    "baycurrents",
    "models",
    "waveclimate",
    "heatwave",
    "solutions",
    "recentwaves",
    "ifcb",
    "satellite"]


#
# set up loop index for the pages above
aloop=np.arange(0,len(thepages))
# Now loop through the pages and output the results
for oo in aloop:
    #
    # code to read the old data
    #
    try:
       olddataframe=pd.read_csv(mypath+thefilesout[oo]+'.csv')
       lastmonth=olddataframe['month'].iloc[-1]
       if mynow.month < lastmonth:
           mystart=dt.datetime(mynow.year-1,lastmonth,1)
       else:
           mystart=dt.datetime(mynow.year,lastmonth,1)
       nodataframe=0
    except:
       mystart=mynow-dt.timedelta(days=31)
       nodataframe=1
    #
    # set up the request and the properties 
    requests=RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=mydimensions,
        metrics=mymetrics,
        date_ranges=mydaterange,
        dimension_filter=FilterExpression(
            filter=Filter(
                field_name="pagePath",
                string_filter=Filter.StringFilter(value=thepages[oo]),
                )
            ),
        )
    # request the response
    response=client.run_report(requests)
    # now need to put code in to deal with the response for each page accessed.
    dim_len=len(response.dimension_headers)
    metric_len=len(response.metric_headers)
    all_data=[]
    # set up sizes and set up empty array to put data into
    for row in response.rows:
            row_data={}
            for i in range(0,dim_len):
                    row_data.update({response.dimension_headers[i].name: row.dimension_values[i].value})
            for i in range(0, metric_len):
                    row_data.update({response.metric_headers[i].name: row.metric_values[i].value})
            all_data.append(row_data)

    newdataframe=pd.DataFrame(all_data)
    # sort data based upon month?
    if len(newdataframe) > 0:
        x=newdataframe.sort_values(by=['year','month']).copy(deep=True)
        x=x.reset_index()
        x=x.drop(columns='index')
        spv=x['screenPageViews']
        sess=x['sessions']
        nuser=x['newUsers']
        totu=x['totalUsers']
    #
        out_df=x[['month','year','sessions','totalUsers']].copy(deep=True)
        out_df['sessions']=out_df['sessions'].astype(str)
        out_df['totalUsers']=out_df['totalUsers'].astype(str)
     
    #
        if nodataframe==0:
            thelastmonth=olddataframe['month'].iloc[-1]
            thelastyear=olddataframe['year'].iloc[-1]
            therowineed=x[(x['month'].astype(int)==thelastmonth)&(x['year'].astype(int)==thelastyear)].index.to_numpy()
            try:
                therow=therowineed[0]
    #
                olddataframe.iloc[-1,2]=x['sessions'][therow]
                olddataframe.iloc[-1,3]=x['totalUsers'][therow]
    #
    # row to drop
                rows_to_remove=np.arange(0,therow+1)
                out_df=out_df.drop(rows_to_remove)
    # merge dataframes
                olddataframe=pd.concat([olddataframe,out_df],ignore_index=True)
            except:
                pass
        else:
            olddataframe=out_df
    #
    # output concanenated data fram
        olddataframe.to_csv('/home/flbahr/test/'+thefilesout[oo]+'.csv',index=False)
    
