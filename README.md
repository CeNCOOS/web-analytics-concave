## Generating Google Analytics data CeNCOOS Website ##

Data are accessed using the Analytics API V4. Getting this set up is kind of a pain.

[Here is the Guide from Google](https://developers.google.com/analytics/devguides/reporting/core/v4/quickstart/service-py)

First, you have to create a Service account on the Google Cloud services. Then you have Grant User access to that service account and generate a prive key that is stored in a JSON file.

The Second step is that you have to add that Service Account email to google analytics as a users with read priviliages at the `View` level. That email looks something like this one:

`quickstart@PROJECT-ID.iam.gserviceaccount.com`

Since CeNCOOS does not have Admin privileges on the Data Portal and Services Analytics pages, you will need to request that someone (Kyle or Shane) add it.

## Running the Script Here
`generate-analytics.py` is set on a CRON job to run at 23:00 every day, this is setup using the `schedule-cron-web-analytics.py` script, which should only be run at setup.

## Output
`generate-analytics.py` creates a csv file which is then moved over to skyrocket8 via a scp command. In order to scp, you have to setup an SSH-KEY and give the public key to Skyrocket8. Pat can help with setting this up.

## Update
The new google data api (ga4) is running currently on the CeNCOOS website.  The monthly file and daily files are now get_monthly_ga4_update.py and get_analytics_ga4_cencoos_update.py<p>
The update files have been placed into the repository.  Note that Google IDs and Google credential files have been removed and you will need to add your own IDs and credentials in the appropriate places to have the code run.
## old code
The files generate-analytics.py and generate-analytics-monthly.py are for the old Google Analytics (Universal Analytics).  These files are depreciated.
