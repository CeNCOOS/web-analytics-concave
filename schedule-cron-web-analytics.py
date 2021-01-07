from crontab import CronTab


my_cron = CronTab(user='pdaniel')

job = my_cron.new(command='/home/pdaniel/anaconda3/bin/python /home/pdaniel/web-analytics/generate-analytics.py')
job.hour.on(23)
job = my_cron.new(command='/home/pdaniel/anaconda3/bin/python /home/pdaniel/web-analytics/generate-analytics-monthly.py')
job.hour.on(23)


for job in my_cron:
    print(job,job.is_valid())