#Copyright 2023 Chris Webber

import datetime
import numpy as np
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
import psycopg
import urllib.request
import json

import sys

connectionString = ""
forecastUri = ""

todayDt = datetime.datetime.now()
if len(sys.argv) == 1:
    monthPart = todayDt.month
    dayPart = todayDt.day
    tempToday = None
elif len(sys.argv) == 2:
    monthPart = todayDt.month
    dayPart = todayDt.day
    tempToday = sys.argv[1]
elif len(sys.argv) == 3:
    monthPart = sys.argv[1]
    dayPart = sys.argv[2]
    tempToday = None
elif len(sys.argv) == 4:
    monthPart = sys.argv[1]
    dayPart = sys.argv[2]
    tempToday = sys.argv[3]
else:
    exit("Usage: dailycompare [month day] [tempToday]")


sqlQuery = '''select 
    stationid
    ,s.name
    ,s.state
    ,date
    --,tmax
    ,round(((tmax / 10.0) * (9.0/5.0)) + 32, 2) as tmax_f
    --,tmin
    ,round(((tmin / 10.0) * (9.0/5.0)) + 32, 2) as tmin_f
    ,round(((((tmax + tmin) / 2) / 10.0) * (9.0/5.0)) + 32, 2) as tavg_f
from ghcn.daily d 
left join ghcn.station s on d.stationid = s.id
where tmax is not NULL and tmin is not null
and s.state = 'CO'
and stationid not in ('USW00023066')
and date_part('month', date) = {month}
and date_part('day', date) = {day}
order by stationid, date
'''.format(month=monthPart, day=dayPart)


with psycopg.connect(connectionString) as conn:
    tempDf = pd.read_sql(sqlQuery, conn, index_col=None, parse_dates=["date"])

if tempToday is None:
    req = urllib.request.Request(url=forecastUri)
    con = urllib.request.urlopen(req)
    page = con.read()
    forecastData = json.loads(page)
    tempToday = forecastData["temperatureMax"][0]
    
tempDf["today"] = pd.Series(tempToday, index=tempDf.index, dtype='float')

fig, ax = plt.subplots()

if tempToday is not None:
    ax.plot(tempDf.loc[tempDf['stationid'] == 'USC00051401', ["date"]], 
        tempDf.loc[tempDf['stationid'] == 'USC00051401', ["today"]], 
        color="tab:red",
        label="Today")

stationIds = tempDf['stationid'].unique()
for stationId in stationIds:
    stationDf = tempDf.loc[tempDf['stationid'] == stationId]
    stationName = stationDf.iloc[0]["name"]
    ax.plot(stationDf["date"], 
        stationDf["tmax_f"], 
        label=stationName)

ax.set_xlabel('Year')
ax.set_ylabel('Daily High (F)')

ax.legend()

plt.show()
