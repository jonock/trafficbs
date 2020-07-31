import csv
import json
import pandas as pd
import requests
from datetime import datetime

import processing as pc
import test_weekly_data

now = datetime.now()


def dailyupdate(legacyfile='data/dailiesnew.csv', dataset_id=100013):
    # import existing dataset
    legacy = pd.read_csv(legacyfile)
    legacy = legacy.loc[legacy['Year'] > 2017]
    legacydate = []
    for date in legacy.iterrows():
        legacydate.append(datetime.strptime(date[1]['Date'], '%d.%m.%Y'))
    maxtime = max(legacydate)
    datecode = datetime.strftime(maxtime, '%Y-%m-%dT%h%m')
    print(datecode)
    # 2020-05-07T23%3A00%3A00%2B00.00
    data = gatherBS(dataset_id)
    dataagg = pd.DataFrame()
    for entry in data:
        dataagg = dataagg.append(entry['fields'], ignore_index=True)
    dataagg.sort_values(by=['year', 'month', 'day'])
    dataagg.to_csv('data/poll_' + datetime.strftime(datetime.now(), '%y%m%d.csv'))
    dataagg = dataagg.loc[dataagg['datetimefrom'] >= datecode]
    datasums = sumdata(dataagg)
    aggregate = pd.concat([legacy, datasums], ignore_index=True)
    aggregate.drop_duplicates(subset=['SiteCode', 'Date', 'Total', 'TrafficType'])
    aggregate.to_csv(legacyfile)
    print('csv gespeichert ab ' + str(datecode[:10]) + ' neue Einträge ' + str(len(datasums)))
    return dataagg

def sumdata(data):
    groups = pd.DataFrame()
    groups = data.groupby(['sitecode', 'date']).agg(
        {
            'total': sum,
            'sitename': 'first',
            'valuesapproved': 'last',
            'traffictype': 'first',
            'year': 'first',
            'month': 'first',
            'day': 'first',
            'weekday': 'first',
            'geo_point_2d': 'last'
        })
    groups = groups.reset_index(drop=False)
    groups['Geo Point'] = ' '
    groups['Geo Point'] = [','.join(map(str, l)) for l in groups['geo_point_2d']]
    # groups['Geo Point'].join(groups['geo_point_2d'])
    groups.drop('geo_point_2d', axis=1)
    groups.rename(columns={'total': 'Total', 'sitecode': 'SiteCode', 'sitename': 'SiteName', 'date': 'Date',
                           'valuesapproved': 'ValuesApproved', 'traffictype': 'TrafficType', 'year': 'Year',
                           'month': 'Month', 'day': 'Day', 'weekday': 'Weekday'}, inplace=True)
    groups = groups[
        ['SiteCode', 'Date', 'Total', 'SiteName', 'ValuesApproved', 'TrafficType', 'Year', 'Month', 'Day', 'Weekday',
         'Geo Point']]
    return groups


def gatherBS(id):
    response = requests.get(
        f'https://data.bs.ch/api/records/1.0/search/?dataset={id}&q=&rows=5000&sort=datetimefrom'
    )
    resp = json.loads(response.text)
    print('Daten geholt')
    resp1 = resp['records']
    return resp1

def addData(data,filename,recent):
    with open(f'data/{filename}', 'r') as fileread:
       existingLines = [line for line in csv.reader(fileread)]
    with open (f'data/{recent}', 'r') as recentdata:
        reader2 = csv.reader(recentdata)
        for row in reader2:
            if row not in existingLines:
                print('NEWentry')
                with open(f'data/{filename}', 'a') as dbfile:
                    appender = csv.writer(dbfile)
                    appender.writerow(row)
                    dbfile.close()

    fileread.close()
    recentdata.close()




def writeCSVinit(data, filename):
    file = open(f'data/{filename}', 'w')
    csvwriter = csv.writer(file)
    count = 0
    for i in data:
        if count == 0:
            header = i['fields'].keys()
            csvwriter.writerow(header)
            count+=1
        csvwriter.writerow(i['fields'].values())
    file.close()
    print('CSV ' + filename + ' geschrieben')


def addTimestamp(filename):
    add = now.strftime("%y%m%d_%H%M%S")
    filename = filename + '_' + add
    return filename

def writeCSVcont(data, filename):
    filename = addTimestamp(filename) + '.csv'
    file = open(f'data/{filename}', 'w')
    csvwriter = csv.writer(file)
    count = 0
    for i in data:
        if count == 0:
            header = data[1].keys()
            csvwriter.writerow(header)
            count+=1
        csvwriter.writerow(i.values())
    file.close()
    return filename


# data = gatherBS(100013)
# writeCSVinit(data, 'rawdata_now.csv')
# recent = writeCSVcont(data, 'trafficdata.csv')
# addData(data,'evchargers.csv', recent)
# print('Neue Tabelle geschrieben: ' + recent)
# dailyupdate()
# dailyupdate('data/dailies_MIV.csv', dataset_id=100006)
pc.test_rolling_avg()
test_weekly_data.test_weekly_comparisons()
