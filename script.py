import csv
import json
from datetime import datetime

import requests

# from ftpupload import ftpfileupload

now = datetime.now()


def gatherBS(id=100013):
    response = requests.get(
        f'https://data.bs.ch/api/records/1.0/search/?dataset={id}&q=&rows=10000&sort=datetimefrom'
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

data = gatherBS(100013)
writeCSVinit(data, 'rawdata_now.csv')
#recent = writeCSVcont(data, 'trafficdata.csv')
#addData(data,'evchargers.csv', recent)
#print('Neue Tabelle geschrieben: ' + recent)
