import numpy as np
import pandas as pd
import requests


def loaddata(histdata=False, histfilename='data/200510_download_hist.csv', filename='data/200515_download.csv',
             savename='data/dailytotals.csv'):
    if histdata:
        datahist = pd.read_csv(f'{histfilename}', sep=';')
        datahist = datahist.loc[(datahist['Year'] > 2016) & (datahist['Year'] < 2019)]
    data = pd.read_csv(f'{filename}', sep=';')
    groups = data.groupby(['SiteCode', 'Date']).agg(
        {
            'Total': sum,
            'SiteName': 'first',
            'ValuesApproved': 'last',
            'TrafficType': 'first',
            'Year': 'first',
            'Month': 'first',
            'Day': 'first',
            'Weekday': 'first',
            'Geo Point': 'first'
        }
    )
    print('nix')
    if histdata:
        groupshist = datahist.groupby(['SiteCode', 'Date']).agg(
            {
                'Total': sum,
                'SiteName': 'first',
                'ValuesApproved': 'last',
                'TrafficType': 'first',
                'Year': 'first',
                'Month': 'first',
                'Day': 'first',
                'Weekday': 'first'
            }
        )
        groupshist['Geo Point'] = np.nan
        groupreturn = pd.concat([groupshist, groups])
    else:
        groupreturn = groups
    groupreturn = groupreturn.drop_duplicates()
    groupreturn = groupreturn.sort_values(by=['Year', 'Month', 'Day'])
    if histdata:
        del (groupshist)
        del (datahist)
    del (data)
    del (groups)
    groupreturn.to_csv(f'{savename}')
    print('fertig')
    return (groupreturn)

def monthlyaverages(data):
    monthlyavg = data.groupby(['SiteCode', 'Year','Month']).agg(
        {
            'Total': 'mean',
            'SiteName': 'first',
            'ValuesApproved': 'last',
            'TrafficType': 'first',
            'Year': 'first',
            'Month': 'first',
            'Day': 'first',
            'Weekday': 'first',
            'Geo Point': 'first'
        }
    )
    print('monthly done')
    monthlyavg.to_csv('data/monthlyavg.csv')
    return (monthlyavg)


def csvpoll(bsid, filename):
    url = f'https://data.bs.ch/explore/dataset/{bsid}/download/?format=csv&timezone=Europe/Berlin&lang=de&use_labels_for_header=true&csv_separator=%3B'
    r = requests.get(url, allow_redirects=True)
    open(filename, 'wb').write(r.content)
    print('Datei ' + filename + ' gespeichert')


csvpoll(bsid=100006, filename='data/MIV_newpoll.csv')
csvpoll(bsid=100013, filename='data/bp_newpoll.csv')

mivtotals = loaddata(filename='data/MIV_newpoll.csv', histfilename='data/200531_MIVhist.csv',
                     savename='data/dailies_MIV.csv', histdata=True)
bptotals = loaddata(filename='data/bp_newpoll.csv', histfilename='data/200510_download_hist.csv',
                    savename='bp_dailies.csv', histdata=True)
# monthlyaverages(daytotals)
