import numpy as np
import pandas as pd
import requests
from tqdm import tqdm

def loaddata(histdata=False, histfilename='../data/200510_download_hist.csv', filename='../data/200515_download.csv',
             savename='../data/dailytotals.csv'):
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
    print(savename + ' abgelegt')
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
    print('Start Download des Datensatzes ' + str(bsid))
    r = requests.get(url, allow_redirects=True, stream=True)
    # Progress Bar mit tqdm
    total_size_in_bytes = int(r.headers.get('content-length', 0))
    block_size = 1024  # 1 Kibibyte
    progress_bar = tqdm(total=total_size_in_bytes, unit='iB', unit_scale=True)
    with open(filename, 'wb') as file:
        for data in r.iter_content(block_size):
            progress_bar.update(len(data))
            file.write(data)
    progress_bar.close()
    if total_size_in_bytes != 0 and progress_bar.n != total_size_in_bytes:
        print("ERROR, something went wrong")

    print('Download fertig, File schreiben')
    # open(filename, 'wb').write(r.content)
    print('Datei ' + filename + ' gespeichert')

# monthlyaverages(daytotals)
