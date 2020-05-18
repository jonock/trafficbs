import numpy as np
import pandas as pd


def loaddata():
    datahist = pd.read_csv('data/200510_download_hist.csv', sep=';')
    data = pd.read_csv('data/200515_download.csv', sep=';')
    datahist = datahist.loc[(datahist['Year'] > 2016) & (datahist['Year'] < 2019)]
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
    groupreturn = groupreturn.drop_duplicates()
    groupreturn = groupreturn.sort_values(by=['Year', 'Month', 'Day'])
    del(groupshist)
    del(groups)
    del(datahist)
    del(data)
    groupreturn.to_csv('data/dailytotals.csv')
    print('fertig')
    return(groupreturn)

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
    return(monthlyavg)


daytotals = loaddata()
# monthlyaverages(daytotals)
