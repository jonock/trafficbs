import pandas as pd
from datetime import datetime

def loaddata():
    datahist = pd.read_csv('data/200510_download_hist.csv', sep=';')
    data = pd.read_csv('data/200510_download.csv', sep=';')
    groups = data.groupby(['SiteCode', 'Date']).agg(
        {
            'Total':sum,
            'SiteName':'first',
            'Date':'first',
            'ValuesApproved':'last',
            'TrafficType':'first',
            'Year':'first',
            'Month':'first',
            'Day':'first',
            'Weekday':'first'
        }
    )

    groupshist = datahist.groupby(['SiteCode', 'Date']).agg(
        {
            'Total': sum,
            'SiteName': 'first',
            'Date': 'first',
            'ValuesApproved': 'last',
            'TrafficType': 'first',
            'Year': 'first',
            'Month': 'first',
            'Day': 'first',
            'Weekday': 'first'
        }

    )
    groupreturn = pd.concat([groupshist, groups])
    groupreturn = groupreturn.drop_duplicates()
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
            'Total':'mean',
            'SiteName': 'first',
            'Date': 'first',
            'ValuesApproved': 'last',
            'TrafficType': 'first',
            'Year': 'first',
            'Month': 'first',
            'Day': 'first',
            'Weekday': 'first'
        }
    )
    print('monthly done')
    monthlyavg.to_csv('data/monthlyavg.csv')
    return(monthlyavg)


daytotals = loaddata()
monthlyaverages(daytotals)

