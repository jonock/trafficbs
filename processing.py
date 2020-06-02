import os.path
import sys
from datetime import date

import numpy as np
import pandas as pd

import datakicker as dk


def normalavgWD(data):
    for site in data['SiteCode'].unique():
        thisdata = data.loc[data['SiteCode'] == site].copy()
        thisdata['Month'] = thisdata['Month'].astype(int)
        thisdata['Day'] = thisdata['Day'].astype(int)
        thisdata['Month'] = thisdata['Month'].apply(lambda x: '{0:0>2}'.format(x))
        thisdata['Day'] = thisdata['Day'].astype(int).apply(lambda x: '{0:0>2}'.format(x))
        thisdata = thisdata.sort_values(by=['Year', 'Month', 'Day']).reset_index(drop=True)
        thisdata = thisdata.loc[thisdata['Weekday'] < 5]
        thisdata['rollingavg'] = thisdata['Total']
        thisdata['date'] = thisdata['Year'].astype(str) + '-' + thisdata['Month'].astype(str) + '-' + thisdata[
            'Day'].astype(str)
        thisdata['shortdate'] = thisdata['Month'].astype(str) + '-' + thisdata['Day'].astype(str)
        newdata = pd.DataFrame(columns=['counter', 'date', 'Month', 'Day', '2018', '2019', '2020'])
        da2018 = pd.DataFrame(columns=['Month', 'Day', '2018'])
        da2019 = pd.DataFrame(columns=['Month', 'Day', '2019'])
        da2020 = pd.DataFrame(columns=['Month', 'Day', '2020'])
        da2018['Month'] = thisdata.loc[thisdata['Year'] == 2018]['Month']
        da2018['Day'] = thisdata.loc[thisdata['Year'] == 2018]['Day']
        da2018['2018'] = thisdata.loc[thisdata['Year'] == 2018]['rollingavg']
        da2018['counter'] = range(len(da2018))
        da2019['Month'] = thisdata.loc[thisdata['Year'] == 2019]['Month']
        da2019['Day'] = thisdata.loc[thisdata['Year'] == 2019]['Day']
        da2019['2019'] = thisdata.loc[thisdata['Year'] == 2019]['rollingavg']
        da2019['counter'] = range(len(da2019))
        da2020['Month'] = thisdata.loc[thisdata['Year'] == 2020]['Month']
        da2020['Day'] = thisdata.loc[thisdata['Year'] == 2020]['Day']
        da2020['2020'] = thisdata.loc[thisdata['Year'] == 2020]['rollingavg']
        da2020['date'] = thisdata.loc[thisdata['Year'] == 2020]['shortdate']
        da2020['counter'] = range(len(da2020))
        newdata = pd.merge(da2018[['counter', '2018']], da2019[['2019', 'counter']], how='outer',
                           on=['counter']).reset_index(drop=True)
        newdata = pd.merge(newdata, da2020[['2020', 'counter', 'date']], how='outer', on=['counter']).reset_index(
            drop=True)
        # newdata = newdata.sort_values(by=['Month', 'Day'])
        # da2020['date'] = da2020[['Month', 'Day']].agg('-'.join, axis=1)
        # newdata = newdata.drop(columns = ['Month', 'Day'])
        filestring = 'rollingavgWD_' + thisdata['TrafficType'].iloc[0] + '_' + thisdata['SiteName'].iloc[0].replace(' ',
                                                                                                                    '').replace(
            '/', '')
        if thisdata['TrafficType'].iloc[0] == 'Velo':
            namestring = 'Anzahl ' + thisdata['TrafficType'].iloc[0] + 's bei ' + thisdata['SiteName'].iloc[0][4:]
        else:
            namestring = 'Anzahl ' + thisdata['TrafficType'].iloc[0] + ' bei ' + thisdata['SiteName'].iloc[0][4:]
        folder = 32224
        filename = 'metaconfigs/normalcorona.json'
        chartadminfn = 'normalavg_chartadmin.csv'
        updatedate = str(max(thisdata['date']))
        uploadaverages(site, thisdata, newdata, filestring, namestring, folder, chartadminfn, updatedate, filename)
        coronatime = newdata.loc[(newdata['counter'] > 40) & (newdata['counter'] < sum(~np.isnan(newdata['2020'])))]
        folder = 32225
        chartadminfn = 'normalavg_3m_chartadmin.csv'
        coronatime = coronatime.drop(columns=['counter'])
        filename = 'metaconfigs/normalcorona.json'
        uploadaverages(site, thisdata, coronatime, filestring, namestring, folder, chartadminfn, updatedate, filename)


def rollingavgWD(data, chartadminfn='rollingavg_3m_chartadmin.csv', folder=31911):
    for site in data['SiteCode'].unique():
        thisdata = data.loc[data['SiteCode'] == site].copy()
        thisdata['Month'] = thisdata['Month'].astype(int)
        thisdata['Day'] = thisdata['Day'].astype(int)
        thisdata['Month'] = thisdata['Month'].apply(lambda x: '{0:0>2}'.format(x))
        thisdata['Day'] = thisdata['Day'].astype(int).apply(lambda x: '{0:0>2}'.format(x))
        thisdata = thisdata.sort_values(by=['Year', 'Month', 'Day']).reset_index(drop=True)
        thisdata = thisdata.loc[thisdata['Weekday'] < 5]
        thisdata['rollingavg'] = thisdata['Total'].rolling(3).mean()
        thisdata['date'] = thisdata['Year'].astype(str) + '-' + thisdata['Month'].astype(str) + '-' + thisdata[
            'Day'].astype(str)
        thisdata['shortdate'] = thisdata['Month'].astype(str) + '-' + thisdata['Day'].astype(str)
        newdata = pd.DataFrame(columns=['counter','date','Month', 'Day','2018', '2019', '2020'])
        da2018 = pd.DataFrame(columns=['Month', 'Day', '2018'])
        da2019 = pd.DataFrame(columns=['Month', 'Day', '2019'])
        da2020 = pd.DataFrame(columns=['Month', 'Day', '2020'])
        da2018['Month'] = thisdata.loc[thisdata['Year'] == 2018]['Month']
        da2018['Day'] = thisdata.loc[thisdata['Year'] == 2018]['Day']
        da2018['2018'] = thisdata.loc[thisdata['Year'] == 2018]['rollingavg']
        da2018['counter'] = range(len(da2018))
        da2019['Month'] = thisdata.loc[thisdata['Year'] == 2019]['Month']
        da2019['Day'] = thisdata.loc[thisdata['Year'] == 2019]['Day']
        da2019['2019'] = thisdata.loc[thisdata['Year'] == 2019]['rollingavg']
        da2019['counter'] = range(len(da2019))
        da2020['Month'] = thisdata.loc[thisdata['Year'] == 2020]['Month']
        da2020['Day'] = thisdata.loc[thisdata['Year'] == 2020]['Day']
        da2020['2020'] = thisdata.loc[thisdata['Year'] == 2020]['rollingavg']
        da2020['date'] = thisdata.loc[thisdata['Year'] == 2020]['shortdate']
        da2020['counter'] = range(len(da2020))
        newdata = pd.merge(da2018[['counter', '2018']], da2019[['2019', 'counter']], how='outer',
                           on=['counter']).reset_index(drop=True)
        newdata = pd.merge(newdata, da2020[['2020', 'counter', 'date']], how='outer', on=['counter']).reset_index(
            drop=True)
        # newdata = newdata.sort_values(by=['Month', 'Day'])
        # da2020['date'] = da2020[['Month', 'Day']].agg('-'.join, axis=1)
        # newdata = newdata.drop(columns = ['Month', 'Day'])
        filestring = 'rollingavgWD_' + thisdata['TrafficType'].iloc[0] + '_' + thisdata['SiteName'].iloc[0].replace(' ',
                                                                                                                    '').replace(
            '/', '')

        if thisdata['TrafficType'].iloc[0] == 'Velo':
            namestring = 'Anzahl ' + thisdata['TrafficType'].iloc[0] + 's bei ' + thisdata['SiteName'].iloc[0][4:]
        elif thisdata['TrafficType'].iloc[0] == 'MIV':
            namestring = 'MIV Verkehr bei ' + thisdata['SiteName'].iloc[0][4:]
        else:
            namestring = 'Anzahl ' + thisdata['TrafficType'].iloc[0] + ' bei ' + thisdata['SiteName'].iloc[0][4:]

        coordinates = thisdata.loc[thisdata['Geo Point'].notna()]['Geo Point'].unique().astype(str)
        stationstring = thisdata['SiteName'].iloc[0][4:]
        updatedate = str(max(thisdata['date']))
        coronatime = newdata.loc[(newdata['counter'] > 40) & (newdata['counter'] < sum(~np.isnan(newdata['2020'])))]
        coronatime = coronatime.drop(columns=['counter'])
        filename = 'metaconfigs/rollingcorona.json'
        uploadaverages(site, thisdata, coronatime, filestring, namestring, folder, chartadminfn, updatedate, filename,
                       stationstring, coordinates)


def calendarweeks(bpdata, mivdata, ptdata):
    # add calendar weeks to entries
    bpdata['iso_week_number'] = bpdata.apply(
        lambda x: date(int(x['Year']), int(x['Month']), int(x['Day'])).isocalendar()[1], axis=1)

    mivdata['iso_week_number'] = mivdata.apply(
        lambda x: date(int(x['Year']), int(x['Month']), int(x['Day'])).isocalendar()[1], axis=1)

    # group by traffic type and calendar week
    weekly_bp = pd.DataFrame(columns=['TrafficType', 'iso_week_number', 'Year', 'Month', 'Day', 'Total'])
    weekly_bp = bpdata.groupby(['TrafficType', 'Year', 'iso_week_number']).agg(
        {
            'Total': 'sum',
            'SiteCode': 'count',
            'Month': 'first',
            'Day': 'first'
        }
    )
    # rename number of observations
    weekly_bp = weekly_bp.rename(columns={'SiteCode': 'n_observations'})
    # same for miv data
    weekly_miv = pd.DataFrame(columns=['TrafficType', 'iso_week_number', 'Year', 'Month', 'Day', 'Total'])
    weekly_miv = mivdata.groupby(['TrafficType', 'Year', 'iso_week_number']).agg(
        {
            'Total': 'sum',
            'SiteCode': 'count',
            'Month': 'first',
            'Day': 'first'
        }
    )
    # rename number of observations
    weekly_miv = weekly_miv.rename(columns={'SiteCode': 'n_observations'})
    # Adjust Public Transport Data
    ptdata = ptdata.sort_values(by='Kalenderwoche')

    ptdata['Year'] = ptdata.apply(
        lambda x: int(str(x['Startdatum Woche'])[:4]), axis=1)

    ptdata['Month'] = ptdata.apply(
        lambda x: int(str(x['Startdatum Woche'])[5:7]), axis=1)

    ptdata['Day'] = ptdata.apply(
        lambda x: int(str(x['Startdatum Woche'])[8:10]), axis=1)
    # check for incomplete weeks before merging

    incomplete = []
    # incomplete = incomplete.append(weekly_bp.loc[weekly_bp['n_observations']<100])
    weekly_bp = weekly_bp.loc[weekly_bp['n_observations'] >= 100]
    weekly_miv = weekly_miv.loc[weekly_miv['n_observations'] >= 180]

    # relabel tables for DataWrapper upload
    ptdata = ptdata.drop(columns='Startdatum Woche')
    ptdata = ptdata.rename(columns={'Kalenderwoche': 'iso_week_number', 'Fahrgäste (Einsteiger)': 'Total'})
    ptdata['TrafficType'] = 'BVB'
    ptdata = ptdata.set_index(['TrafficType', 'Year', 'iso_week_number'])

    weekly_developments = pd.concat([weekly_bp, weekly_miv, ptdata], ignore_index=False)
    total_weekly_traffic = weekly_developments.groupby(['Year', 'iso_week_number']).agg(
        {
            'Total': 'sum',
            'Month': 'first',
            'Day': 'first'
        }
    )
    weekly_developments.to_csv('data/weekly_totals_traffictype.csv')
    total_weekly_traffic.to_csv('data/weekly_total_traffic.csv')
    # remove number of observations
    weekly_developments = weekly_developments.drop(columns='n_observations')

    weekly_miv = weekly_miv.reset_index()
    weekly_bp = weekly_bp.reset_index()
    ptdata = mivdata.reset_index()
    weekly_developments = weekly_developments.drop(columns=['Day', 'Month'])
    weekly_upload = weekly_developments.unstack(level=0)
    weekly_upload = weekly_upload.reset_index()
    weekly_upload = weekly_upload.iloc[:, :6]
    weekly_upload = weekly_upload.loc[weekly_upload['Year'] == 2020]
    weekly_upload = weekly_upload.loc[weekly_upload['iso_week_number'] > 6]
    weekly_upload.columns = weekly_upload.columns.to_flat_index()
    weekly_upload.columns = ['Year', 'Wochennummer', 'BVB', 'Fussgänger', 'MIV', 'Velo']
    weekly_upload = weekly_upload.drop(columns=['Year'])
    weekly_upload = weekly_upload[['Wochennummer', 'MIV', 'BVB', 'Velo', 'Fussgänger']]
    weekly_upload.to_csv('data/weekly_totals_traffictype_upload.csv', index=False)
    print('Tabelle für Upload gespeichert')
    dk.updatedwchart(id='Uy7qm', data=weekly_upload,
                     updatedate='Kalenderwoche ' + str(max(weekly_upload['Wochennummer'])), folder=31844,
                     title='Verkehrsmessungen Basel-Stadt')
    print('Tabelle hochgeladen')

def uploadaverages(site, thisdata, newdata, filestring, namestring, folder, chartadminfn, updatedate, filename,
                   stationstring, coordinates=0):
    if os.path.exists(f'{chartadminfn}'):
        charts = pd.read_csv(f'{chartadminfn}', index_col=False)
        if site in charts['SiteCode'].unique():
            thischart = charts.loc[charts['SiteCode'] == site].copy()
            id = thischart['dwid'].iloc[0]
            jsondata = dk.updatedwchart(id, newdata, title=namestring, updatedate=updatedate, folder=folder)
            thischart['title'] = namestring
            thischart['stationstring'] = stationstring
            thischart['embedcode'] = jsondata['metadata']['publish']['embed-codes']['embed-method-responsive']
            thischart['url'] = 'https://datawrapper.dwcdn.net' + jsondata['url'][10:]
            if updatedate[:7] != '2020-05':
                thischart['ignore'] = 1
            else:
                thischart['ignore'] = 0
                thischart['coordinates'] = coordinates.astype(str)
            dk.updatemetadata(id, filename)
            charts.loc[charts['SiteCode'] == site] = thischart
            print(thischart['title'].iloc[0] + ' update erfolgreich')
            charts.to_csv(f'{chartadminfn}', index=False)
        else:
            title = namestring
            id = dk.createDWChart(title)
            app = {'SiteCode': site, 'dwid': id, 'title': title, 'coordinates': coordinates,
                   'stationstring': stationstring}
            updatedate = str(max(thisdata['Date']))
            dk.updatemetadata(id, filename)
            jsondata = dk.updatedwchart(id, newdata, title=namestring, updatedate=updatedate, folder=folder)
            app['embedcode'] = jsondata['metadata']['publish']['embed-codes']['embed-method-responsive']
            app['url'] = 'https://datawrapper.dwcdn.net' + jsondata['url']
            if updatedate[:7] != '2020-05':
                app['ignore'] = 1
            charts = charts.append(app, ignore_index=True)
            print(title + f' neu erstellt {id} und in chartadmin eingetragen')
            charts.to_csv(f'{chartadminfn}', index=False)
        newdata.to_csv(f'data/stations/corona_{filestring}.csv')
    else:
        sys.exit('No chartadmin file found - check if rolling_chartadmin.csv exists')


def monthlyaverages(data):
    for site in data['SiteCode'].unique():
        thisdata = data.loc[data['SiteCode'] == site].copy()
        thisdata.sort_values(by=['Year', 'Month'])
        filestring = thisdata['TrafficType'].iloc[0] + '_' + thisdata['SiteName'].iloc[0].replace(' ', '').replace('/','')
        namestring = thisdata['TrafficType'].iloc[0] + ' - ' + thisdata['SiteName'].iloc[0][4:]
        filename = 'metaconfigs/dailies.json'
        if os.path.exists('monthly_chartadmin.csv'):
            charts = pd.read_csv('monthly_chartadmin.csv', index_col= False)
            if site in charts['SiteCode'].unique():
                thischart = charts.loc[charts['SiteCode']==site].copy()
                id = thischart['dwid'].iloc[0]
                updatedate = str(max(thisdata['Date']))
                jsondata = dk.updatedwchart(id,thisdata,title = namestring,updatedate=updatedate)
                dk.updatemetadata(id, filename)
                print(thischart['title'].iloc[0] + ' update erfolgreich')
            else:
                title = namestring
                id = dk.createDWChart(title)
                app = {'SiteCode': site, 'dwid': id, 'title': title}
                charts = charts.append(app, ignore_index = True)
                print(title + f' neu erstellt {id} und in chartadmin eingetragen')
                updatedate = str(max(thisdata['Date']))
                dk.updatemetadata(id, filename)
                jsondata = dk.updatedwchart(id,thisdata,updatedate)
                print(title + ' update erfolgreich')
                charts.to_csv('monthly_chartadmin.csv', index=False)
        else:
            sys.exit('No chartadmin file found - check if monthly_chartadmin.csv exists')

        thisdata.to_csv(f'data/stations/{filestring}.csv')

def test_monthly():
    data = pd.read_csv('data/monthlyavg.csv')
    monthlyaverages(data)


def test_rolling_avg():
    data = pd.read_csv('data/dailiesnew.csv')
    data2 = pd.read_csv('data/dailies_MIV.csv')
    rollingavgWD(data)
    rollingavgWD(data=data2, chartadminfn='MIV_rollingavg_3m_chartadmin.csv', folder=33162)
    print('Abgeschlossen')


def test_daily_avg():
    data = pd.read_csv('data/dailiesnew.csv')
    normalavgWD(data)
    print('Abgeschlossen')


def test_weekly_comparisons():
    bpdata = pd.read_csv('data/dailiesnew.csv')
    mivdata = pd.read_csv('data/dailies_MIV.csv')
    ptdata = pd.read_csv('data/pt_data.csv', delimiter=';')
    calendarweeks(bpdata, mivdata, ptdata)


test_rolling_avg()
test_weekly_comparisons()
# test_rolling_avg()
# test_monthly()
#BVB datensatz https://data.bs.ch/explore/dataset/100075/download/?format=csv&timezone=Europe/Berlin&lang=de&use_labels_for_header=true&csv_separator=%3B
