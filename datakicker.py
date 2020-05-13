import pandas as pd
import requests
import json
from credentials import dwToken


# DataWrapper API Connection
def dataWrapperConnect():
    print(dwToken + ' ist der Token. Erfolgreich geladen')
    headers = {
        'Authorization': f'Bearer {dwToken}',
    }

    response = requests.get('https://api.datawrapper.de/account', headers=headers)
    print(response.text)
    return response


def createDWChart(title="Test"):
    headers = {
        'Authorization': f'Bearer {dwToken}',
    }

    data = {
        "title": title,
        "type": "d3-lines"
    }

    response = requests.post('https://api.datawrapper.de/v3/charts', headers=headers, data=data)
    resp = response.json()
    print('New Chart created with id :' + resp['id'])
    id = resp['id']
    return id

def updatemetadata(id, filename):
    url = f'https://api.datawrapper.de/v3/charts/{id}'
    headers = {
        'authorization': f'Bearer {dwToken}',
        'accept': "*/*",
        'content-type': "application/json"
    }
    with open(f'{filename}', 'r') as json_file:
        payload = json.load(json_file)
    #payload = json.loads(payload)
    description = (requests.patch(url=url, headers=headers, json=payload))
    url = f'https://api.datawrapper.de/charts/{id}/publish'
    payload = ({'json': True})
    publish = (requests.post(url=url, headers=headers, json=payload))
    print(publish.json())


def updatedwchart(id, data, title, updatedate='-upsi, schick Jonathan eine Mail-', folder=31844):
    data.to_csv('dataupload.csv', encoding='utf8')
    data = data.to_csv(encoding='utf8')
    url = f'https://api.datawrapper.de/v3/charts/{id}/data'
    headers = {
        'authorization': f'Bearer {dwToken}',
        'content-type': 'text/csv'
    }
    dataupdate = ((requests.put(url=url, headers=headers, data=data)))

    # Beschreibung Updaten
    url = f'https://api.datawrapper.de/v3/charts/{id}'
    headers = {
        'authorization': f'Bearer {dwToken}'
    }
    message = 'Letztes Update der Daten: ' + updatedate
    payload = {
        'title': f'{title}',
        'metadata': {
            'annotate': {
                'notes': f'{message}'
            }
        },
        'visualize': {
            'custom-range-y': [0,''],
        },
        'folderId': f'{folder}'
    }
    #    payload = json.dumps(payload)
    description = ((requests.patch(url=url, headers=headers, json=payload)))
    url = f'https://api.datawrapper.de/charts/{id}/publish'
    payload = ({'json': True})
    publish = (requests.post(url=url, headers=headers, json=payload))
    print(publish.json())
    return description.json()

def addDWData(id, dataimp):
    headers = {
        'authorization': f'Bearer {dwToken}',
        'content-type': 'text/csv'
    }
    print(dataimp)
    data = dataimp.to_csv(f'data/dwcharts/{id}_data.csv', index=True, encoding='utf-8')
    print(repr(data))
    url = f'https://api.datawrapper.de/v3/charts/{id}/data'

    #    respo = requests.put(url, headers=headers, data=data)
    #    webbrowser.open(f'https://datawrapper.de/chart/{id}/upload')
    headers = {
        'authorization': f'Bearer {dwToken}'
    }
    print((requests.put(url=f'https://api.datawrapper.de/v3/charts/{id}/data', headers=headers, body=data).json()))


def getChartMetadata(id):
    headers = {
        'authorization': f'Bearer {dwToken}'
    }
    metadataJson = requests.get(url=f'https://api.datawrapper.de/v3/charts/{id}', headers=headers)
    metadataDict = metadataJson.json()
    print('Metadaten erhalten')
    return metadataDict, metadataJson

def getFolders():
    headers = {
        'authorization': f'Bearer {dwToken}'
    }
    temp = requests.get(url=f'https://api.datawrapper.de/v3/folders', headers=headers)
    return temp.json()


def metaDatatemp(id):
    metadata, metadataJson = getChartMetadata(id)
    with open('metaconfigs/temp.json', 'w') as writefile:
        json.dump(metadata, writefile)
