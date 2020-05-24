# trafficbs
Analyzing traffic in Basel based on public data and plotting it with Datawrapper


## Data Gathering
- CSV data was downloaded from the open data portal of the canton Basel-Stadt
- Historic data and recent data was merged into one dataset
- Dailiy updates are polled via the JSON API of the data portal and then merged with the main dataset

## Processing
- Subsets of the datasets are made and totals for each day calculated
- Rolling averages are added

## Visualisation
- The datakicker script allows the direct upload to Datawrapper
- Datawrapper token is stored in the 'credentials.py' file
- New charts can be created by the script and added to a 'chardadmin' file which is used to keep an overview of all the charts
- 'script.py' serves as a daily script that can be run with a cronjob
