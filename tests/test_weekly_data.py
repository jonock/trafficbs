import pandas as pd

from trafficbs import processing


def test_weekly_comparisons():
    bpdata = pd.read_csv('../data/dailiesnew.csv')
    mivdata = pd.read_csv('../data/dailies_MIV.csv')
    ptdata = pd.read_csv('../data/pt_newpoll.csv', delimiter=';')
    processing.calendarweeks(bpdata, mivdata, ptdata)

# test_weekly_comparisons()
