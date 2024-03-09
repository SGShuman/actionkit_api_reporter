import os
import pandas as pd
from actionkit_axe.axe import *
from gsheet_ghost.ghost import *
from time import sleep


### Start Configuration

url = ''
user = ''
pw = ''
sheet_url_id = ''
data_path = 'data/threefifty/'

d = {
    'report_name1':'sheet_name1',
    'report_name2':'sheet_name2',
}

# If instead of overwriting, you just want to add rows to the bottom of the gsheet
# Put the report name here
append_list = []

wait_ = 30
max_retries = 1
### End Configuration

def main():

    with open(data_path+'state.txt', 'r') as f:
        state = f.read()

    axe = Axe(url, user, pw, data_path=data_path)
    if state != 'gsheet_ghost':
        axe.log("Running reports in Actionkit")
        # Run reports asynchronously in ActionKit
        for k, v in d.items():
            axe.log("Running AK report: " + str(k))
            i = axe.hew(k)
        with open(data_path+'state.txt', 'w') as f:
            f.write('gsheet_ghost')
        return
    axe.log("Awakening Ghost")
    ghost = Ghost(sheet_url_id)
    n_reports = len(d)
    # Try to get their results, if not available wait and try again
    retries = 0
    completed_reports = []
    while len(completed_reports) != n_reports and max_retries != retries:
        for k, v in d.items():
            if k in completed_reports: continue
            axe.log("Getting " + str(k))
            df = axe.chop(k, quick=True)
            if df is None:
                axe.log("Failed to get " + str(k))
                continue
            if k in append_list:
                g = ghost.possess(v, verbose=False)
                df = pd.concat([g,df], axis=0, ignore_index=True)
            axe.log("Storing in " + str(v))
            ghost.haunt(df, v, csv=False)
            completed_reports.append(k)
        if len(completed_reports) != n_reports:
            axe.log('Completed {n} of {t} reports, waiting {x} minutes, on {f} retries'.format(x=wait_, n=len(completed_reports), t=n_reports, f=retries))
            retries += 1
            if retries != max_retries:
                sleep(60*wait_)
    if len(completed_reports) == n_reports:
        axe.log("Completed Run")
    else:
        axe.log("Run failed: completed {n} of {t} reports".format(n=len(completed_reports), t=n_reports))
        for k in d.keys():
            if k not in completed_reports:
                axe.log("{} failed".format(k))
    with open(data_path+'state.txt', 'w') as f:
        f.write('actionkit')

if __name__ == '__main__':
    main()
