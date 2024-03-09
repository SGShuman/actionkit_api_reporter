import os
import sys
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import json, requests
from io import BytesIO

class Axe:
    """Run reports via the ActionKit API. You may either run custom SQL
    or run and pass parameters to existing reports
    """
    def __init__(self, ak_url, user, pw, path='data/'):
        self.report_url = "https://act."+ak_url+"/rest/v1/report/run/sql/"
        self.download_url = "https://act."+ak_url+"/rest/v1/report/download/"
        self.background_url = "https://act."+ak_url+"/rest/v1/report/background/"
        self.backgroundtask_url = "https://act."+ak_url+"/rest/v1/backgroundtask/"
        self.user = user
        self.pw = pw
        self.path = path
        self.headers = {'Content-type': 'application/json'}

    def _exec_query(self, query, verbose=False):
        """Execute a custom SQL query
        """
        start = datetime.now()
        data = '"query": "{q}"'.format(q=query)
        data = '{' + data + ', "refresh":1}'
        response = requests.post(self.report_url, headers=self.headers, data=data, auth=(self.user,self.pw))
        elapsed = datetime.now() - start
        elapsed = elapsed.total_seconds()
        if verbose:
            print("Fetched {r} rows ".format(r=len(response.json)))
            print("Query took {s} seconds ({m} minutes) at {x}".format(s="{:,.1f}".format(elapsed),
                                                                       m="{:,.1f}".format(elapsed/60),
                                                                       x=datetime.now().strftime('%H:%M:%S')))
        return response.json()

    def _download_csv(self, backgroundtask_id, verbose=False, zip=False):
        """Get the CSV from a report that has finished running in the background
        """
        start = datetime.now()
        x = self.download_url+str(backgroundtask_id) +  '/'
        if verbose: print(x)
        csv = requests.get(x, auth=(self.user,self.pw), headers=self.headers)
        if csv.status_code != 200: return None
        df = pd.read_csv(BytesIO(csv.content), compression='zip')
        elapsed = datetime.now() - start
        elapsed = elapsed.total_seconds()
        if verbose:
            print("Fetched {r} rows ".format(r=df.shape[0]))
            print("Query took {s} seconds ({m} minutes) at {x}".format(s="{:,.1f}".format(elapsed),
                                                                       m="{:,.1f}".format(elapsed/60),
                                                                       x=datetime.now().strftime('%H:%M:%S')))
        return df

    def get_last_updated_datetime(self, tz='US/Eastern'):
        q = """
        SELECT MAX(CONVERT_TZ(created_at, 'GMT', '{t}'))
        FROM core_action
        """.format(t=tz)
        j = self._exec_query(q)
        df = pd.DataFrame(j, columns=['last_updated_at'])
        return df['last_updated_at'].values[0]

    def hew(self, report_name, params={}, format='csv'):
        """Run a report asynchronously"""
        params_ = {"format":format, "zip":True, 'refresh':1}
        params_.update(params)
        r = requests.post(self.background_url+str(report_name)+"/",
                          auth=(self.user,self.pw),
                          headers=self.headers,
                          params=params_)
        i = r.headers['location'].split('/')[-2]
        with open(self.path+report_name+'backgroundtask_id.txt', 'w') as f:
            f.write(i)
        return i

    def get_backgroundtask_id(self, report_name):
        """Read latest backgroundtask_id from file"""
        with open(self.path+report_name+'backgroundtask_id.txt', 'r') as f:
            i = f.read()
        return i.strip('\n').strip()

    def log(self, txt, verbose=True):
        """Write to the log file"""
        dt_string = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
        x = dt_string + "    " + txt
        if verbose: print(x)
        with open(self.path+'log.txt', 'a+') as f:
            f.write(x+'\n')

    def sharpen(self, backgroundtask_id):
        """Check if a report is done running"""
        r = requests.get(self.backgroundtask_url+str(backgroundtask_id)+"/",
                          auth=(self.user,self.pw),
                          headers=self.headers)
        status = r.json()['details']['status']
        if status == 'failed':
            self.log(report_name + ' failed')
            return 'failed'
        elif status != 'complete':
            self.log(report_name+" not complete")
            return 'incomplete'
        return status

    def chop(self, report_name, quick=False, zip=False):
        """Get the csv for that report"""
        i = self.get_backgroundtask_id(report_name)
        if not quick:
            status = self.sharpen(i)
            if status != 'complete': return
        df = self._download_csv(i, verbose=False, zip=zip)
        if df is None:
            return
        if df.shape[0] == 0:
            return
        return df
