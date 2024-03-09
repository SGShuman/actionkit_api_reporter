import pickle
import sys
import os
import pandas as pd
from googleapiclient.discovery import build
from gsheet_ghost.credentials.authenticate import get_auth, get_service

# convenience routine
def find_sheet_id_by_name(API, gsheet_id, sheet_name):
    # ugly, but works
    sheets_with_properties = API \
        .spreadsheets() \
        .get(spreadsheetId=gsheet_id, fields='sheets.properties') \
        .execute() \
        .get('sheets')

    for sheet in sheets_with_properties:
        if 'title' in sheet['properties'].keys():
            if sheet['properties']['title'] == sheet_name:
                return sheet['properties']['sheetId']

class Ghost:
    """Send and get data from Google sheets;
    Requires a service account
    """
    def __init__(self, gsheet_id):
        self.gcreds = get_service()
        self.gsheet_id = gsheet_id
        self.API = build('sheets', 'v4', credentials=self.gcreds)

    def _push_csv_to_gsheet(self, path_or_df, gsheet_name, csv=False, header=True):
        """Push a CSV to a gsheet
        Takes either a file path to a csv or a data frame
        For CSV, mark csv=True
        If no column names are present, set header=False
        """
        sheet_id = find_sheet_id_by_name(self.API, self.gsheet_id, gsheet_name)
        if csv:
            with open(path_or_df, 'r') as csv_file:
                output = csv_file.read()
        else:
            output = path_or_df.to_csv(index=False, header=header)
        body = {
            'requests': [{
                'pasteData': {
                    "coordinate": {
                        "sheetId": sheet_id,
                        "rowIndex": "0",  # adapt this if you need different positioning
                        "columnIndex": "0", # adapt this if you need different positioning
                    },
                    "data": output,
                    "type": 'PASTE_NORMAL',
                    "delimiter": ',',
                }
            }]
        }
        request = self.API.spreadsheets().batchUpdate(spreadsheetId=self.gsheet_id, body=body)
        response = request.execute()
        return response

    def get_df_from_gsheet(self, gsheet_name, verbose=False):
        """Copy a gsheet to a df"""
        result = self.API.spreadsheets().values().get(
            spreadsheetId=self.gsheet_id, range=gsheet_name).execute()
        values = result.get('values', [])
        if not values:
            if verbose: print('No data found.')
        else:
            if verbose: print("Data copied")
            return pd.DataFrame(values[1:], columns=values[0])

    def haunt(self, path_or_df, gsheet_name, csv=True, verbose=False, header=True):
        """Upload data to a gsheet"""
        self._push_csv_to_gsheet(path_or_df, gsheet_name, csv=csv, header=header)
        if verbose: print("Uploaded CSV to GSheet")

    def possess(self, gsheet_name, verbose=False):
        """Get data from a Gsheet"""
        df = self.get_df_from_gsheet(gsheet_name, verbose=verbose)
        return df
