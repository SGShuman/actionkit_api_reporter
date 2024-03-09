# actionkit_api_reporter
This program enables you to create free Google Sheets dashboards from the data in your ActionKit instance, without syncing it to another database.

First create the necessary reports in ActionKit. Then, point this program at them to run them asynchronously. Then run it again to scoop up the results and send them to Google Sheets. Automate it with a service like Heroku.

## How it works
See the `example_usage.py` file for an example of how to setup the code for a practical application. Use the ActionKit Axe to run reports either synchronously or asynchronously then use the GSheet Ghost to either overwrite or append data to an existing google sheet.

To setup the GSheet Ghost you will need a service account setup and a JSON version of the credentials saved in the `gsheet_ghost/credentials` directory.
