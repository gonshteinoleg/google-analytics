import pandas as pd
from datetime import datetime, timedelta
from gaapi4py import GAClient
import pandas as pd
import time
from google.cloud import bigquery

import warnings
warnings.filterwarnings('ignore')

SERVICE_KEY = 'api-key.json'
c = GAClient(SERVICE_KEY)

def get_data_from_ga(view, start, end):
    c.set_view_id(view)
    c.set_dateranges(start, end)
    request_body = {
        'dimensions': {
            'ga:date','ga:hostname','ga:eventAction','ga:eventLabel','ga:adwordsCustomerID','ga:adwordsCampaignID','ga:adwordsAdGroupID','ga:adwordsCreativeID'
        },
        'metrics': {
            'ga:totalEvents'
        },
        'filter': 'ga:eventCategory==Bitrix_conversions;ga:sourceMedium==google / cpc'
    }
    df = c.get_all_data(request_body)
    df = df['data']
    df['date'] = pd.to_datetime(df['date']).dt.date
    return df

def load_data_to_gbq(df, dataset_name, table_name):
    client = bigquery.Client.from_service_account_json(
        'secret-file.json')

    dataset_ref = client.dataset(dataset_name)
    dataset = bigquery.Dataset(dataset_ref)

    table_ref = dataset_ref.table(table_name)

    client.load_table_from_dataframe(df, table_ref).result()

# get data
result = get_data_from_ga_in_loop('1234567', '2021-12-15', '2022-01-15')

# load data to gbq
load_data_to_gbq(result, 'dataset-name', 'table-name')
