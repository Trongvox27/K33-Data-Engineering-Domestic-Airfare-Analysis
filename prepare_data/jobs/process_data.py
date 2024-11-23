import pandas as pd
import json
from sqlalchemy import create_engine
import gcsfs


def write_postgres(connection_str, df, table_name, mode='append'):
    engine = create_engine(connection_str)
    df.to_sql(table_name, engine, index=False, if_exists=mode)

def get_routes():
    return [
    ["SGN","HAN"],
    ["HAN","SGN"],
    ["SGN","VDO"],
    ["VDO","SGN"],
    ["SGN","HPH"],
    ["HPH","SGN"],
    ["SGN","VII"],
    ["VII","SGN"],
    ["SGN","HUI"],
    ["HUI","SGN"],
    ["SGN","DAD"],
    ["DAD","SGN"],
    ["SGN","CXR"],
    ["CXR","SGN"],
    ["SGN","VCA"],
    ["VCA","SGN"],
    ["SGN","PQC"],
    ["PQC","SGN"]
]


def process_data(run_date, POSTGRES_CONFIG):
    # Postgres connection string
    connection_str = f"postgresql://{POSTGRES_CONFIG[user]}:{POSTGRES_CONFIG[password]}@{POSTGRES_CONFIG[host]}:{POSTGRES_CONFIG[port]}/{POSTGRES_CONFIG[dbname]}"

    # read raw data from cloud storage
    y, m, d = run_date.split('-')
    gcs_path = f'dataraw-duongle/data_collect_{y[2:][m][d]}.json'
    
    fs = gcsfs.GCSFileSystem()
    with fs.open(gcs_path, 'rb') as file:
        data = json.load(file)

    # process data
    l_route = get_routes()

    for route in l_route:
        try: 
            _data = data[f'{route[0]}.{route[1]}']

            df = pd.json_normalize(
                _data,
                # record_path=['BaggageInformation'],  # Flatten the list of baggage information
                meta=[
                    'price', 'price_discount', 'departureAirport', 'arrivalAirport', 
                    'flightNumber', 'aircraft', 'date', 'collectionDate',
                    'BaggageInformation',
                    ['departureTime', 'hour'], ['departureTime', 'minute'],
                    ['arrivalTime', 'hour'], ['arrivalTime', 'minute']
                ],
                sep='_'
            )
            
            df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y').dt.strftime('%Y-%m-%d')
            df['collectionDate'] = pd.to_datetime(df['collectionDate'], format='%d-%m-%Y').dt.strftime('%Y-%m-%d')

            df_exploded = df.explode('BaggageInformation')\
                            .explode('BaggageInformation')\
                            .reset_index(drop=True)

            flatten_df = pd.json_normalize(df_exploded['BaggageInformation'])\
                        .rename(columns={
                                'totalFareAgentWithCurrency.currency':'currency', 
                                'totalFareAgentWithCurrency.amount' : 'amount'
                            })\
                        .drop("totalFareAgentWithCurrency.nullOrEmpty", axis=1)
            df_exploded = df_exploded.drop(columns=['BaggageInformation']).join(flatten_df)

            if 'price_discout' in df_exploded.columns:
                df_exploded = df_exploded.rename(columns={'price_discout':'price_discount'})

            # write
            write_postgres(connection_str, df_exploded.drop('airline', axis=1), "flight_raw", mode='append')
            # print(df_exploded.head(1).T)
            # break
        except Exception as e:
            print("ERROR: ", e)
            print(route)
            




