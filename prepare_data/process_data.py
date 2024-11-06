import pandas as pd
from pymongo import MongoClient
import json
from sqlalchemy import create_engine

# edit later
connection_str = "postgresql://admin:datateam@localhost:5432/database"

def write_postgres(connection_str, df, table_name, mode='append'):
    engine = create_engine(connection_str)
    df.to_sql(table_name, engine, index=False, if_exists=mode)


def process_data():
    # Connect to MongoDB on localhost
    client = MongoClient("mongodb://localhost:27017/")

    # Connect to collection
    db = client['local']
    collection = db['raw_log']

    # Get data from collection
    res = collection.find({})

    docs = []
    for doc in res:
        docs.append(doc)

    # Get data for SGN to HAN route for example
    l_flight_route = ['SGN.HAN']


    data = docs[0]['SGN.HAN']
    print(data[0])
    # flights_df = pd.json_normalize(
    #     data,
    #     record_path='passengers',
    #     meta=['flight_no', ['details', 'departure'], ['details', 'arrival'], ['details', 'airline']]
    # )
    # df = pd.read_json(json.loads(data[0]))
    # print(df)

    df = pd.json_normalize(
        data,
        # record_path=['flightNumber'],  # Flatten the list of baggage information
        meta=[
            'price', 'price_discout', 'departureAirport', 'arrivalAirport', 
            'flightNumber', 'aircraft', 'date', 'collectionDate',
            ['departureTime', 'hour'], ['departureTime', 'minute'],
            ['arrivalTime', 'hour'], ['arrivalTime', 'minute']
        ],
        sep='_'
    ).drop('BaggageInformation', axis=1).drop_duplicates()

    # test
    write_postgres(connection_str, df, "tmp_table", mode='replace')

if __name__ == '__main__':
    process_data()



