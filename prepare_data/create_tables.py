import pandas as pd
import json
from sqlalchemy import create_engine
import psycopg2


# edit later
connection_str = "postgresql://airflow:airflow@34.171.191.111:5432/postgres"

def write_postgres(connection_str, df, table_name, mode='append'):
    engine = create_engine(connection_str)
    df.to_sql(table_name, engine, index=False, if_exists=mode)

def read_postgres(query):
    conn = psycopg2.connect(
        dbname="postgres",
        user="airflow",
        password="airflow",
        host="34.171.191.111",
        port="5432"
    )    
    return pd.read_sql_query(query, conn)

def read_excel(path, sheet_name):
    return pd.read_excel(path, sheet_name=sheet_name)

def write_table_from_sheet(df, table_name, connection_str):
    write_postgres(connection_str, df, table_name, mode='replace')

def create_flight_table(table_name):
    query = \
    """
    with tmp as (select
        distinct
            SPLIT_PART(fl."flightNumber", '-', 1) AS prefix_flight,
            "flightNumber" as flight_number,
            a."Airport ID" as departure_airport_id,
            b."Airport ID" as arrival_airport_id,
            fl.aircraft,
            fl."departureTime_hour" as departure_time_hour,
            fl."departureTime_minute" as departure_time_minute,
            fl."arrivalTime_hour" as arrival_time_hour,
            fl."arrivalTime_minute" as arrival_time_minute,
            fl.date as date_departure
    from flight_raw fl join international_airport a on fl."departureAirport" = a."IATA Code"
        join international_airport b on fl."arrivalAirport" = b."IATA Code")

    select tmp.*, airline.airline_id as airline_id
    from tmp left join airline on tmp.prefix_flight=airline.iata_code
    """
    df = read_postgres(query).drop("prefix_flight", axis=1).reset_index()\
        .rename(columns={'index':'flight_id'})
    # print(df.head(10))
    write_postgres(connection_str, df, table_name, mode='replace')

def create_baggage_table(table_name):
    query = """
            with fr as (
                select distinct
                    title, currency, amount,
                    "flightNumber" as flight_number,
                    a."Airport ID" as departure_airport_id,
                    b."Airport ID" as arrival_airport_id,
                    fl.aircraft,
                    fl."departureTime_hour" as departure_time_hour,
                    fl."departureTime_minute" as departure_time_minute,
                    fl.date as date_departure
            from flight_raw fl join international_airport a on fl."departureAirport" = a."IATA Code"
                join international_airport b on fl."arrivalAirport" = b."IATA Code")

            select distinct
                flight_id,
                title, currency, amount
            from fr left join flight f
                on
                    fr.flight_number=f.flight_number
                    and fr.departure_airport_id=f.departure_airport_id
                    and fr.arrival_airport_id=f.arrival_airport_id
                    and fr.aircraft=f.aircraft
                    and fr.departure_time_hour = f.departure_time_hour
                    and fr.departure_time_minute = f.departure_time_minute
                    and fr.date_departure=f.date_departure
            """
    df = read_postgres(query).reset_index()\
        .rename(columns={'index':'baggage_id'})
    # print(df.head(10))
    write_postgres(connection_str, df, table_name, mode='replace')

def create_price_table(table_name):
    query = \
    """
    with fr as (
        select distinct
            price,
            price_discount,
            currency, title, amount,
            "collectionDate" as collection_date,
            "flightNumber" as flight_number,
            a."Airport ID" as departure_airport_id,
            b."Airport ID" as arrival_airport_id,
            fl.aircraft,
            fl."departureTime_hour" as departure_time_hour,
            fl."departureTime_minute" as departure_time_minute,
            fl.date as date_departure
    from flight_raw fl join international_airport a on fl."departureAirport" = a."IATA Code"
        join international_airport b on fl."arrivalAirport" = b."IATA Code")

    select distinct 
        collection_date || '_' || f.flight_id as id,
        f.flight_id,
        fr.price,
        fr.price_discount,
        fr.currency,
        fr.collection_date
    from fr left join flight f
        on
            fr.flight_number=f.flight_number
            and fr.departure_airport_id=f.departure_airport_id
            and fr.arrival_airport_id=f.arrival_airport_id
            and fr.aircraft=f.aircraft
            and fr.departure_time_hour = f.departure_time_hour
            and fr.departure_time_minute = f.departure_time_minute
            and fr.date_departure=f.date_departure
        
    """
    # left join baggage bg on
        #     fr.title = bg.title and fr.amount = bg.amount and f.flight_id=bg.flight_id
    df = read_postgres(query)
    write_postgres(connection_str, df, table_name, mode='replace')

def dedup_raw():
    query = \
    """
    select distinct *
    from flight_raw 
    """
    df = read_postgres(query)
    write_postgres(connection_str, df, 'flight_raw', mode='replace')

def process():
    raw_name = 'flight_raw'
    
   
    print("===Airline table===")
    xlsx_file_path = "./data/Flight_Schema.xlsx"
    airline_df = read_excel(xlsx_file_path, "Airlines")
    write_table_from_sheet(airline_df, 'airline', connection_str)
    
    print("===International airports table===")    
    inter_airport_df = read_excel(xlsx_file_path, 'International airports')
    write_table_from_sheet(inter_airport_df, 'international_airport', connection_str)

    print("===Flight table===")
    create_flight_table("flight")

    print("===Baggage table===")
    create_baggage_table("baggage")

    print("===Price table===")
    create_price_table('price')
    # dedup_raw()

    print("Done")

if __name__ == '__main__':
    process()