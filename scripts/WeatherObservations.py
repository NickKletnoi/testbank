#!/usr/bin/python
import requests
import pandas as pd
import urllib
from sqlalchemy import create_engine
import datetime
import pyodbc

start_time = datetime.datetime.now()

#conn = pyodbc.connect('DRIVER={SQL Server};SERVER=interlake-bi.database.windows.net,1433', user='BIAdmin@interlake-bi', password='sb98D&B(*#$@', database='ISS_DW')
#conn = pyodbc.connect('DRIVER={/opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.0.so.1.1};SERVER=interlake-bi.database.windows.net,1433', user='BIAdmin@interlake-bi', password='sb98D&B(*#$@', database='ISS_DW')
#con_str = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};SERVER=interlake-bi.database.windows.net;DATABASE=ISS_DW;UID=BIAdmin;PWD=sb98D&B(*#$@")
#engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % con_str)

uname = 'BIAdmin@interlake-bi'
pword = 'sb98D&B(*#$@'
server = 'interlake-bi.database.windows.net'
dbname = 'ISS_DW'
engine = create_engine("mssql+pyodbc://" + uname + ":" + pword + "@" + server + "/" + dbname + "?driver=ODBC+Driver+17+for+SQL+Server")

stations_lst=[9063063,9075099,9075099,9076027,9076033,9076070,9075080,9087031,9099064,9063079,9063053,9063063,9075065]

times_df = pd.DataFrame({"Time": [
    pd.Timestamp("01:00:00.000"),
    pd.Timestamp("02:00:00.000"),
    pd.Timestamp("03:00:00.000"),
    pd.Timestamp("04:00:00.000"),
    pd.Timestamp("05:00:00.000"),
    pd.Timestamp("06:00:00.000"),
    pd.Timestamp("07:00:00.000"),
    pd.Timestamp("08:00:00.000"),
    pd.Timestamp("09:00:00.000"),
    pd.Timestamp("10:00:00.000"),
    pd.Timestamp("11:00:00.000"),
    pd.Timestamp("12:00:00.000"),
    pd.Timestamp("13:00:00.000"),
    pd.Timestamp("14:00:00.000"),
    pd.Timestamp("15:00:00.000"),
    pd.Timestamp("16:00:00.000"),
    pd.Timestamp("17:00:00.000"),
    pd.Timestamp("18:00:00.000"),
    pd.Timestamp("19:00:00.000"),
    pd.Timestamp("20:00:00.000"),
    pd.Timestamp("21:00:00.000"),
    pd.Timestamp("22:00:00.000"),
    pd.Timestamp("23:00:00.000"),
    pd.Timestamp("00:00:00.000")
]})


base_url_part1 = 'https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?date=today&station='
base_url_part2_wind = '&product=wind&datum=IGLD&time_zone=gmt&units=english&format=json'
base_url_part2_airtemp = '&product=air_temperature&datum=IGLD&time_zone=gmt&units=english&format=json'
base_url_part2_airpress = '&product=air_pressure&datum=IGLD&time_zone=gmt&units=english&format=json'
base_url_part2_watertemp = '&product=water_temperature&datum=IGLD&time_zone=gmt&units=english&format=json'

def ingest_wind_from_noaa():
    final_station_list = [base_url_part1 + str(i) + base_url_part2_wind for i in stations_lst]
    final_df = pd.DataFrame()
    for url in final_station_list:
        response = requests.get(url=url)
        data = response.json()
        dfcols = ["StationId", "ObservationTime", "WindSpeed","WindDirection"]
        station = data['metadata']['id']
        rows = []
        for d in data['data']:
            stationid = station
            time = d['t']
            wind_speed = d['s']
            wind_direction = d['dr']
            rows.append({"StationId": stationid, "ObservationTime": time, "WindSpeed": wind_speed, "WindDirection": wind_direction})
        df = pd.DataFrame(rows, columns=dfcols)
        final_df = pd.concat([final_df, df], axis=0)

    stations_df = pd.DataFrame(stations_lst, columns=['StationId'])
    stations_df = stations_df.astype({'StationId': 'int64'})

    stations_df['key'] = 0
    times_df['key'] = 0

    stations_final_df = stations_df.merge(times_df, on='key', how='outer')
    stations_final_df = stations_final_df.drop(['key'], axis=1)
    stations_final_df.rename(columns={'Time': 'StationsTime'}, inplace=True)

    final_df = final_df[final_df.WindSpeed != '']
    final_df = final_df.astype({'StationId': 'int64','ObservationTime': 'datetime64[ns]','WindSpeed': 'string','WindDirection': 'string'})
    final_df.rename(columns={'ObservationTime': 'WindTime'}, inplace=True)

    df_ff = pd.merge_asof(
        left=stations_final_df.sort_values(['StationsTime']),
        right=final_df.sort_values(['WindTime']),
        left_on='StationsTime',
        right_on='WindTime',
        by='StationId',
        direction='nearest',
        allow_exact_matches=True).sort_values(['WindTime'])

    df_ff.rename(columns={'WindTime': 'ObservationTime'}, inplace=True)
    df_ff = df_ff.drop(['StationsTime'], axis=1)

    df_ff.to_sql('WeatherObservations_Wind_stg', con=engine, index=False, if_exists='append', schema='dbo')

    end_time = datetime.datetime.now()
    execution_time = end_time - start_time
    print(f"execution time was: {execution_time}")
    print(df_ff.to_string())
    print("Success")


def ingest_airtemp_from_noaa():
    final_station_list = [base_url_part1 + str(i) + base_url_part2_airtemp for i in stations_lst]
    final_df = pd.DataFrame()
    for url in final_station_list:
        response = requests.get(url=url)
        data = response.json()
        dfcols = ["StationId", "ObservationTime", "AirTemperature"]
        station = data['metadata']['id']
        rows = []
        for d in data['data']:
            stationid = station
            time = d['t']
            airtemp = d['v']
            rows.append({"StationId": stationid, "ObservationTime": time, "AirTemperature": airtemp})
        df = pd.DataFrame(rows, columns=dfcols)
        final_df = pd.concat([final_df, df], axis=0)

    stations_df = pd.DataFrame(stations_lst, columns=['StationId'])
    stations_df = stations_df.astype({'StationId': 'int64'})

    stations_df['key'] = 0
    times_df['key'] = 0

    stations_final_df = stations_df.merge(times_df, on='key', how='outer')
    stations_final_df = stations_final_df.drop(['key'], axis=1)
    stations_final_df.rename(columns={'Time': 'StationsTime'}, inplace=True)

    final_df = final_df[final_df.AirTemperature != '']
    final_df = final_df.astype({'StationId': 'int64', 'ObservationTime': 'datetime64[ns]', 'AirTemperature': 'string'})
    final_df.rename(columns={'ObservationTime': 'AirTemperatureTime'}, inplace=True)

    df_ff = pd.merge_asof(
        left=stations_final_df.sort_values(['StationsTime']),
        right=final_df.sort_values(['AirTemperatureTime']),
        left_on='StationsTime',
        right_on='AirTemperatureTime',
        by='StationId',
        direction='nearest',
        allow_exact_matches=True).sort_values(['AirTemperatureTime'])

    df_ff.rename(columns={'AirTemperatureTime': 'ObservationTime'}, inplace=True)
    df_ff = df_ff.drop(['StationsTime'], axis=1)

    df_ff.to_sql('WeatherObservations_AirTemp_stg', con=engine, index=False, if_exists='append', schema='dbo')

    end_time = datetime.datetime.now()
    execution_time = end_time - start_time
    print(f"execution time was: {execution_time}")
    print(df_ff.to_string())
    print("Success")


def ingest_watertemp_from_noaa():
    final_station_list = [base_url_part1 + str(i) + base_url_part2_watertemp for i in stations_lst]
    final_df = pd.DataFrame()
    for url in final_station_list:
        response = requests.get(url=url)
        data = response.json()
        dfcols = ["StationId", "ObservationTime", "WaterTemperature"]
        station = data['metadata']['id']
        rows = []
        for d in data['data']:
            stationid = station
            time = d['t']
            airtemp = d['v']
            rows.append({"StationId": stationid, "ObservationTime": time, "WaterTemperature": airtemp})
        df = pd.DataFrame(rows, columns=dfcols)
        final_df = pd.concat([final_df, df], axis=0)

    stations_df = pd.DataFrame(stations_lst, columns=['StationId'])
    stations_df = stations_df.astype({'StationId': 'int64'})

    stations_df['key'] = 0
    times_df['key'] = 0

    stations_final_df = stations_df.merge(times_df, on='key', how='outer')
    stations_final_df = stations_final_df.drop(['key'], axis=1)
    stations_final_df.rename(columns={'Time': 'StationsTime'}, inplace=True)

    final_df = final_df[final_df.WaterTemperature != '']
    final_df = final_df.astype({'StationId': 'int64','ObservationTime': 'datetime64[ns]','WaterTemperature': 'string'})
    final_df.rename(columns={'ObservationTime': 'WaterTemperatureTime'}, inplace=True)

    df_ff = pd.merge_asof(
        left=stations_final_df.sort_values(['StationsTime']),
        right=final_df.sort_values(['WaterTemperatureTime']),
        left_on='StationsTime',
        right_on='WaterTemperatureTime',
        by='StationId',
        direction='nearest',
        allow_exact_matches=True).sort_values(['WaterTemperatureTime'])

    df_ff.rename(columns={'WaterTemperatureTime': 'ObservationTime'}, inplace=True)
    df_ff = df_ff.drop(['StationsTime'], axis=1)

    df_ff.to_sql('WeatherObservations_WaterTemp_stg', con=engine, index=False, if_exists='append', schema='dbo')

    end_time = datetime.datetime.now()
    execution_time = end_time - start_time
    print(f"execution time was: {execution_time}")
    print(df_ff.to_string())
    print("Success")

def ingest_airpress_from_noaa():
    final_station_list = [base_url_part1 + str(i) + base_url_part2_airpress for i in stations_lst]
    final_df = pd.DataFrame()
    for url in final_station_list:
        response = requests.get(url=url)
        data = response.json()
        dfcols = ["StationId", "ObservationTime", "AirPressure"]
        station = data['metadata']['id']
        rows = []
        for d in data['data']:
            stationid = station
            time = d['t']
            airpress = d['v']
            rows.append({"StationId": stationid, "ObservationTime": time, "AirPressure": airpress})
        df = pd.DataFrame(rows, columns=dfcols)
        final_df = pd.concat([final_df, df], axis=0)

    stations_df = pd.DataFrame(stations_lst, columns=['StationId'])
    stations_df = stations_df.astype({'StationId': 'int64'})

    stations_df['key'] = 0
    times_df['key'] = 0

    stations_final_df = stations_df.merge(times_df, on='key', how='outer')
    stations_final_df = stations_final_df.drop(['key'], axis=1)
    stations_final_df.rename(columns={'Time': 'StationsTime'}, inplace=True)

    final_df = final_df[final_df.AirPressure != '']
    final_df = final_df.astype({'StationId': 'int64','ObservationTime': 'datetime64[ns]','AirPressure': 'string'})
    final_df.rename(columns={'ObservationTime': 'AirPressureTime'}, inplace=True)

    df_ff = pd.merge_asof(
        left=stations_final_df.sort_values(['StationsTime']),
        right=final_df.sort_values(['AirPressureTime']),
        left_on='StationsTime',
        right_on='AirPressureTime',
        by='StationId',
        direction='nearest',
        allow_exact_matches=True).sort_values(['AirPressureTime'])

    df_ff.rename(columns={'AirPressureTime': 'ObservationTime'}, inplace=True)
    df_ff = df_ff.drop(['StationsTime'], axis=1)

    df_ff.to_sql('WeatherObservations_AirPressure_stg', con=engine, index=False, if_exists='append', schema='dbo')

    end_time = datetime.datetime.now()
    execution_time = end_time - start_time
    print(f"execution time was: {execution_time}")
    print(df_ff.to_string())
    print("Success")

def final_table_assemble():
    conn.execute("exec dbo.sp_weatherobs_insert")
    conn.commit()

ingest_wind_from_noaa()
ingest_airtemp_from_noaa()
ingest_watertemp_from_noaa()
ingest_airpress_from_noaa()
#final_table_assemble()
