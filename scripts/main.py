import azure.functions as func
import requests
import pandas as pd
import urllib
from sqlalchemy import create_engine
import datetime

start_time = datetime.datetime.now()

con_str = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};SERVER=interlake-bi.database.windows.net;DATABASE=ISS_DW;UID=BIAdmin;PWD=sb98D&B(*#$@")
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % con_str)


stations_lst=[9014080,9063038,9099090,9075035,9075065,9087077,9087096,9087023]

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
base_url_part2 = '&interval=h&product=water_level&datum=LWD&time_zone=lst_ldt&application=Interlake&units=english&format=json'

def main(mytimer: func.TimerRequest) -> None:
    final_station_list = [base_url_part1 + str(i) + base_url_part2 for i in stations_lst]
    final_df = pd.DataFrame()
    for url in final_station_list:
        response = requests.get(url=url)
        data = response.json()
        dfcols = ["StationId", "Time", "WaterLevel"]
        station = data['metadata']['id']
        rows = []
        for d in data['data']:
            stationid = station
            time = d['t']
            waterlevel = d['v']
            rows.append({"StationId": stationid, "Time": time, "WaterLevel": waterlevel})
        df = pd.DataFrame(rows, columns=dfcols)
        final_df = pd.concat([final_df, df], axis=0)

    stations_df = pd.DataFrame(stations_lst, columns=['StationId'])
    stations_df = stations_df.astype({'StationId': 'int64'})

    stations_df['key'] = 0
    times_df['key'] = 0

    stations_final_df = stations_df.merge(times_df, on='key', how='outer')
    stations_final_df = stations_final_df.drop(['key'], axis=1)
    stations_final_df.rename(columns={'Time': 'StationsTime'}, inplace=True)

    final_df = final_df[final_df.WaterLevel != '']
    final_df = final_df.astype({'WaterLevel': 'float64', 'Time': 'datetime64[ns]', 'StationId': 'int64'})
    final_df['WaterLevel'] = final_df['WaterLevel'].apply(lambda x: (x * 12))
    final_df.rename(columns={'Time': 'WaterlevelsTime'}, inplace=True)

    df_ff = pd.merge_asof(
        left=stations_final_df.sort_values(['StationsTime']),
        right=final_df.sort_values(['WaterlevelsTime']),
        left_on='StationsTime',
        right_on='WaterlevelsTime',
        by='StationId',
        direction='nearest',
        allow_exact_matches=True).sort_values(['WaterlevelsTime'])

    df_ff.rename(columns={'WaterlevelsTime': 'Time'}, inplace=True)
    df_ff = df_ff.drop(['StationsTime'], axis=1)
    df_ff.to_sql('WaterLevelMeasurements', con=engine, index=False, if_exists='append', schema='dbo')

    end_time = datetime.datetime.now()
    execution_time = end_time - start_time
    print(f"execution time was: {execution_time}")
    print(df_ff.to_string())
    print("Success")

    return func.HttpResponse(f"Successfully executed  in  {execution_time} seconds")
