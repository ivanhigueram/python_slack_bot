'''
Retrive upload status from Dropbox to output a 
table to the bot
'''

import yaml
import numpy as np
import pandas as pd
from datetime import date
from sqlalchemy import create_engine


def create_pgconn(credentials_yaml):
    '''
    Create SQL connection object

    Arguments:
        - credentials_yaml: .yaml file with db credentials
    '''
    with open(credentials_yaml) as f:
        configs = yaml.load(f)
    try:
        conn_url = "postgresql://{user}:{pass}@{host}:{port}/{db}".format(**configs)
        conn = create_engine(conn_url)
    except Exception as e:
        print(str(e))

    return conn

def get_uploaded_ids(dropbox_conn, path):

    response = dropbox_conn.files_list_folder(path)

    entries = []
    if response.has_more is True:
        response_entries = response.entries
        response_cursor = response.cursor
        for entry in response_entries:
            entries.append(entry.name)
        more_responses = dropbox_conn.files_list_folder_continue(response_cursor)
        if more_responses.has_more:
            while more_responses.has_more is True:
                for i in more_responses.entries:
                    entries.append(i.name)
                cur = more_responses.cursor
                more_responses = dropbox_conn.files_list_folder_continue(cur)
            else:
                response_final = dropbox_conn.files_list_folder_continue(cur)
                for i in response_final.entries:
                    entries.append(i.name)
        else:
            for i in more_responses.entries:
                entries.append(i.name)

    else:
        response_entries = response.entries
        for entry in response_entries:
            entries.append(entry.name)


    station_codes = [i.split('_')[3] for i in entries]
    years = [i.split('_')[4].split('.')[0] for i in entries]
    timestamps = [date(int(year), 1, 1).strftime('%Y-%m-%d') for year in years]

    file_ids = [code + time for code, time in zip(station_codes, timestamps)]

    return file_ids

def return_count_files(dropbox_conn,
                       engine,
                       path):

    file_ids = get_uploaded_ids(dropbox_conn, path)

    retrieved_reuslts = pd.read_sql(
			f'''
            select year_query,
                   usaf || wban as station_id,
                   st_astext(st_makepoint(station_lon::float,
                                           station_lat::float)) as geom,
                   array_agg(county_name) as counties,
                   array_agg(geoid) as county_id
            from noaa_raw_data.crosswalk_counties
            where year_query >= '2013-01-01'
            and usaf || wban || year_query not in {tuple(file_ids)}
            group by usaf,
                     wban,
                     year_query,
                     name,
                     station_lat,
                     station_lon,
                     service_time,
                     state,
                     usaf || wban
            order by year_query, station_id
            ''', con=engine)

    retrieved_year = retrieved_reuslts.groupby(['year_query']).count()
    retrieved_year = retrieved_year.filter(['station_id'])
    retrieved_year['estimate_time_hr'] = retrieved_year.station_id/150
    
    return retrieved_year

