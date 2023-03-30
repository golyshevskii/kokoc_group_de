import psycopg2
import requests
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# файл ключей для подключения к API
from creds import NASA_API, DWH_PASSWORD


def get_near_earth_objects(url: str, start_dt: datetime, end_dt: datetime):
    """Функция записывающая в файл и возвращающая список астероидов за последние 3 дня"""
    print(f'get_near_earth_objects: START -> {datetime.now()}')
    try:
        # определение параметров для извлечения данных из API
        params = {'start_date': start_dt.strftime('%Y-%m-%d'), 
                  'end_date': end_dt.strftime('%Y-%m-%d'), 
                  'api_key': NASA_API}

        # извлечение данных        
        response = requests.get(url, params=params).json()['near_earth_objects']

        # запись данных в файл
        with open('near_earth_objects_3_days.json', 'w') as outfile:
            json.dump(response, outfile)
    except (requests.exceptions.HTTPError, Exception) as ex:
        response = None
        print(f'get_near_earth_objects: ERROR -> {datetime.now()}\n{ex}')

    print(f'get_near_earth_objects: END -> {datetime.now()}')
    return response


def data_transformation(data: dict):
    """Функция трансформирующая данные в DataFrame"""
    print(f'data_transformation: START -> {datetime.now()}')
    # словарик атрибутов
    df_data = {
        'id': [],
        'name': [],
        'is_potentially_hazardous_asteroid': [],
        'estimated_diameter_min_km': [],
        'estimated_diameter_max_km': [],
        'relative_velocity_km_sec': [],
        'miss_distance_km': [],
        'searching_date': []
    }

    # циклы извлечения, трансформации и наполнения данными словарика атрибутов
    try:
        for date in data.keys():
            for r in data[date]:
                df_data['id'].append(int(r['id']))
                df_data['name'].append(r['name'])
                df_data['is_potentially_hazardous_asteroid'].append(r['is_potentially_hazardous_asteroid'])
                df_data['estimated_diameter_min_km'].append(float(r['estimated_diameter']['kilometers']['estimated_diameter_min']))
                df_data['estimated_diameter_max_km'].append(float(r['estimated_diameter']['kilometers']['estimated_diameter_max']))
                df_data['relative_velocity_km_sec'].append(float(r['close_approach_data'][0]['relative_velocity']['kilometers_per_second']))
                df_data['miss_distance_km'].append(float(r['close_approach_data'][0]['miss_distance']['kilometers']))
                df_data['searching_date'].append(datetime.strptime(date, "%Y-%m-%d").date())

        # формирование таблицы данных и их запись в файл csv
        df = pd.DataFrame(df_data)
        df.to_csv('near_earth_objects_3_days.csv', index=False)
    except Exception as ex:
        df = None
        print(f'data_transformation: ERROR -> {datetime.now()}\n{ex}')

    print(f'data_transformation: END -> {datetime.now()}')
    return df


def get_metrics(df: pd.DataFrame):
    """Функция построение метрик относительно поступаемой таблицы с информацией об астероидах"""
    print(f'get_metrics: START -> {datetime.now()}')
    try:
        # формирование метрик
        metrics = {'potentially_hazardous_count': df['is_potentially_hazardous_asteroid'].value_counts().loc[True],
                   'name_with_max_estimated_diam': df.loc[(df['estimated_diameter_max_km'].max() == df['estimated_diameter_max_km'])].name.iloc[0],
                   'min_collision_hours': (df['miss_distance_km'] / df['relative_velocity_km_sec']).min() / 3600}
    except Exception as ex:
        metrics = None
        print(f'get_metrics: ERROR -> {datetime.now()}')

    print(f'get_metrics: END -> {datetime.now()}')
    return metrics


def dwh_connection():
    """Функция устанавливающая соединение с БД"""
    try:
        conn = psycopg2.connect(host='135.181.61.116', user='slava.golyshevskii', password=DWH_PASSWORD, dbname='postgres', port='5432')
    except (psycopg2.Error) as ex:
        conn = None
        print(f'dwh_connection: ERROR -> {datetime.now()}\n{ex}')
    return conn


def create_asteroids(conn: psycopg2.connection):
    """Функция создающая таблицу в БД"""
    print(f'create_asteroids: START -> {datetime.now()}')
    # инициализация таблички данных
    try:
        sql = """
        create table if not exists asteroids (
	        id int8,
	        name varchar(256),
            is_potentially_hazardous_asteroid bool,
            estimated_diameter_min_km float8,
            estimated_diameter_max_km float8,
            relative_velocity_km_sec float8,
            miss_distance_km float8,
            searching_date date
        );
        """
        curs = conn.cursor()
        curs.execute(sql)
    except (psycopg2.Error, Exception) as ex:
        print(f'create_asteroids: ERROR -> {datetime.now()}\n{ex}')
    finally:
        # закрытие соединения
        curs.close()
        conn.close()
    print(f'create_asteroids: END -> {datetime.now()}')


def insert_data(df: pd.DataFrame):
    print(f'insert_data: START -> {datetime.now()}')

    print(f'insert_data: END -> {datetime.now()}')


if __name__ == '__main__':
    # источники данных (API)
    ne_obj_url = 'https://api.nasa.gov/neo/rest/v1/feed'

    # переменные дат
    today = datetime.now().date()
    three_days_ago = today - timedelta(days=3)

    # ETL процессы (первая часть)
    neo = get_near_earth_objects(ne_obj_url, three_days_ago, today)
    neo_df = data_transformation(neo)
    metrics = get_metrics(neo_df)

    # ETL процессы (вторая часть)
    conn = dwh_connection()
    create_asteroids(conn)
    insert_data(conn, neo_df)

    # вывод метрик
    print(f"potentially_hazardous_count: {metrics['potentially_hazardous_count']}\nname_with_max_estimated_diam: {metrics['name_with_max_estimated_diam']}\nmin_collision_hours: {metrics['min_collision_hours']}")
