import requests
import json
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime, timedelta

# файл ключей для подключения к API и DWH
from credentials import NASA_API, DWH_PASSWORD


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


def create_asteroids():
    """Функция создающая таблицу в БД"""
    print(f'create_asteroids: START -> {datetime.now()}')
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
            searching_date date,
            primary key (id, searching_date)
        );
        create index if not exists idx_miss_distance_km_asteroids on asteroids using btree (miss_distance_km) with (fillfactor=50); 
        """
        conn = dwh_connection()
        curs = conn.cursor()

        # инициализация таблицы данных и индекса (fillfactor=50 для меньшей нагрузки, при вставке значений)
        curs.execute(sql)
    except (psycopg2.Error, Exception) as ex:
        print(f'create_asteroids: ERROR -> {datetime.now()}\n{ex}')
    finally:
        # закрытие соединения
        curs.close()
        conn.commit()
        conn.close()
    print(f'create_asteroids: END -> {datetime.now()}')


def insert_data(df: pd.DataFrame):
    """Функция вставки данных в таблицу asteroids"""
    print(f'insert_data: START -> {datetime.now()}')
    try:
        sql = """
        insert into asteroids (id, name, is_potentially_hazardous_asteroid, estimated_diameter_min_km, estimated_diameter_max_km, relative_velocity_km_sec, miss_distance_km, searching_date)
        values (%s, %s, %s, %s, %s, %s, %s, %s) on conflict (id, searching_date) do update set
        name = excluded.name,
        is_potentially_hazardous_asteroid = excluded.is_potentially_hazardous_asteroid,
        estimated_diameter_min_km = excluded.estimated_diameter_min_km,
        estimated_diameter_max_km = excluded.estimated_diameter_max_km,
        relative_velocity_km_sec = excluded.relative_velocity_km_sec,
        miss_distance_km = excluded.miss_distance_km
        """
        conn = dwh_connection()
        curs = conn.cursor()

        # подготовка генератора данных для вставки в таблицу
        data = ((row[1]['id'], 
                 row[1]['name'], 
                 row[1]['is_potentially_hazardous_asteroid'],
                 row[1]['estimated_diameter_min_km'],
                 row[1]['estimated_diameter_max_km'],
                 row[1]['relative_velocity_km_sec'],
                 row[1]['miss_distance_km'],
                 row[1]['searching_date']
                ) for row in df.iterrows())

        # вставка значений в таблицу пачками (функция очень хорошо перформит)
        execute_batch(curs, sql, data, page_size=100)
    except (psycopg2.Error, Exception) as ex:
        print(f'insert_data: ERROR -> {datetime.now()}\n{ex}')
    finally:
        # закрытие соединения
        curs.close()
        conn.commit()
        conn.close()
    print(f'insert_data: END -> {datetime.now()}')


def select_data(start_dt: datetime, end_dt: datetime, mdkm_from, mdkm_to):
    """Функция извлечения данных из таблицы asteroids"""
    print(f'select_data: START -> {datetime.now()}')
    try:
        sql = """
        select name from asteroids
        where searching_date >= %s and searching_date < %s
            and miss_distance_km >= %s and miss_distance_km < %s
        """
        conn = dwh_connection()
        curs = conn.cursor()

        # извлечение данных из DWH
        curs.execute(sql, (start_dt, end_dt, mdkm_from, mdkm_to))
        data = curs.fetchall()
    except (psycopg2.Error, Exception) as ex:
        data = None
        print(f'insert_data: ERROR -> {datetime.now()}\n{ex}')
    finally:
        # закрытие соединения
        curs.close()
        conn.close()

    print(f'select_data: END -> {datetime.now()}')
    return data


if __name__ == '__main__':
    # источники данных (API)
    ne_obj_url = 'https://api.nasa.gov/neo/rest/v1/feed'

    # переменные дат
    today = datetime.now().date()
    three_days_ago = today - timedelta(days=3)

    print(f'etl_near_earth_objects.py > START -> {datetime.now()}')
    # ETL процессы (первая часть)
    neo = get_near_earth_objects(ne_obj_url, three_days_ago, today)
    neo_df = data_transformation(neo)
    metrics = get_metrics(neo_df)

    # вывод метрик
    print(f"potentially_hazardous_count: {metrics['potentially_hazardous_count']}\nname_with_max_estimated_diam: {metrics['name_with_max_estimated_diam']}\nmin_collision_hours: {metrics['min_collision_hours']}")

    # ETL процессы (вторая часть)
    create_asteroids()
    insert_data(neo_df)
    selected_data = select_data(three_days_ago, today, 25000000, 50000000) 
    print(f'etl_near_earth_objects.py > END -> {datetime.now()}')
