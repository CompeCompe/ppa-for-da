import requests
import pandas as pd
import pendulum
import json
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

url = "https://op.itmo.ru/auth/token/login"
username = Variable.get("username")
password = Variable.get("password")
auth_data = {"username": username, "password": password}

token_txt = requests.post(url, auth_data).text
token = json.loads(token_txt)["auth_token"]
headers = {'Content-Type': "application/json",
           'Authorization': "Token " + token}


def get_wp_descriptions():
    # нет учета времени, просто удаляем все записи
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
        """
        truncate stg.work_programs  restart identity cascade;
        """
        )
    target_fields = ['id', 'academic_plan_in_field_of_study', 'wp_in_academic_plan', 'update_ts']
    url_down = 'https://op.itmo.ru/api/record/academic_plan/academic_wp_description/all?format=json&page=1'
    page = requests.get(url_down, headers=headers)
    c = json.loads(page.text)['count']
    for p in range(1, c//10+2):
        url_down = 'https://op.itmo.ru/api/record/academic_plan/academic_wp_description/all?format=json&page=' + str(p)
        dt = pendulum.now().to_iso8601_string()
        page = requests.get(url_down, headers=headers)
        res = json.loads(page.text)['results']
        for r in res:
            df = pd.DataFrame([r], columns=r.keys())
            df['academic_plan_in_field_of_study'] = df[~df['academic_plan_in_field_of_study'].isna()]["academic_plan_in_field_of_study"].apply(lambda st_dict: json.dumps(st_dict))
            df['wp_in_academic_plan'] = df[~df['wp_in_academic_plan'].isna()]["wp_in_academic_plan"].apply(lambda st_dict: json.dumps(st_dict))
            df.loc[:, 'update_ts'] = dt
            PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('stg.work_programs', df.values, target_fields=target_fields)


def get_structural_units():
    # нет учета времени, просто удаляем все записи
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
        """
        truncate stg.su_wp  restart identity cascade;
        """
        )
    url_down = 'https://op.itmo.ru/api/record/structural/workprogram'
    target_fields = ['fak_id', 'fak_title', 'wp_list', 'fk_status']
    status_list = ['EX', 'AC', 'WK']
    for status in status_list:
        url_final = url_down + '?status=' + status
        page = requests.get(url_final, headers=headers)
        res = list(json.loads(page.text))
        for su in res:
            df = pd.DataFrame.from_dict(su)
            # превращаем предпоследний столбец в json
            df['work_programs'] = df[~df['work_programs'].isna()]["work_programs"].apply(lambda st_dict: json.dumps(st_dict))
            df['fk_status'] = status
            PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('stg.su_wp', df.values, target_fields=target_fields)


def get_academic_plans():
    '''
    Для работы данной функции параметр AIRFLOW__CORE__ENABLE_XCOM_PICKLING
    должен быть True в файле /opt/airflow/airflow.cfg
    Так как админка выключена, пришлось менять через терминал контейнера
    в докере
    '''

    from pathlib import Path

    # нет учета времени, просто удаляем все записи
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
        """
        truncate stg.up_description  restart identity cascade;
        """
        )
    target_fields = ['index', 'id', 'plan_type', 'direction_id', 'ns_id',
                     'direction_code', 'direction_name', 'edu_program_id',
                     'edu_program_name', 'faculty_id', 'faculty_name',
                     'training_period', 'university_partner', 'up_country',
                     'lang', 'military_department', 'total_intensity',
                     'ognp_id', 'ognp_name', 'selection_year',
                     'disciplines_blocks']

    DATA_DIR = Path('/lessons/data')

    json_pattern = "*.json"

    for path_file in DATA_DIR.glob(json_pattern):

        with open(path_file, encoding='utf-8') as file:
            data = json.load(file)
            # так как все json разные вводим условие
            if 'result' not in data:
                data = data
            else:
                data = data['result']
            df = pd.DataFrame(data)
            df['disciplines_blocks'] = df[~df['disciplines_blocks'].isna()]["disciplines_blocks"].apply(lambda st_dict: json.dumps(st_dict))
            df = df.reset_index()  # сброс индекса, так как id не уникален
            PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('stg.up_description', df.values, target_fields=target_fields)


# def get_online_courses():
#     # нет учета времени, просто удаляем все записи
#     PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
#         """
#         truncate stg.online_courses  restart identity cascade;
#         """
#         )
#     target_fields = ['id', 'title', 'institution', 'topic_with_online_course']
#     url_down = 'https://op.itmo.ru/api/course/onlinecourse/?format=json&page=1'
#     page = requests.get(url_down, headers=headers)
#     c = json.loads(page.text)['count']
#     for p in range(1, c//10+2):
#         print(p)
#         url_down = 'https://op.itmo.ru/api/course/onlinecourse/?format=json&page=' + str(p)
#         page = requests.get(url_down, headers=headers)
#         res = json.loads(page.text)['results']
#         for r in res:
#             df = pd.DataFrame([r], columns=r.keys())
#             df = df[['id', 'title', 'institution', 'topic_with_online_course']]
#             df['institution'] = df[~df['institution'].isna()]["institution"].apply(lambda st_dict: json.dumps(st_dict))
#             df['topic_with_online_course'] = df[~df['topic_with_online_course'].isna()]["topic_with_online_course"].apply(lambda st_dict: json.dumps(st_dict))
#             PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('stg.online_courses', df.values, target_fields=target_fields)


with DAG(dag_id='get_data',
         start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
         schedule_interval='0 1 * * *',
         catchup=False
         ) as dag:

    t1 = PythonOperator(
        task_id='get_wp_descriptions',
        python_callable=get_wp_descriptions
        )

    t2 = PythonOperator(
        task_id='get_structural_units',
        python_callable=get_structural_units
        )

    # t3 = PythonOperator(
    #     task_id='get_online_courses',
    #     python_callable=get_online_courses
    #     )

    t4 = PythonOperator(
        task_id='get_academic_plans',
        python_callable=get_academic_plans
        )

t1 >> t2 >> t4
