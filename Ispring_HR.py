import json 
import time 
import datetime 
import requests 
import pandas as pd 
from airflow.decorators import dag, task
from airflow.models import Variable
from sqlalchemy import create_engine 
from airflow.providers.telegram.hooks.telegram import TelegramHook 


# Параметры для аутентификации и входа

user = 'grigoriy'
host = '10.100.8.47'
db = 'internal_data'
pwd = Variable.get('planning_datas_password') 

postgresql_url = f'postgresql+psycopg2://{user}:{pwd}@{host}/{db}'
engine = create_engine(postgresql_url) 

# запрос к БД для получения id различных курсов

query_db = '''SELECT COUNT(t.user_id) AS count_user_id 
            FROM ispring_results_courses_users AS t''' 

URL_TOKEN = 'https://api-learn.ispringlearn.ru/api/v3/token' 
URL_CONTENT = 'https://api-learn.ispringlearn.ru/learners/results' 

HEADER = { 
        'Content-Type': 'application/x-www-form-urlencoded', 
        'Accept': 'application/json'
        } 

HEADERS_R = { 
            'X-Target-Locale': 'en-US', 
            'Accept': 'application/json'
            } 

CLIENT_SECRET = Variable.get('ispring_api_client_secret') 

DATA_URLENCODE = { 
                'client_id': '11bbaa03-8547-0000-0000-0000000000', 
                'client_secret': CLIENT_SECRET, 
                'grant_type': 'client_credentials'
                } 

default_args = {
                'owner': 'airflow'
                } 

TELEGRAM_TOKEN = Variable.get('tg_bot_token') 
CHAT_ID = '000000000' 

COLUMNS = ['user_id', 'course_id', 'course_title', 'access_date',
           'completion_status', 'is_overdue', 'enrollment_id', 'completion_date',
           'time_spent', 'progress', 'duration', 'awarded_score', 'passing_score',
           'views_count'] 


# Необходимые функции: 

def error_to_telegram(token: str, chat_id: str, e, name_dag: str): 
    '''
    Функция отправляет сообщения об ошибках в чат телеграмм 
    :param: token: "xxx:xxx" API токен для telegram telegram_token
    :param: chat_id куда отправлять сообщение
    :param: e - ошибка 
    :param: name_dag - название DAG
    ''' 
    telegram_conn_id = 'telegram_default' 
    
    telegram_hook = TelegramHook(telegram_conn_id, 
                                token, 
                                chat_id) 
    
    t = datetime.datetime.now() 
    
    date_and_time = str(t.date()) + ' ' + str(t.time()) 
    
    message = f'🛑 В *DAG  {name_dag}* произошла ошибка {e}' 
    message_text = f'{message} Время ошибки: {date_and_time}' 
    
    telegram_hook.send_message({
                                'text': message_text, 
                                'disable_notification': True 
                                }) 

    
@dag(default_args=default_args,
     schedule_interval='30 12 * * *',
     start_date=datetime.datetime(2023, 1, 1),
     catchup=False, 
     )
def get_results_courses_users_ispring(): 
    @task
    def get_len_courses_db(query: str, engine) -> pd.DataFrame: 
        """
        Функция получает количество курсов из БД
        :param query: запрос к БД
        :return: список
        """
        dis_id_db = pd.read_sql(query, engine) 
        
        return int(dis_id_db.iloc[0, 0]) 
    
    @task
    def get_token_ispring(url_token: str, 
                        header: dict, 
                        data_urlcode: dict, 
                        telegram_token: str, 
                        id_chat: str) -> str: 
        '''
        Функция получает токен 
        :param: url_token - url 
        :param: header - headers 
        :param: data_urlcode - params 
        :return: token
        ''' 
        res = requests.post(url=url_token, 
                            headers=header, 
                            data=data_urlcode) 
        
        if res.status_code == 200: 
            
            # Результаты получения токена: 

            res_token = json.loads(res.text) 
            
            # Токен для доступа: 

            authorization_token = res_token['token_type'].capitalize() + ' ' + res_token['access_token'] 
            
            return authorization_token 
        
        else: 
            error_to_telegram(token=telegram_token, 
                              chat_id=id_chat, 
                              e=str(res.status_code) + ' ' + str(res.text), 
                              name_dag='get_results_courses_users_ispring') 

    @task
    def get_data_users_courses(url_content: str, 
                                header_r: dict, 
                                autho_token: str, 
                                cols: list, 
                                telegram_token: str, 
                                id_chat: str) -> pd.DataFrame: 

        # Присвоим токен к заголовку: 

        header_r['Authorization'] = autho_token 
        time.sleep(3) 
        
        result = requests.get(url=url_content, headers=header_r) 
        
        if result.status_code == 200: 
            
            user_courses = pd.DataFrame(columns=cols) 
            datas = json.loads(result.text) 
            
            if 'results' in datas.keys(): 
            
                # Таблица со списком курсов: 

                user_cours = pd.DataFrame(datas['results']) 

                # Переименуем столбцы: 

                user_cours = user_cours.rename(columns={'userId': 'user_id', 
                                                 'courseId': 'course_id', 
                                                 'courseTitle': 'course_title', 
                                                 'accessDate': 'access_date', 
                                                 'completionStatus': 'completion_status',
                                                 'isOverdue': 'is_overdue', 
                                                 'enrollmentId': 'enrollment_id', 
                                                 'completionDate': 'completion_date', 
                                                 'timeSpent': 'time_spent', 
                                                 'awardedScore': 'awarded_score', 
                                                 'passingScore': 'passing_score', 
                                                 'viewsCount': 'views_count'}) 

                user_courses = pd.concat([user_courses, user_cours]) 

                # Приведем в необходимый тип данные: 

                user_courses['access_date'] = pd.to_datetime(user_courses['access_date'], utc=True).dt.tz_localize(None).astype('datetime64[ns]') 
                user_courses['completion_date'] = pd.to_datetime(user_courses['completion_date'], utc=True).dt.tz_localize(None).astype('datetime64[ns]') 

                user_courses = user_courses.astype({'user_id': str, 
                                                     'course_id': str, 
                                                     'course_title': str, 
                                                     'access_date': 'datetime64[ns]', 
                                                     'completion_status': str, 
                                                     'is_overdue': bool, 
                                                     'enrollment_id': str, 
                                                     'completion_date': 'datetime64[ns]', 
                                                     'time_spent': float, 
                                                     'progress': float, 
                                                     'duration': float, 
                                                     'awarded_score': float, 
                                                     'passing_score': float,
                                                     'views_count': float}) 
            
            # Выберем необходимые столбцы: 

            user_courses = user_courses[cols] 
        
            return user_courses.reset_index(drop=True) 
        
        else: 
            error_to_telegram(token=telegram_token, 
                              chat_id=id_chat, 
                              e=str(result.status_code) + ' ' + str(result.text), 
                              name_dag='get_results_courses_users_ispring') 
    
    @task
    def append_data_to_db(data: pd.DataFrame, 
                          table: str, 
                          engine, 
                          id_courses: int): 
        """
        Функция добавляет данные из датафрейма в БД
        :param data: датафрейм
        :param table: имя таблицы
        :return: None
        """ 
        data_drop = tuple(data[['user_id', 'course_id']].apply(lambda x: x[0] + '|' + x[1], axis=1)) 
        
        with engine.connect() as conn: 
            delete_query = f"DELETE FROM ispring_results_courses_users AS t WHERE CONCAT(t.user_id, '|', t.course_id) IN {data_drop}" 
            conn.execute(delete_query) 

        time.sleep(1) 
            
        for i in range(len(data) // 10000 + 1):
            data.iloc[i * 10000: (i + 1) * 10000].to_sql(con=engine,
                                                         name=table,
                                                         if_exists='append',
                                                         index=False)
            time.sleep(3) 


    # Загрузка таблицы со списком курсов в БД
    
    dis_id_db = get_len_courses_db(query=query_db, engine=engine) 
    
    token_ispring = get_token_ispring(url_token=URL_TOKEN, 
                                     header=HEADER, 
                                     data_urlcode=DATA_URLENCODE, 
                                     telegram_token=TELEGRAM_TOKEN, 
                                     id_chat=CHAT_ID) 
    
    df = get_data_users_courses(url_content=URL_CONTENT, 
                                header_r=HEADERS_R, 
                                autho_token=token_ispring, 
                                cols=COLUMNS, 
                                telegram_token=TELEGRAM_TOKEN, 
                                id_chat=CHAT_ID) 
    
    append_data_to_db(data=df, 
                      table='ispring_results_courses_users', 
                      engine=engine, 
                      id_courses=dis_id_db) 

get_results_courses_users_ispring = get_results_courses_users_ispring()
