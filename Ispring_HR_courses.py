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

query_db = '''SELECT t.module_id, t.content_item_id, t.course_id 
            FROM ispring_course_modules_table AS t 
            GROUP BY t.module_id, t.content_item_id, t.course_id''' 

URL_TOKEN = 'https://api-learn.ispringlearn.ru/api/v3/token' 
URL_CONTENT = 'https://api-learn.ispringlearn.ru/courses/modules' 

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
                'client_id': '11bbaa03-8547-11ee-b5e8-8e2de42fa8ef', 
                'client_secret': CLIENT_SECRET, 
                'grant_type': 'client_credentials'
                } 

default_args = {
                'owner': 'airflow'
                } 

TELEGRAM_TOKEN = Variable.get('tg_bot_token') 
CHAT_ID = '000000000' 

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
def get_data_course_modules_from_ispring(): 
    @task
    def get_id_courses_db(query: str, engine) -> pd.DataFrame: 
        """
        Функция получает список различных id курсов из БД
        :param query: запрос к БД
        :return: список
        """
        dis_id_db = pd.read_sql(query, engine) 
        
        return dis_id_db 
    
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

            authorization_token = res_token['access_token'] 
            
            return authorization_token 
        
        else: 
            error_to_telegram(token=telegram_token, 
                              chat_id=id_chat, 
                              e=str(res.status_code) + ' ' + str(res.text), 
                              name_dag='get_data_course_modules_from_ispring') 

    @task
    def get_data_courses(url_content: str, 
                        header_r: dict, 
                        autho_token: str, 
                        id_courses_t: pd.DataFrame, 
                        telegram_token: str, 
                        id_chat: str) -> pd.DataFrame: 

        # Присвоим токен к заголовку: 

        header_r['Authorization'] = autho_token 
        
        result = requests.get(url=url_content, headers=header_r) 
        
        if result.status_code == 200: 
            
            # Таблица со списком курсов: 

            course_content = pd.DataFrame(json.loads(result.text)["modules"]) 
            
            # Переименуем столбцы: 

            course_content = course_content.rename(columns={'moduleId': 'module_id', 
                                                          'contentItemId': 'content_item_id', 
                                                          'courseId': 'course_id', 
                                                          'authorId': 'author_id', 
                                                          'addedDate': 'added_date', 
                                                          'viewUrl': 'view_url'}) 
            
            # Приведем в необходимый тип данные: 

            course_content['added_date'] = pd.to_datetime(course_content['added_date'], utc=True).dt.tz_localize(None).astype('datetime64[ns]') 
            course_content = course_content.astype({'module_id': str, 
                                                  'content_item_id': str, 
                                                  'course_id': str, 
                                                  'title': str, 
                                                  'description': str, 
                                                  'author_id': str, 
                                                  'added_date': 'datetime64[ns]', 
                                                  'view_url': str}) 
            
            # Выберем необходимые столбцы: 

            cols = ['module_id', 'content_item_id', 'course_id', 'title', 'description', 
                   'author_id', 'added_date', 'view_url'] 

            course_content = course_content[cols]  
        
            return course_content[~((course_content['module_id'].isin(id_courses_t['module_id'])) 
                                  & (course_content['content_item_id'].isin(id_courses_t['content_item_id'])) 
                                  & (course_content['course_id'].isin(id_courses_t['course_id'])))].reset_index(drop=True) 
        
        else: 
            error_to_telegram(token=telegram_token, 
                              chat_id=id_chat, 
                              e=str(result.status_code) + ' ' + str(result.text), 
                              name_dag='get_data_course_modules_from_ispring') 
    
    @task
    def append_data_to_db(data: pd.DataFrame, table: str, engine):
        """
        Функция добавляет данные из датафрейма в БД
        :param data: датафрейм
        :param table: имя таблицы
        :return: None
        """
        for i in range(len(data) // 10000 + 1):
            data.iloc[i * 10000: (i + 1) * 10000].to_sql(con=engine,
                                                         name=table,
                                                         if_exists='append',
                                                         index=False)
            time.sleep(3) 

    # Загрузка таблицы со списком курсов в БД
    
    dis_id_db = get_id_courses_db(query=query_db, engine=engine) 
    
    token_ispring = get_token_ispring(url_token=URL_TOKEN, 
                                     header=HEADER, 
                                     data_urlcode=DATA_URLENCODE, 
                                     telegram_token=TELEGRAM_TOKEN, 
                                     id_chat=CHAT_ID) 
    
    df = get_data_courses(url_content=URL_CONTENT, 
                        header_r=HEADERS_R, 
                        autho_token=token_ispring, 
                        id_courses_t=dis_id_db, 
                        telegram_token=TELEGRAM_TOKEN, 
                        id_chat=CHAT_ID) 
    
    append_data_to_db(data=df, table='ispring_course_modules_table', engine=engine) 

get_data_course_modules_from_ispring = get_data_course_modules_from_ispring()
