import re 
import json 
import time 
import datetime 
import requests 
import pandas as pd 
import numpy as np 
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

query_db = '''SELECT DISTINCT(t.user_id) AS user_id 
            FROM ispring_users_id_courses AS t''' 

URL_TOKEN = 'https://api-learn.ispringlearn.ru/api/v3/token' 
URL_CONTENT = 'https://api-learn.ispringlearn.ru/user' 

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
CHAT_ID = '0000000000' 

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

def new_number(x): 
    '''
    Очистим и приведем номера телефонов к одному формату
    ''' 
    if type(x) == float and np.isnan(x): 
        return np.nan 
    elif type(x) == int: 
        if len(str(x)) == 11: 
            return '7'+''.join(list(str(x)[1:])) 
        elif len(str(x)) == 10: 
            return '7'+''.join(list(str(x))) 
        elif len(str(x)) > 11: 
            return '7'+''.join(list(str(x)[1:11])) 
        elif len(str(x)) < 5: 
            return np.nan 
    elif type(x) == str or type(x) == object: 
        x = re.findall(r'\d+', x) 
        x = ''.join(x) 
        if len(x.replace('0','')) == 0: 
            return np.nan 
        elif x.isdigit(): 
            if len(x) == 11: 
                return '7'+''.join(list(x[1:])) 
            elif len(x) == 10: 
                return '7'+''.join(list(str(x))) 
            elif len(x) > 11: 
                return '7'+''.join(list(x[1:11])) 
            elif len(x) < 5: 
                return np.nan 
        elif x.isalpha(): 
            return np.nan 
        return np.nan 
    elif type(x) == float: 
        x1 = int(x) 
        if len(str(x1)) == 11: 
            return '7'+''.join(list(str(x1)[1:])) 
        elif len(str(x1)) == 10: 
            return '7'+''.join(list(str(x1))) 
        elif len(str(x1)) > 11: 
            return '7'+''.join(list(str(x1)[1:11])) 
        elif len(str(x1)) < 5: 
            return np.nan 
    return x 

def transform_json_data(json_data: dict) -> pd.DataFrame: 
    '''
    Функция принимает json и возвращает табличку
    :param: json_data - входные данные 
    :return: table 
    ''' 
    role = [] 
    role_id = [] 
    user_id = [] 
    department_id = [] 
    status = [] 
    login = [] 
    email = [] 
    first_name = [] 
    last_name = [] 
    country_id = [] 
    UB9mT = [] 
    phone = [] 
    job_title = [] 
    about_me = [] 
    added_date = [] 
    user_roles_role_id = [] 
    user_roles_role_type = [] 
    groups = [] 
    last_login_date = [] 
    subordination_subordination_type = [] 
    co_subordination_subordination_type = [] 
    manageable_department_ids = [] 
    
    for row in json_data: 
        if 'role' in row.keys(): 
            role.append(row['role']) 
        if 'role' not in row.keys(): 
            role.append(None) 
        
        if 'roleId' in row.keys(): 
            role_id.append(row['roleId']) 
        if 'roleId' not in row.keys(): 
            role_id.append(None) 
        
        if 'userId' in row.keys(): 
            user_id.append(row['userId']) 
        if 'userId' not in row.keys(): 
            user_id.append(None) 
        
        if 'departmentId' in row.keys(): 
            department_id.append(row['departmentId']) 
        if 'departmentId' not in row.keys(): 
            department_id.append(None) 
        
        if 'status' in row.keys(): 
            status.append(row['status']) 
        if 'status' not in row.keys(): 
            status.append(None) 
        
        if 'fields' in row.keys(): 
            fiels = [] 
            fild_log = [] 
            fild_email = [] 
            fild_first_name = [] 
            fild_last_name = [] 
            fild_country_id = [] 
            fild_UB9mT = [] 
            fild_phone = [] 
            fild_job_title = [] 
            fild_about_me = [] 
            
            for fild in row['fields']: 
                fiels.append(fild['name']) 
                if fild['name'] == 'login': 
                    fild_log.append(fild['value']) 
                if fild['name'] == 'email': 
                    fild_email.append(fild['value']) 
                if fild['name'] == 'first_name': 
                    fild_first_name.append(fild['value']) 
                if fild['name'] == 'last_name': 
                    fild_last_name.append(fild['value']) 
                if fild['name'] == 'country_id': 
                    fild_country_id.append(fild['value']) 
                if fild['name'] == 'UB9mT': 
                    fild_UB9mT.append(fild['value']) 
                if fild['name'] == 'phone': 
                    fild_phone.append(fild['value']) 
                if fild['name'] == 'job_title': 
                    fild_job_title.append(fild['value']) 
                if fild['name'] == 'about_me': 
                    fild_about_me.append(fild['value']) 
                    
            if 'login' in fiels: 
                login.append('|'.join(fild_log)) 
            if 'login' not in fiels: 
                login.append(None) 
            
            if 'email' in fiels: 
                email.append('|'.join(fild_email)) 
            if 'email' not in fiels: 
                email.append(None) 
                
            if 'first_name' in fiels: 
                first_name.append('|'.join(fild_first_name)) 
            if 'first_name' not in fiels: 
                first_name.append(None) 
            
            if 'last_name' in fiels: 
                last_name.append('|'.join(fild_last_name)) 
            if 'last_name' not in fiels: 
                last_name.append(None) 
            
            if 'country_id' in fiels: 
                country_id.append('|'.join(fild_country_id)) 
            if 'country_id' not in fiels: 
                country_id.append(None) 
            
            if 'UB9mT' in fiels: 
                UB9mT.append('|'.join(fild_UB9mT)) 
            if 'UB9mT' not in fiels: 
                UB9mT.append(None) 
            
            if 'phone' in fiels: 
                phone.append('|'.join(fild_phone)) 
            if 'phone' not in fiels: 
                phone.append(None) 
            
            if 'job_title' in fiels: 
                job_title.append('|'.join(fild_job_title)) 
            if 'job_title' not in fiels: 
                job_title.append(None) 
            
            if 'about_me' in fiels: 
                about_me.append('|'.join(fild_about_me)) 
            if 'about_me' not in fiels: 
                about_me.append(None) 
            
        if 'fields' not in row.keys(): 
            login.append(None) 
            email.append(None) 
            first_name.append(None) 
            last_name.append(None) 
            country_id.append(None) 
            UB9mT.append(None) 
            phone.append(None) 
            job_title.append(None) 
            about_me.append(None) 
        
        if 'addedDate' in row.keys(): 
            added_date.append(row['addedDate']) 
        if 'addedDate' not in row.keys(): 
            added_date.append(None) 
        
        if 'userRoles' in row.keys(): 
            rl_id = [] 
            rl_type = [] 
            for us in row['userRoles']: 
                if 'roleId' in us.keys(): 
                    rl_id.append(us['roleId']) 
                if 'roleType' in us.keys(): 
                    rl_type.append(us['roleType']) 
            user_roles_role_id.append('|'.join(rl_id)) 
            user_roles_role_type.append('|'.join(rl_type)) 
        if 'userRoles' not in row.keys(): 
            user_roles_role_id.append(None) 
            user_roles_role_type.append(None) 
        
        if 'groups' in row.keys(): 
            groups.append('|'.join(row['groups'])) 
        if 'groups' not in row.keys(): 
            groups.append(None) 
            
        if 'lastLoginDate' in row.keys(): 
            last_login_date.append(row['lastLoginDate']) 
        if 'lastLoginDate' not in row.keys(): 
            last_login_date.append(None) 
        
        if 'subordination' in row.keys(): 
            subordination_subordination_type.append(row['subordination']['subordinationType']) 
        if 'subordination' not in row.keys(): 
            subordination_subordination_type.append(None) 
        
        if 'coSubordination' in row.keys(): 
            co_subordination_subordination_type.append(row['coSubordination']['subordinationType']) 
        if 'coSubordination' not in row.keys(): 
            co_subordination_subordination_type.append(None) 
        
        if 'manageableDepartmentIds' in row.keys(): 
            manageable_department_ids.append('|'.join(row['manageableDepartmentIds'])) 
        if 'manageableDepartmentIds' not in row.keys(): 
            manageable_department_ids.append(None) 
        
    ispring_datas_courses = {'role': role, 
                            'role_id': role_id,
                            'user_id': user_id,
                            'department_id': department_id,
                            'status': status,
                            'login': login,
                            'email': email,
                            'first_name': first_name,
                            'last_name': last_name,
                            'country_id': country_id,
                            'ub9mt': UB9mT,
                            'phone': phone,
                            'job_title': job_title,
                            'about_me': about_me,
                            'added_date': added_date,
                            'user_roles_role_id': user_roles_role_id,
                            'user_roles_role_type': user_roles_role_type,
                            'groups': groups,
                            'last_login_date': last_login_date,
                            'subordination_subordination_type': subordination_subordination_type,
                            'co_subordination_subordination_type': co_subordination_subordination_type,
                            'manageable_department_ids': manageable_department_ids} 
    
    return pd.DataFrame(ispring_datas_courses) 
    
@dag(default_args=default_args,
     schedule_interval='30 12 * * *',
     start_date=datetime.datetime(2023, 1, 1),
     catchup=False, 
     )
def get_data_users_courses_from_ispring(): 
    @task
    def get_id_users_db(query: str, engine) -> pd.DataFrame: 
        """
        Функция получает список различных id user из БД
        :param query: запрос к БД
        :return: список
        """
        dis_id_db = pd.read_sql(query, engine) 
        id_list = list(dis_id_db['user_id']) 
        
        return id_list 
    
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
                              name_dag='get_data_users_courses_from_ispring') 

    @task
    def get_data_users_courses(url_content: str, 
                                header_r: dict, 
                                autho_token: str, 
                                id_courses_list: list, 
                                telegram_token: str, 
                                id_chat: str) -> pd.DataFrame: 

        # Присвоим токен к заголовку: 

        header_r['Authorization'] = autho_token 
        
        result = requests.get(url=url_content, headers=header_r) 
        
        if result.status_code == 200: 
            
            # Преобразуем в табличку полученные данные: 

            json_users_data = json.loads(result.text) 
            
            if json_users_data is not None: 
                users_datas = transform_json_data(json_data=json_users_data) 

                # Очистим данные и заполним: 

                users_datas['phone'] = users_datas['phone'].apply(lambda x: new_number(x)) 
                users_datas['status'] = users_datas['status'].fillna(0) 
                users_datas['country_id'] = users_datas['country_id'].fillna(0) 
                users_datas['status'] = users_datas['status'].apply(lambda x: x if str(x).isdigit() else 0) 
                users_datas['country_id'] = users_datas['country_id'].apply(lambda x: x if str(x).isdigit() else 0) 
                users_datas['added_date'] = pd.to_datetime(users_datas['added_date']) 
                users_datas['last_login_date'] = pd.to_datetime(users_datas['last_login_date']) 
                
                # Приведем в необходимый формат данные: 

                users_datas = users_datas.astype({'role': str, 
                                                'role_id': str, 
                                                'user_id': str, 
                                                'department_id': str, 
                                                'status': int, 
                                                'login': str,
                                                'email': str, 
                                                'first_name': str, 
                                                'last_name': str, 
                                                'country_id': int, 
                                                'ub9mt': str, 
                                                'phone': object, 
                                                'job_title': str, 
                                                'about_me': str, 
                                                'added_date': 'datetime64[ns]', 
                                                'user_roles_role_id': str,
                                                'user_roles_role_type': str, 
                                                'groups': str, 
                                                'last_login_date': 'datetime64[ns]',
                                                'subordination_subordination_type': str,
                                                'co_subordination_subordination_type': str, 
                                                'manageable_department_ids': str}) 
        
                return users_datas[~users_datas['user_id'].isin(id_courses_list)].reset_index(drop=True) 
        
            else: 
                error_to_telegram(token=telegram_token, 
                                  chat_id=id_chat, 
                                  e='Нет данных', 
                                  name_dag='get_data_users_courses_from_ispring') 
        
        else: 
            error_to_telegram(token=telegram_token, 
                              chat_id=id_chat, 
                              e=str(result.status_code) + ' ' + str(result.text), 
                              name_dag='get_data_users_courses_from_ispring') 
    
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
    
    dis_id_db = get_id_users_db(query=query_db, engine=engine) 
    
    token_ispring = get_token_ispring(url_token=URL_TOKEN, 
                                     header=HEADER, 
                                     data_urlcode=DATA_URLENCODE, 
                                     telegram_token=TELEGRAM_TOKEN, 
                                     id_chat=CHAT_ID) 
    
    df = get_data_users_courses(url_content=URL_CONTENT, 
                                header_r=HEADERS_R, 
                                autho_token=token_ispring, 
                                id_courses_list=dis_id_db, 
                                telegram_token=TELEGRAM_TOKEN, 
                                id_chat=CHAT_ID) 
    
    append_data_to_db(data=df, table='ispring_users_id_courses', engine=engine) 

get_data_users_courses_from_ispring = get_data_users_courses_from_ispring()
