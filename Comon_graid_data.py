import re 
import requests 
import time 
import datetime 
import pandas as pd 

from urllib3.util.retry import Retry 
from airflow.decorators import dag, task
from airflow.models import Variable 
from airflow.providers.telegram.hooks.telegram import TelegramHook 
from sqlalchemy import create_engine

# Параметры для аутентификации и входа 

user = 'grigoriy'
host = '10.100.8.47'
db = 'internal_data'
pwd = Variable.get('planning_datas_password') 

postgresql_url = f'postgresql+psycopg2://{user}:{pwd}@{host}/{db}'
engine = create_engine(postgresql_url)

# Запрос к БД для получения списка задач 

ID_EMPLOYEES = "SELECT DISTINCT(t.id) FROM public.grade_employees AS t" 

# Входные параметры

USER_LOGIN = 'xxxxxxxxxxxxx@deltaclick.ru' 
USER_PASSWORD = Variable.get('comon_password') 

URL = 'https://comon.company/bitrix/admin/highloadblock_rows_list.php?login=yes&ENTITY_ID=8&lang=ru#authorize' 

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0'
}

default_args = {'owner': 'airflow'}

REGEX_TABLE = '<table class="adm-list-table" id="tbl_grade_crossing">(.*?)</table>'  
TAGS_HEAD = 'thead' 
TAGS_BODY = 'tbody' 
HEAD, BODY = list(), list() 

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
    message_text = f'{message}. Время ошибки: {date_and_time}' 
    
    telegram_hook.send_message({
                                'text': message_text, 
                                'disable_notification': True
                                }) 

def read_blok(tags: str, table: str) -> str: 
    '''
    Считываем блок таблицы
    '''
    seq = '<'+tags+'>(.*?)</'+tags+'>' 
    matches = re.findall(seq, table, flags=re.DOTALL) 
    
    return matches[0] 


def read_rows_head(table: str, rows_values: list) -> list: 
    ''' 
    Считываем подстроки между тегами и 
    складываем в список строк
    ''' 
    value_seq = '<div class="adm-list-table-cell-inner">(.*?)</div>' 
    values_row = re.findall(value_seq, table, flags=re.DOTALL) 
    values_row = [x.strip().replace('\r\n','') for x in values_row] 
    
    return values_row 


def id_tags(value: str): 
    '''
    Возвращает id между тегов
    ''' 
    seq = '>(.*?)</a>' 
    values = re.findall(seq, value, flags=re.DOTALL) 
    values = [x.strip().replace('\r\n','') for x in values] 
    
    return values[0]


def read_rows_body(table: str, rows_values: list) -> list: 
    ''' 
    Считываем подстроки между тегами и 
    складываем в список строк
    ''' 
    row_seq = '<tr class="adm-list-table-row" (.*?)</tr>' 
    value_seq = '<td class="adm-list-table-cell">(.*?)</td>' 
    value_seq_last = '<td class="adm-list-table-cell adm-list-table-cell-last">(.*?)</td>' 
    
    rows = re.findall(row_seq, table, flags=re.DOTALL) 
    
    for row in rows: 
        matches = re.findall(value_seq, row, flags=re.DOTALL) 
        matches = [x.strip().replace('\r\n','') for x in matches] 
        matches_last = re.findall(value_seq_last, row, flags=re.DOTALL) 
        matches_last = [x.strip().replace('\r\n','') for x in matches_last] 
        matches[0] = id_tags(matches[0]) 
        matches.append(matches_last[0]) 
        rows_values.append(matches) 
    
    return rows_values 


def number_page(page_text: str) -> list: 
    '''
    Получение номеров страниц
    ''' 
    number_pages = list() 
    seq_number_pages = '<div class="adm-nav-pages-block">(.*?)</div>' 
    seq_page = ' class="adm-nav-page">(.*?)</a>' 
    
    block_number_page = re.findall(seq_number_pages, page_text, flags=re.DOTALL) 
    
    for pages in block_number_page: 
        number = re.findall(seq_page, pages, flags=re.DOTALL) 
        number = [x.strip().replace('\r\n','') for x in number] 
        number = [int(x) if x.isdigit() else 0 for x in number] 
        number_pages.extend(number) 
        
    max_number = max(number_pages) 
    list_numbers = [i for i in range(2, max_number+1)] 
    
    return list_numbers 


@dag(default_args=default_args,
     schedule_interval='30 12 * * *',
     start_date=datetime.datetime(2023, 1, 1),
     catchup=False, 
     )
def comon_graid_data(): 
    @task
    def get_tasks(query: str, engine): 
        """
        Функция получает список различных номеров ID из БД
        :param query: запрос к БД
        :return: список номеров ID
        """
        number_tasks_db = pd.read_sql(query, engine) 
        
        return list(number_tasks_db['id']) 

    @task
    def get_data_comon_web(url: str, 
                            user_login: str, 
                            user_password: str, 
                            headers: dict, 
                            regex_table: str, 
                            tags_head: str, 
                            tags_body: str, 
                            head: list, 
                            body: list, 
                            id_employees_db: list, 
                            telegram_token: str, 
                            id_chat: str) -> pd.DataFrame: 
        
        # Создаем сессию 

        session = requests.Session() 
        session.headers.update(headers) 
        
        # Осуществляем вход с помощью метода POST с указанием необходимых данных 
        # Авторизация в Битрикс24 

        post_request = session.post(url, {
                                        'AUTH_FORM': 'Y', 
                                        'TYPE': 'AUTH', 
                                        'USER_LOGIN': user_login, 
                                        'USER_PASSWORD': user_password 
                                    }) 

        if post_request.status_code == 200: 
            
            retry = Retry(connect=3, backoff_factor=0.5) 
            adapter = requests.adapters.HTTPAdapter(max_retries=retry) 
            session.mount('https://', adapter) 
            
            # Получим содержимое страницы: 

            get_request = session.get(url, timeout=50) 
            
            # Считываем блоки таблицы
            
            table = re.findall(regex_table, get_request.text, flags=re.DOTALL) 
            head_string = read_blok(tags_head, table[0]) 
            body_string = read_blok(tags_body, table[0]) 
            
            # Считываем значения и записываем в списки:  

            head_value = read_rows_head(head_string, head)[2:] 
            body_value = read_rows_body(body_string, body) 
        
            # Считываем блоки содержания таблиц с других страниц и добавляем к начальному блоку: 

            numbers_list = number_page(get_request.text) 
            numbers_list.sort(reverse=True) 
            tags_body = 'tbody' 

            for num in numbers_list: 
                try: 
                    get_req = session.get(f'https://comon.company/bitrix/admin/highloadblock_rows_list.php?PAGEN_1={num}&SIZEN_1=20&ENTITY_ID=8&lang=ru', timeout=50) 
                    table = re.findall(regex_table, get_req.text, flags=re.DOTALL) 
                    bodys = list() 

                    # Считываем блоки таблицы

                    body_string = read_blok(tags_body, table[0]) 
                    body_values = read_rows_body(body_string, bodys) 
                    body_value.extend(body_values) 

                    time.sleep(1) 
                except Exception as err: 
                    error_to_telegram(token=telegram_token, 
                                      chat_id=id_chat, 
                                      e=str(err).replace('<','|').replace('>','|').replace('\\','|').replace('{',' ').replace('}',' '), 
                                      name_dag='comon_graid_employees') 
                    continue 
                    
            # Считываем значения и записываем в таблицу: 

            grade_employees = pd.DataFrame(body_value, columns=head_value) 
            
            # Добавим столбцы из очищенных столбцов: 

            grade_employees['ID'] = grade_employees['ID'].astype(int) 
            grade_employees['ID_Сотрудника'] = grade_employees['Сотрудник'].apply(lambda x: id_tags(x)).astype(int) 
            grade_employees['email'] = grade_employees['Сотрудник'].apply(lambda x: x.split(']')[1].split(')')[0].strip('(').strip()) 
            grade_employees['Сотрудник'] = grade_employees['Сотрудник'].apply(lambda x: x.split(']')[1].split(')')[1].strip('(').strip()) 
            grade_employees['Баллы (Карма)'] = grade_employees['Баллы (Карма)'].replace({'&nbsp;': None}) 
            grade_employees['Дата перевода'] = pd.to_datetime(grade_employees['Дата перевода'], format='%d.%m.%Y') 
            
            # Переименуем столбцы 

            grade_employees = grade_employees.rename(columns={
                                                    'ID': 'id', 
                                                    'Сотрудник': 'employees', 
                                                    'Дата перевода': 'date_appointment', 
                                                    'Актив': 'employee_division', 
                                                    'Группа': 'division_group', 
                                                    'Грейд': 'grade', 
                                                    'Баллы (Карма)': 'points_karma', 
                                                    'ID_Сотрудника': 'id_employees' 
                                                    }) 
            
            # Отберем необходимые столбцы

            columns = ['id', 'employees', 'date_appointment', 
                       'employee_division', 'division_group', 
                       'grade', 'points_karma', 
                       'id_employees', 'email'] 
            
            grade_employees = grade_employees[columns] 
            
            # Определим формат данных: 

            grade_employees = grade_employees.astype({'id': int, 
                                            'employees': str, 
                                            'date_appointment': 'datetime64[ns]', 
                                            'employee_division': str, 
                                            'division_group': str, 
                                            'grade': str, 
                                            'points_karma': str, 
                                            'id_employees': int, 
                                            'email': str}) 
            
        else: 
            error_to_telegram(token=telegram_token, 
                              chat_id=id_chat, 
                              e=str(post_request.status_code) + ' ' 
                                  + str(post_request.text).replace('{', ' ').replace('}', ' '), 
                              name_dag='comon_graid_employees') 
        
        return grade_employees[~grade_employees['id'].isin(id_employees_db)].reset_index(drop=True) 
    
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

    # Загрузка грейдов сотрудников из comon в БД
    
    comon_tasks_db = get_tasks(query=ID_EMPLOYEES, engine=engine) 
    
    df = get_data_comon_web(url=URL, 
                            user_login=USER_LOGIN, 
                            user_password=USER_PASSWORD, 
                            headers=HEADERS, 
                            regex_table=REGEX_TABLE, 
                            tags_head=TAGS_HEAD, 
                            tags_body=TAGS_BODY, 
                            head=HEAD, 
                            body=BODY, 
                            id_employees_db=comon_tasks_db, 
                            telegram_token=TELEGRAM_TOKEN, 
                            id_chat=CHAT_ID) 
    
    append_data_to_db(data=df, table='grade_employees', engine=engine) 

comon_graid_data = comon_graid_data() 
