import psycopg2 as pg
from datetime import datetime as dt
from datetime import timedelta as td
import requests
import logging
import json
import sys
import os
import re
import io

class tgLog(logging.Handler):
    
    '''
            *******
            ! DOC !
            *******
            
            Создание кастомного Handler для отправки логов в группу через телеграм бота
            
            1. Наследие через __super__ класса хэндлеров
            2. создание сэссии через requests
            3. Замена базовой функции EMIT на кастомную которая отправляет сообщения через TG API
            
    '''
    
    def __init__(s, chat, key):
        
        super().__init__()
        
        s._chat = chat
        s._key = key
        s._session = requests.Session()
        
        
    def emit(s, message):
        
        _text = s.format(message)
        s._session.post(f'https://api.telegram.org/bot{s._key}/sendMessage?chat_id={s._chat}&text={_text}')

class sf:
    
    __slots__ = '_user', '_psw', '_client_key', '_client_secret', '_auth_url', '_atoken','_aurl', '_auth_url', '_limits_url', '_query', '_qurl', '_res', '_cols', '_table', '_info_logger', '_fail_logger', '_tg_logger', '_mapping'
    
    
    def __init__(s, user = None, psw = None, client_key = None , client_secret = None, mapping = None):

        s._info_logger, s._fail_logger, s._tg_logger = s.get_loggers()

        s._mapping = mapping
        s._user = user
        s._psw = psw
        s._client_key = client_key
        s._client_secret = client_secret
        s._auth_url = 'https://login.salesforce.com/services/oauth2/token'
        s._aurl, s._atoken = s.connect()

    def get_loggers(s):

        '''
            Функция инициализации логгеров.
            Вызывается в методе INIT.

            Использует кастомный класс для отправки сообщений в ТГ канал.
        '''

        info_logger = logging.getLogger('SF_INFO_LOGGER')
        fail_logger = logging.getLogger("SF_FAIL_LOGGER")
        tg_logger = logging.getLogger('SF_TG_LOGGER')

        info_logger.setLevel(logging.INFO)
        fail_logger.setLevel(logging.ERROR)
        tg_logger.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - func_name:  %(funcName)s - %(message)s')


        # из кастомного класса tgLog создаем Handler с измененной функцией EMIT
        handler = logging.FileHandler('_sf_logger.txt', 'a')
        tg_handler = tgLog(
            chat='-725709893',
            key='5250617487:AAF9j4edtPjY-_d92In0z6FiGijgSrFjAUo')

        tg_handler.setFormatter(formatter)
        handler.setFormatter(formatter)

        info_logger.addHandler(handler)
        fail_logger.addHandler(handler)
        tg_logger.addHandler(tg_handler)

        return info_logger, fail_logger, tg_logger
                    
    def connect(s):

        '''
            Функция подключения к API а получения токена сессии и URL сессии.
            Возвращает вышеуказанные параметры.
        '''
        
        try:

            s._info_logger.info('Trying to set SF API connection and retrieve session Token')
        
            res = requests.post(s._auth_url , data = {
                                            'client_id': s._client_key,
                                            'client_secret': s._client_secret,
                                            'grant_type': 'password',
                                            'username': s._user,
                                            'password': s._psw
                                            })
            
            _atoken = json.loads(res.text)['access_token']
            _aurl = json.loads(res.text)['instance_url']

            s._info_logger.info(f'SF API connection successful, Token {_atoken} recieved, session URL {_aurl} recieved - OK')
        
            return _aurl, _atoken
        
        except Exception:
            
            s._fail_logger.error(f'Failed to set SF API connection {sys.exc_info()} - FAIL')
        
    def lim(s):

        '''
            Функция проверки лимитов запросов
        '''
        
        _auth_url , _auth_token = s.connect()
        
        _limits_url = _auth_url + '/services/data/v53.0/limits/'

        try:
        
            response = requests.get(url = _limits_url , headers = {'Authorization':'Bearer '+ _auth_token})
            
            return json.loads(response.text)

        except Exception:

            s._fail_logger.error(f'Failed to retrieve limits {sys.exc_info()} - FAIL')
    
    def parse_response(s, response, cols):

        '''
            Функция парсинга ответа.
            Возвращает StringIO handle
        
        '''

        try:

            s._info_logger.info('Starting response parse')
        
            _data_loader = io.StringIO()

            for x in response:
                
                _data_loader.write('^'.join([str(x.get(_col,'')).replace('None','').replace('\n','').replace('\r','') for _col in cols]) + '\n')
                
            _data_loader.seek(0)

            s._info_logger.info('Parse successful')
                
            return _data_loader

        except Exception:

            s._fail_logger.error(f'Failed to parse response {sys.exc_info()} - FAIL')
    
    def query(s, query):
        
        '''
            Отправка запроса SOQL.
            
            Функция возвращает тело ответа в виде JSON и заголовок ответа (HEADER).
            
            Если результат возвращается на нескольких страницах то ответ склеивается из этих страниц.
            В этом случае результат склеивается в цикле.
            
            Функция отдает StringIO (через функцию parse_response)
        '''
        
        s._qurl = s._aurl + '/services/data/v53.0/query/'
        s._query = query
        s._res = []
        
        try:
            s._info_logger.info('Starting column names extraction')

            # получение списка запрашиваемых полей
            s._cols = re.search(r'[Ss][Ee][Ll][Ee][Cc][Tt]\s*(?P<cols>([ ,_A-Za-z0-9\s\.]+))[Ff][Rr][Oo][Mm]', s._query).group('cols').replace('\n','').replace(' ','').split(',')

            s._table = re.search(r'[Ff][Rr][Oo][Mm]\s*(?P<tab>([_A-Za-z0-9\.]+))', s._query).group('tab') 

            s._info_logger.info(f'Columns extracted from query: {s._cols}')

        except Exception:
            s._fail_logger.error(f'Failed to extract columns from query {sys.exc_info()} - FAIL')

        try:

            s._info_logger.info('Making API request (query)')
            #базовый запрос
            response = requests.get(url= s._qurl , headers = {'Authorization':'Bearer '+ s._atoken}, params = {'q':s._query})

            s._info_logger.info(f'Query returned status {response.status_code}')

        except Exception:
            s._fail_logger.error(f'Failed to make request {sys.exc_info()} - FAIL')
             
        #накопление записей ответа в DICT
        if response.status_code != 200:
            s._fail_logger.error(f'Failed to form query. Check query, error: {response.text} - FAIL')

        else:
            try:
                s._info_logger.info('Starting response processing')

                for _rec in json.loads(response.text)['records']:
                    s._res.append(_rec)

                #проверка наличия последующих страниц ответа (в случае формирования мультристраничного запроса-ответа)
                if bool(json.loads(response.text).get('nextRecordsUrl')):

                    s._info_logger.info('Recieved multipage response, processing')
                    #получение адреса следующей страницы
                    _nextPage = json.loads(response.text).get('nextRecordsUrl')

                    #запуск цикла получения ответов по страница
                    while bool(_nextPage):

                        response = requests.get(url= s._aurl +  _nextPage, headers = {'Authorization':'Bearer '+ s._atoken}, params = {'q':s._query})

                        #накопление записей с последующих страниц в DICT с ответами
                        for rec in json.loads(response.text)['records']:
                            s._res.append(rec)

                        s._info_logger.info('Recieving next page')
                        _nextPage = json.loads(response.text).get('nextRecordsUrl')

                    return s._table, s._cols, s.parse_response(s._res, s._cols)

                else:

                    return s._table, s._cols, s.parse_response(s._res, s._cols)
            
            except Exception:
                s._fail_logger.error(f'Failed to process query / response {sys.exc_info()} - FAIL')

    def save_file(s, _file, _tab):
    
        '''
            Сохранения временного файла с данными ответа API.
            Возвращает ИМЯ файла.
        '''
        
        _now = dt.strftime(dt.now(), '%Y-%m-%d %H-%M-%S')
        _file_name = f'query {_tab} at {_now}.txt'

        try:
            s._info_logger.info('Opening file for result write')

            with open(_file_name, 'w', encoding='utf-8') as f:
                s._info_logger.info(f'Starting write operation to file "{_file_name}"')

                f.write(_file.getvalue())

            s._info_logger.info('File saved')

            return _file_name

        except Exception:
            s._fail_logger.error(f'Failed to open file for write or something went wrong {sys.exc_info()} - FAIL')

    def set_con(s):

        '''
            Функция открытия соединения
        '''

        try:

            s._info_logger.info('Trying to set DB connection')

            connection = pg.connect(host='192.168.166.13', port=5432, user='master', password='e8k.z85Mn4jGz', dbname='pole_pg')
            cursor = connection.cursor()

            s._info_logger.info('Connection successful')

            return connection, cursor

        except Exception:
            s._fail_logger.error(f'Failed to set connection {sys.exc_info()}')

    def close_con(s, connection, cursor):

        '''
            Функция закрытия соединения с базой и коммита
        '''

        try:

            s._info_logger.info('Trying to close DB connection')

            connection.commit()
            cursor.close()
            connection.close()

            s._info_logger.info('Connection commit successful')

        except Exception:
            s._fail_logger.error(f'Failed commit connection {sys.exc_info()}')

    def put_table(s, _file_name, _table, cur, truncate=False):

        '''
            Функция вставки данных из файла в таблицу назначения (см переменную s._mapping)
        '''
        
        if truncate:
            try:
                cur.execute(f'truncate table {s._mapping.get(_table)}')
                s._info_logger.info(f'Table {s._mapping.get(_table)} cleared successfully - OK')

            except Exception:
                s._fail_logger.error(f'Failed to clear table {_table} with error {sys.exc_info()}')

        try:
            s._info_logger.info(f'Trying to open file {_file_name}')

            with open(_file_name, 'r', encoding='utf-8') as file:

                s._info_logger.info(f'Trying insert into {_table}')
                cur.copy_from(file, sep = '^', table = s._mapping.get(_table))

                s._info_logger.info(f'Insert successful {_table} - OK')
                s._tg_logger.info(f'Table {_table} updated successfully - OK')

        except Exception:
            s._fail_logger.error(f'Failed to open file {_file_name} with error: {sys.exc_info()} - FAIL')

    def clean(s, file_name):

        try:
            s._info_logger.info(f'Trying to remove file {file_name}')
            os.remove(file_name)
            s._info_logger.info(f'File {file_name} removed successfully - OK')

        except Exception:
            s._fail_logger.error(f'Failed to remove {file_name} with error {sys.exc_info()} - FAIL')

    def call(s, _query):

        '''
            Финальная функция вызова алгоритма:
                1. отправка запроса
                2. сохранение временного файла и получение имени
                3. открытие соединения с БД
                4. вставка данных в таблицу из временного файла
                5. закрытие соединения
                6. удаление временного файла
        '''

        _table, _cols, _response = s.query(_query)
        _file_name = s.save_file(_response, _table)
        con, cur = s.set_con()
        s.put_table(_file_name=_file_name, _table=_table, cur=cur, truncate=True)
        s.close_con(con,cur)
        s.clean(_file_name)

        logging.shutdown()
        
    def update():
        pass

