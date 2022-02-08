
import requests
import json
import sys
import os
import re
import io

class sf:
    
    __slots__ = '_user','_psw','_client_key','_client_secret','_auth_url','_atoken','_aurl','_auth_url','_limits_url','_query','_qurl','_res','_cols'
    
    
    def __init__(s, user = None, psw = None, client_key = None , client_secret = None):
    
        s._user = user
        s._psw = psw
        s._client_key = client_key
        s._client_secret = client_secret
        s._auth_url = 'https://login.salesforce.com/services/oauth2/token'
        s._aurl, s._atoken = s.connect()
                    
    def connect(s):
        
        try:
        
            res = requests.post(s._auth_url , data = {
                                            'client_id': s._client_key,
                                            'client_secret': s._client_secret,
                                            'grant_type': 'password',
                                            'username': s._user,
                                            'password': s._psw
                                            })
            
            _atoken = json.loads(res.text)['access_token']
            _aurl = json.loads(res.text)['instance_url']
        
            return _aurl, _atoken
        
        except Exception:
            
            return sys.exc_info()
        
    def lim(s):
        
        _auth_url , _auth_token = s.connect()
        
        _limits_url = _auth_url + '/services/data/v53.0/limits/'
        
        response = requests.get(url = _limits_url , headers = {'Authorization':'Bearer '+ _auth_token})
        
        return json.loads(response.text)
    
    def parse_response(s, response, cols):
        
        _data_loader = io.StringIO()

        for x in response:
            
            _data_loader.write('^'.join([str(x.get(_col,'')).replace('None','') for _col in cols]) + '\n')
            
        _data_loader.seek(0)
            
        return _data_loader
    
    def qu(s, query):
        
        '''
            Отправка запроса SOQL.
            
            Функция возвращает тело ответа в виде JSON и заголовок ответа (HEADER).
            
            Если результат возвращается на нескольких страницах то ответ склеивается из этих страниц.
            В таком случае ответы получаются в цикле
        '''
        
        s._qurl = s._aurl + '/services/data/v53.0/query/'
        s._query = query
        s._res = []
        
        # получение списка запрашиваемых полей
        s._cols = re.search(r'[Ss][Ee][Ll][Ee][Cc][Tt]\s*(?P<cols>([ ,_A-Za-z0-9\s]+))from', query).group('cols').replace('\n','').replace(' ','').split(',')
        
        #базовый запрос
        response = requests.get(url= s._qurl , headers = {'Authorization':'Bearer '+ s._atoken}, params = {'q':s._query})
            
        #накопление записей ответа в DICT
        if response.status_code != 200:
            print(response.status_code,'\n',response.text)
            sys.exit()
        
        for _rec in json.loads(response.text)['records']:
            s._res.append(_rec)

        #проверка наличия последующих страниц ответа (в случае формирования мультристраничного запроса-ответа)
        if bool(json.loads(response.text).get('nextRecordsUrl')):

            #получение адреса следующей страницы
            _nextPage = json.loads(response.text).get('nextRecordsUrl')

            #запуск цикла получения ответов по страница
            while bool(_nextPage):

                response = requests.get(url= s._aurl +  _nextPage, headers = {'Authorization':'Bearer '+ s._atoken}, params = {'q':s._query})

                #накопление записей с последующих страниц в DICT с ответами
                for rec in json.loads(response.text)['records']:
                    s._res.append(rec)

                _nextPage = json.loads(response.text).get('nextRecordsUrl')

            return s._cols, s.parse_response(s._res, s._cols)

        else:

            return s._cols, s.parse_response(s._res, s._cols)
