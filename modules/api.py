import requests
from json import loads
from typing import Literal
from urllib.parse import urljoin
from logging import info, error

BASE_URL = 'https://newsapi.org'
SEARCHIN_OPTIONS = Literal['title','description','content']
CATEGORY_OPTIONS = Literal["business","entertainment","general","health","science","sports","technology"]
LANGUAGE_OPTIONS = Literal["ar","de","en","es","fr","he","it","nl","no","pt","ru","sv","ud","zh"]
COUNTRY_OPTIONS = Literal['ae', 'ca', 'ch', 'cn', 'co', 'cu', 'cz', 'de', 'eg', 'fr', 'gb', 'gr', 'hk', 'hu', 'id', 'ie', 'il', 'in', 'it', 'jp', 'mx', 'my', 'ng', 'nl', 'no', 'nz', 'ph', 'pl', 'pt', 'ro', 'rs', 'ru', 'sa', 'se', 'sg', 'si', 'sk', 'th', 'za']
SORTBY_OPTIONS = Literal['relevancy', 'popularity', 'publishedAt']

def get_sources(apiKey:str,category:CATEGORY_OPTIONS=None,language:LANGUAGE_OPTIONS=None,country:COUNTRY_OPTIONS=None):
    '''
    '''
    params = {
        'apiKey':apiKey,
        'category':category,
        'language':language,
        'country':country
    }
    try:
        url = urljoin(BASE_URL,'/v2/top-headlines/sources')
        response = requests.get(url=url,params=params)
        response.raise_for_status()
        return response
    except Exception as ex:
        raise ex

def get_everything(apiKey:str,
                   q:str=None,
                   searchIn:SEARCHIN_OPTIONS=None,
                   sources:list[str]=None,
                   domains:list[str]=None,
                   excludeDomains:list[str]=None,
                   _from:str=None,
                   _to:str=None,
                   language:LANGUAGE_OPTIONS=None,
                   sortBy:SORTBY_OPTIONS=None,
                   pageSize:int=None,
                   page:int=None):
    '''
    '''
    if searchIn is not None: # Verificando a variável searchIn
        info("Verificando a variável searchIn.")
        if searchIn not in ['title','description','content']:
            raise ValueError(f"O valor inserido na variável 'searchIn não é válido.")

    if sources is not None: # Verificando a variável sources
        info("Verificando a(s) fonte(s) passada(s)")
        _verify_sources(apiKey, sources)

    if domains is not None: # Verificando a variável domains
        info("Verificando a variável domains")
        _verify_domains(domains=domains)

    if excludeDomains is not None:
        info("Verificando a variável excludedDomains")
        _verify_excludedDomains(excludedDomains=excludeDomains)
    
    ## Construção dos Parâmetros
    info("Construindo os parametros.")
    params = {
        'apiKey':apiKey,
        'q':q,
        'searchIn':searchIn,
        'sources':sources,
        'domains':domains,
        'excludeDomains':excludeDomains,
        'from':_from,
        'to':_to,
        'language':language,
        'sortBy':sortBy,
        'pageSize':pageSize,
        'page': page
    }

    try:
        info("Criando a url para requisião")
        url = urljoin(BASE_URL,'/v2/everything')
        info("Efetuando a requisição")
        response = requests.get(url=url,params=params)
        response.raise_for_status()
        return response.json()
    except Exception as ex:
        error(f"Erro ao executar a requisição: {ex}")
        raise ex


def _verify_sources(apiKey, sources):
    '''
    '''
    if isinstance(sources,list): # Vericar se é uma lista
        if len(sources) > 20:
            raise ValueError("Só pode escolher até 20 fontes.")
        response_get_sources = get_sources(apiKey=apiKey)
        list_of_sources = [dictionary.get('id') for dictionary in loads(response_get_sources.content).get('sources')]
        for source in sources:
            if not isinstance(source,str): # Verificar se é uma string
                raise ValueError("Um dos itens da lista não é uma string.")
            if source not in list_of_sources:
                raise ValueError(f"A fonte '{source}' está na lista de fontes utilizáveis.")
    else:
        raise ValueError("A variável 'sources' deve ser uma lista")
    
def _verify_domains(domains:list[str]):
    '''
    '''
    if isinstance(domains,list):
        for domain in domains:
            if not isinstance(domain,str): # Verificar se é uma string
                raise ValueError("Um dos itens da lista não é uma string.")
    else:
        raise ValueError("A variável 'domains' deve ser uma lista")
    
def _verify_excludedDomains(excludedDomains:list[str]):
    '''
    '''
    if isinstance(excludedDomains,list):
        for domain in excludedDomains:
            if not isinstance(domain,str): # Verificar se é uma string
                raise ValueError("Um dos itens da lista não é uma string.")
    else:
        raise ValueError("A variável 'excludedDomains' deve ser uma lista")