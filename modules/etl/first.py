import requests
import pyspark.pandas as ps
from modules.utils import range_de_hora
from logging import info, error, basicConfig, INFO


url = "http://127.0.0.1:5000/NewsApi/get_everything"
basicConfig(level=INFO, format=f'[%(asctime)s] %(message)s',datefmt='%d/%m/%Y %H:%M:%S')

def etl(query:str,languague:str):
    '''
    '''
    try:
        info("FIST STEP (FOR EACH 1 HOUR)")
        info("Extracting...")
        inicio,fim = range_de_hora()
        response = _extract(query=query,languague=languague,_from=inicio,_to=fim)
        info("Transforming...")
        transformed_response = _transform(response=response)
        if transformed_response:
            info("Saving...")

    except Exception as e:
        error(f"Erro ao executar a execução first: {e}")

def _extract(query:str,languague:str,_from:str,_to:str,page:int=None):
    '''
    '''
    info("Construindo os dados a serem enviados na requisição")
    data = {
        "q":query,
        "language":languague,
        "from": _from,
        "to":_to,
        "page":page
        }
    
    try:
        info("Enviando a requisição")
        response = requests.post(url=url,json=data)

        response.raise_for_status()

        info("Requisição feita com sucesso!!")
        return response.json()
    except requests.RequestException as erro:
        error(f"Erro ao executar a requisição: {erro}")
        raise erro
    except Exception as e:
        error(f"Erro inesperado ao efetuar a requisção ao webhook: {e}")
        raise e
    
def _transform(response:dict):
    '''
    '''
    if len(response.get("articles")) == 0:
        range = range_de_hora()
        error(f"Não há artigos disponíveis entre {range[0]} e {range[1]}")
        return None
    
    df = ps.DataFrame(response.get("articles"))