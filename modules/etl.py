import os
import json
import requests
import pandas as pd
from datetime import datetime
from modules.utils import range_de_hora
from logging import info, error, basicConfig, INFO


url = "http://127.0.0.1:5000/NewsApi/get_everything"
basicConfig(level=INFO, format=f'[%(asctime)s] %(message)s',datefmt='%d/%m/%Y %H:%M:%S')

def bronze_step(query:str,languague:str):
    '''
    '''
    try:
        info("FIST STEP (FOR EACH 1 HOUR)")
        info("Extracting...")
        inicio,fim = range_de_hora()
        inicio,fim = "2024-04-01T00:00:35","2024-04-03T19:00:35"
        response = _extract(query=query,languague=languague,_from=inicio,_to=fim)
        articles = response.get('articles')
        
        if len(articles) == 0:
            info(f"Não há novos artigos no periodo de {inicio} a {fim}")
        else:
            info("Saving response in data/raw")
            with open(f'data/raw/response.json','w') as file:
                file.write(json.dumps(response,indent=1))

    except Exception as e:
        error(f"Erro ao executar a execução first: {e}")

def silver_step():
    '''
    '''
    ## Extract
    data_raw_path = "data/raw"
    list_of_json_paths = [os.path.join(data_raw_path,json_file) for json_file in os.listdir(data_raw_path)]
    if len(list_of_json_paths) != 0:
        list_of_json_obj = [open(path) for path in list_of_json_paths]
        list_of_dictionary = [json.load(json_obj) for json_obj in list_of_json_obj]
        list_of_articles = [dictionary.get('articles') for dictionary in list_of_dictionary]
        list_of_dataframes = [pd.DataFrame(articles) for articles in list_of_articles]

        # Transform
        df_concatened = pd.concat(list_of_dataframes)
        df_concatened['load_date'] = datetime.now()

        # Load
        df_concatened.to_parquet("data/processed/silver_step.parquet")

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
    