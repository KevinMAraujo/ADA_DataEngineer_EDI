import os
import json
import requests
import pandas as pd
from datetime import datetime
from logging import info, error, basicConfig, INFO


url = "http://127.0.0.1:5000/NewsApi/get_everything"
basicConfig(level=INFO, format=f'[%(asctime)s] %(message)s',datefmt='%d/%m/%Y %H:%M:%S')

def landing_step(query:str,languague:str,inicio:str,fim:str=None):
    '''
    '''
    try:
        info("FIST STEP (FOR EACH 1 HOUR)")
        info("Extracting...")
        response = _extract(query=query,languague=languague,_from=inicio,_to=fim)

        info("Verificando se é necessárioa mais de uma página")
        total_results = response.get('totalResults')
        num_of_pages = (total_results // 100) + 1

        if num_of_pages == 1:
        
            info("Mais de uma página não é necessaŕio! Só um json será salvo!")
            path = f'data/raw/{inicio.split("T")[0]}.json'
            save_json_response(inicio, fim, response,path=path)
        
        elif num_of_pages > 1:
            
            info(f"Temos {num_of_pages} páginas de artigos! Serão salvos jsons individualmente!")
            
            info("Salvando a página 1!")
            path = f'data/raw/{inicio.split("T")[0]} (Page 1).json'
            save_json_response(inicio, fim, response,path=path)
            info("Página 1 salva com sucesso!!")

            for page in range(2,num_of_pages+1):
                response = _extract(query=query,languague=languague,_from=inicio,_to=fim,page=page)
                
                info(f"Salvando a página {page}!!")
                path = f'data/raw/{inicio.split("T")[0]} (Page {page}).json'
                save_json_response(inicio, fim, response,path=path)
                info(f"Página {page} salva com sucesso!!")

    except Exception as e:
        error(f"Erro ao executar a execução first: {e}")

def bronze_step():
    '''
    A função `bronze_step` é responsável por extrair dados brutos, transformá-los e carregá-los em um arquivo parquet.

    Etapas:
    1. Extração: Extrai todos os arquivos JSON do diretório "data/raw" e carrega cada arquivo em um DataFrame do pandas.
    2. Transformação: Se um arquivo parquet já existir em "data/processed/bronze.parquet", ele é carregado e anexado à lista de DataFrames. Em seguida, todos os DataFrames são concatenados em um único DataFrame. Uma coluna 'load_date' é adicionada, que registra a data e hora atuais.
    3. Carga: O DataFrame resultante é salvo (ou sobrescrito se já existir) em "data/processed/bronze.parquet".

    Retorna:
    None. O resultado é salvo em um arquivo parquet.
    '''

    ## Extract
    data_raw_path = "data/raw"
    info("Listando os jsons que estão no diretório data/raw")
    list_of_json_paths = [os.path.join(data_raw_path,json_file) for json_file in os.listdir(data_raw_path)]
    info("Verificando se o diretório data/aw está vazio")
    if len(list_of_json_paths) != 0:
        
        info("Criando uma lsita de objetos json")
        list_of_json_obj = [open(path) for path in list_of_json_paths]
        
        info("Transformando-os em dicionários Python")
        list_of_dictionary = [json.load(json_obj) for json_obj in list_of_json_obj]
        
        info("Separando somente a key articles")
        list_of_articles = [dictionary.get('articles') for dictionary in list_of_dictionary]
        
        info("Transformando em dataframes")
        list_of_dataframes = [pd.DataFrame(articles) for articles in list_of_articles]

        # Transform
        info("COncatenando os dataframes")
        df_concatened = pd.concat(list_of_dataframes)
        
        info("Inserindo a colunad 'load_date'")
        df_concatened['load_date'] = datetime.now().date()

        info("Verificando se já tem um arquivo bronze")
        bronze_parquet_path = "data/processed/bronze.parquet"
        if os.path.exists(bronze_parquet_path):
            df_parquet = pd.read_parquet(bronze_parquet_path)
            df_concatened = pd.concat([df_concatened,df_parquet])

        # Load
        df_concatened.to_parquet(bronze_parquet_path)


def _extract(query:str,languague:str,_from:str,_to:str,page:int=None,pageSize:int=100):
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
    
def save_json_response(inicio, fim, response, path:str):
    articles = response.get('articles')
    if len(articles) == 0:
        info(f"Não há novos artigos no periodo de {inicio} a {fim}")
    else:
        info("Saving response in data/raw")
        with open(path,'w') as file:
            file.write(json.dumps(response,indent=1))