import os
from datetime import datetime, timedelta
from logging import info, error, basicConfig, INFO

basicConfig(level=INFO, format=f'[%(asctime)s] %(message)s',datefmt='%d/%m/%Y %H:%M:%S')

def prepare_data_filesystem():
    '''
    '''
    list_of_path = ['data/raw','data/processed']
    for path in list_of_path:
        try:
            info(f"Tentando criar o diretório {path}.")
            os.makedirs(path)
            info(f"Diretório {path} criado com sucesso!!")
        except FileExistsError as e:
            error(f"O diretório {path} já existe, nada será feito.")
        except Exception as e:
            error(f"Um erro inesperado ocorreu: {e}")
            raise e

def get_env_var(group: str, variable: str = None, default: str = None):
    """
    Retorna a variável de ambiente correspondente ao grupo e à variável especificada.
    
    Args
    ----
        group (str): O grupo de variáveis de ambiente.
        variable (str): A variável específica dentro do grupo (opcional).
        default (str): O valor padrão a ser retornado se a variável não estiver definida (opcional).

    Returns
    -------
        str: O valor da variável de ambiente ou o valor padrão, se especificado.
        
    Raises
    ------
        KeyError: Se o grupo de variáveis especificado não existir.
    """
    group_vars = {
        'api_key': os.environ.get("news_api")
    }
    
    if group not in group_vars:
        raise KeyError(f"O grupo de variáveis '{group}' não existe!")
    
    if variable:
        return group_vars[group].get(variable, default)
    else:
        return group_vars[group]
    
def range_de_horarios():
    '''
    '''
    hora_atual = datetime.now()

    inicio = hora_atual.replace(hour=0,minute=0,second=59)
    fim = hora_atual.replace(hour=23,minute=59,second=59)

    return (inicio,fim)