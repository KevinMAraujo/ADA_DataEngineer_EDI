# Trabalhando com Webhooks e orquestração ETL
## Contexto
O grupo trabalha no time de engenharia de dados na HealthGen, uma empresa especializada em genômica e pesquisa de medicina personalizada. A genômica é o estudo do conjunto completo de genes de um organismo, desempenha um papel fundamental na medicina personalizada e na pesquisa biomédica. Permite a análise do DNA para identificar variantes genéticas e mutações associadas a doenças e facilita a personalização de tratamentos com base nas características genéticas individuais dos pacientes.

A empresa precisa se manter atualizada sobre os avanços mais recentes na genômica, identificar oportunidades para pesquisa e desenvolvimento de tratamentos personalizados e acompanhar as tendências em genômica que podem influenciar estratégias de pesquisa e desenvolvimento. Pensando nisso, o time de dados apresentou uma proposta de desenvolvimento de um sistema que coleta, analisa e apresenta as últimas notícias relacionadas à genômica e à medicina personalizada, e também estuda o avanço do campo nos últimos anos.

O time de engenharia de dados tem como objetivo desenvolver e garantir um pipeline de dados confiável e estável. As principais atividades são:

    1. Consumo de dados com a News API
    2. Definir Critérios de Relevância
    3. Cargas em Batches

## Equipe
- [Rafael Queiroz Santos](https://github.com/RafaelQSantos-RQS)
- [Caio Marques Ribeiro](https://github.com/caiombribeiro)
- [Kevin Araújo](https://github.com/KevinMAraujo)

## Visão geral
Este projeto visa o desenvolvimento de uma solução de engenharia de dados, com a utilização de um webhook para integrar a News API. O processo envolverá cargas em batches, realizadas a cada hora, garantindo consistência na extração dos dados. Ademais, uma carga diária será executada para o tratamento dos dados extraídos ao longo do dia, inserindo-os na base de dados correspondente.

## Tecnologias usadas
- Flask
- Python
- Pandas

## Detalhes técnicos
### [Módulo Utils](modules/utils.py)
A biblioteca de utilidades fornece uma série de funções auxiliares úteis para simplificar tarefas comuns em projetos de processamento de dados. As funcionalidades incluem:

- **Configuração do Sistema de Arquivos**: A função `prepare_data_filesystem` verifica e cria diretórios necessários para armazenar dados em diferentes estágios de processamento: "data/raw", "data/bronze", "data/silver" e "data/gold".
- **Recuperação de Variáveis de Ambiente**: A função `get_env_var` facilita a recuperação de variáveis de ambiente, permitindo especificar um grupo e uma variável específica dentro desse grupo. Ela é útil para acessar chaves de API ou outras configurações.
- **Geração de Intervalos de Tempo**: As funções `range_de_horarios` e `range_de_hora_passado` geram intervalos de tempo, como o intervalo de horas do dia atual ou o intervalo de horas do dia anterior.
  
Essas funções foram projetadas para simplificar tarefas rotineiras e fornecer uma maneira conveniente de lidar com operações comuns em projetos de processamento de dados.


### [Webhook](webhook.py)
O arquivo `webhook.py` é um aplicativo Flask que implementa um webhook para acessar a API de notícias. Quando enviado um POST para a rota `/NewsApi/get_everything`, o aplicativo verifica se os dados recebidos contêm as chaves necessárias conforme definido na lista `chaves_esperadas`. Caso contrário, retorna um erro indicando chaves inválidas. As chaves 'from' e 'to' são renomeadas para evitar conflito com palavras reservadas. O aplicativo então tenta obter a chave da API e faz uma solicitação à API de notícias com os parâmetros fornecidos. Se a solicitação for bem-sucedida, retorna os dados da resposta em formato JSON; caso contrário, retorna um erro com detalhes sobre a falha na requisição. Quando executado diretamente, o aplicativo inicia o servidor Flask para lidar com as solicitações. Certifique-se de configurar a variável de ambiente `api_key` com a chave de API necessária para acessar o serviço de notícias.

### [Etapa Landing](modules/etl.py)
A função `landing_step` é responsável por extrair informações de texto usando uma API de extração de texto e salvar os resultados em arquivos JSON. Ela recebe os seguintes parâmetros:

- `query (str)`: O texto a ser extraído.
- `language (str)`: O idioma do texto.
- `inicio (str)`: Data inicial para a busca (formato YYYY-MM-DDTHH:MM:SS).
- `fim (str, opcional)`: Data final para a busca (formato YYYY-MM-DDTHH:MM:SS).

A função utiliza a função `_extract` para fazer a extração dos dados. Primeiro, verifica se há mais de uma página de resultados e salva cada página de resultados em arquivos JSON individualmente, se necessário. Caso contrário, salva todos os resultados em um único arquivo JSON. Em caso de erro durante a execução da função, uma exceção é lançada.

### [Etapa Bronze](modules/etl.py)
A função `bronze_step` é responsável por extrair dados brutos, transformá-los e carregá-los em um arquivo parquet. Ela executa as seguintes etapas:

1. **Extração**: Extrai todos os arquivos JSON do diretório "data/raw" e carrega cada arquivo em um DataFrame do pandas.
2. **Transformação**: Se um arquivo parquet já existir em "data/processed/bronze.parquet", ele é carregado e anexado à lista de DataFrames. Em seguida, todos os DataFrames são concatenados em um único DataFrame. Uma coluna 'load_date' é adicionada, que registra a data e hora atuais. Valores duplicados são removidos para garantir que o arquivo bronze contenha apenas notícias diferentes.
3. **Carga**: O DataFrame resultante é salvo (ou sobrescrito se já existir) em "data/processed/bronze.parquet".

### [Etapa Silver](modules/etl.py)
A função `silver_step` é responsável por realizar o processamento e limpeza dos dados de notícias coletados na etapa anterior (`bronze_step`). Ela executa as seguintes etapas:

1. **Carregamento do Arquivo Bronze**: Carrega o arquivo parquet mais recente do diretório de bronze.
2. **Remoção de Notícias**: Remove as notícias com título "[Removed]", que possivelmente foram deletadas.
3. **Preenchimento de Valores Ausentes**: Preenche valores ausentes na coluna "author" com "Não Informado".
4. **Tratamento da Coluna "publishedAt"**: Converte para datetime, extrai ano, mês e dia, e remove a informação de hora.
5. **Redistribuição dos Dados da Coluna "source"**: Divide os dados em "source_id" e "source_name".
6. **Remoção da Coluna "source"**: Remove a coluna "source".
7. **Verificação e Concatenação de Arquivos Silver**: Verifica se já existe um arquivo silver. Se sim, carrega o arquivo anterior, concatena com o novo DataFrame e remove duplicados.
8. **Salvamento do DataFrame Final**: Salva o DataFrame final como um arquivo parquet no diretório de silver.

### [Etapa Gold](modules/etl.py)
A função `gold_step` é responsável por gerar as métricas finais a partir dos dados processados na etapa silver. Ela executa as seguintes etapas:

1. **Carregamento do Arquivo Silver**: Carrega o arquivo parquet mais recente do diretório de silver.
2. **Criação das Métricas**:
    - **4.1 - Quantidade de notícias por ano, mês e dia de publicação**:
        - 4.1.1 - Quantidade de notícias por ano (agrupado por ano).
        - 4.1.2 - Quantidade de notícias por mês (agrupado por mês).
        - 4.1.3 - Quantidade de notícias por dia (agrupado por dia).
        - 4.1.4 - Quantidade de notícias por ano, mês e dia (agrupado por ano, mês e dia).
    - **4.2 - Quantidade de notícias por fonte e autor**:
        - 4.2.1 - Quantidade de notícias por fonte (agrupado por fonte).
        - 4.2.2 - Quantidade de notícias por autor (agrupado por autor).
        - 4.2.3 - Quantidade de notícias por fonte e autor (agrupado por fonte e autor).
    - **4.3 - Quantidade de aparições de 3 palavras chaves por ano, mês e dia de publicação** (ainda não implementado).
3. **Salvamento dos Resultados**: Cada métrica é salva em um arquivo parquet separado no diretório de gold.
4. **Criação das Dimensões** `author` e `source`:
    - Cria uma dimensão para autor e outra para fonte, e as salva em arquivos parquet separados.

## Como Executar

Para executar este projeto, siga os passos abaixo:

1. **Crie um Ambiente Virtual**: Recomenda-se criar um ambiente virtual para isolar as dependências deste projeto de outras instalações Python. Você pode criar um ambiente virtual usando a ferramenta `venv` com o seguinte comando:

    ```bash
    python -m venv myenv
    ```

2. **Instale as Dependências**: Após criar o ambiente virtual, ative-o e instale as dependências listadas no arquivo `requirements.txt` usando o `pip`. Você pode instalar as dependências com o seguinte comando:

    ```bash
    source myenv/bin/activate  # Linux/Mac
    myenv\Scripts\activate.bat  # Windows
    pip install -r requirements.txt
    ```

3. **Execute o Arquivo `webhook.py`**: O arquivo `webhook.py` contém o servidor Flask responsável por lidar com as solicitações da API. Você pode executá-lo com o seguinte comando:

    ```bash
    python webhook.py
    ```

   Certifique-se de que o servidor está em execução antes de prosseguir para o próximo passo.

4. **Execute as Células do Notebook `main.ipynb`**: O notebook `main.ipynb` contém o código para interagir com a API e realizar as etapas do processamento de dados. Abra o notebook em um ambiente Jupyter Notebook ou JupyterLab e execute as células conforme descrito nas instruções fornecidas no próprio notebook.

Certifique-se de seguir esses passos na ordem especificada para garantir uma execução correta do projeto.

Se houver algum problema durante a execução, verifique se todas as dependências foram instaladas corretamente e se o servidor Flask está em execução conforme esperado.