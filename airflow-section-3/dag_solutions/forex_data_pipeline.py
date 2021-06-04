from airflow import DAG
from airflow.providers.http.sensor.http import HttpSensor
from datetime import datetime, timedelta
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash. import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.email import EmailOperator
import csv
import requests
import json


#definição de dicionário de parâmetros que serão comum a todas as DAGS
default_args = {
    "owner": "aifrlow",  
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1, #quantidade de tentativas em caso de erro
    "retry_delay": timedelta(minutes=5) #tempo até o retry
}

# Para a Task 3 - criaçao da funçao download_rates()
# Download forex rates according to the currencies we want to watch
# described in the file forex_currencies.csv
def download_rates():
    BASE_URL = "https://gist.githubusercontent.com/marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b/raw/"
    ENDPOINTS = {
        'USD': 'api_forex_exchange_usd.json',
        'EUR': 'api_forex_exchange_eur.json'
    }
    with open('/opt/airflow/dags/files/forex_currencies.csv') as forex_currencies:
        reader = csv.DictReader(forex_currencies, delimiter=';')
        for idx, row in enumerate(reader):
            base = row['base']
            with_pairs = row['with_pairs'].split(' ')
            indata = requests.get(f"{BASE_URL}{ENDPOINTS[base]}").json()
            outdata = {'base': base, 'rates': {}, 'last_update': indata['date']}
            for pair in with_pairs:
                outdata['rates'][pair] = indata['rates'][pair]
            with open('/opt/airflow/dags/files/forex_rates.json', 'a') as outfile:
                json.dump(outdata, outfile)
                outfile.write('\n')


#definiçao da DAG, sequencia de parametros:
#id único em todo o Ariflow, 
#data de inicio de schedule
#intervalo. Se utilizar @daily, starta na meia noite
#dicionario padrao definido acima
#catchup boa pratica. Garante que o airflow nao saia rodando as execucoes (backfill)
#desde a data de inicio do job até hoje
with DAG("forex_data_pipeline", start_date=datetime(2021,1,1), 
    schedule_interval="@daily"), default_args=default_args, catchup=False) as dag:

# Task 1 - primeiro iremos verificar se a url está online
    is_forex_rates_available = HttpSensor(
        task_id="is_forex_rates_available", #tem que ser único dentro da DAG
        http_conn_id="forex_api", 
        endpoint="marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b",
        response_check=lambda response: "rates" in response.text, #verificar se o retorno está de acordo
        poke_interval = 5, #a cada 5 segundos verifica se a url e ap está online
        timeout= 20 # em segundos, ate dar erro
    )


# Task 2 - Criar um sensor para ver se existe arquivo dentro de um diretorio. Ver documentaçao de sensores
    is_forex_currencies_file_available = FileSensor(
        taskd_id= "is_forex_currencies_file_available",
        fs_conn_id= "forex_path", #connection que foi criada no airflow web
        filepath = "forex_currencies.csv", #arquivo que estamos procurando
        poke_interval = 5,
        timeout= 20
    )


# Task 3 - Executando a função Python criada no inicio do codigo
    download_rates = PythonOperator(
        task_id="downloading_rates",
        python_callable=download_rates
    )


# Task 4 - jogar o arquivo json no hdfs
# as 3 aspas abaixo permitem escrever comandos em multiplas linhas.
# primeiro comando cria um diretório chamado /fores
# segundo comando joga o arquivo json neste diretorio criado

    saving_rates = BashOperator(
        taks_id="saving_rates",
     
        bash_command="""
        hdfs dfs -mkdir -p /forex && \ 
        hdfs dfs -put -f $AIRFLOW_HOME/dags/files/forex_rates.json /forex
        """
    )

# Task 5 - Criar tabela HIVE 
creating_forex_rates_table = HiveOperator(
        task_id="creating_forex_rates_table",
        hive_cli_conn_id="hive_conn",
        hql="""
            CREATE EXTERNAL TABLE IF NOT EXISTS forex_rates(
                base STRING,
                last_update DATE,
                eur DOUBLE,
                usd DOUBLE,
                nzd DOUBLE,
                gbp DOUBLE,
                jpy DOUBLE,
                cad DOUBLE
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """
    )

# Task 6 - Processar os dados com SPARK
    forex_processing = SparkSubmitOperator(
        task_id="forex_processing",
        application="/opt/airflow/dags/scripts/forex_processing.py", 
        #esse script nao ira rodar dentro do airflow, mas sim do spark
        conn_id="spark_conn",
        verbose=False  #nao gerar muito log
    )

# Task 7 - Mandar email notification (primeiro tem que configurar no airflow.cfg)
    send_email_notification = EmailOperator(
        task_id="send_email_notification",
        to="leonardogw@hotmail.com",
        subject="forex_data_pipeline",
        html_content="<h3>forex_data_pipeline</h3>"
    )
