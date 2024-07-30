from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import investpy as inv
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import boto3
import s3fs
import os

def collect_data_b3():
    # Obter a data de ontem
    date = datetime.now() - timedelta(1)
    date_str = date.strftime('%Y-%m-%d')
    date_end = datetime.now().strftime('%Y-%m-%d')

    # Obter as ações do mercado brasileiro
    br = inv.stocks.get_stocks(country='brazil')
    carteira = []

    # Adicionar sufixo '.SA' para ações com símbolos de até 5 caracteres
    for a in br['symbol']:
        if len(a) <= 5:
            carteira.append(a + '.SA')

    # Baixar os dados ajustados de fechamento
    data = yf.download(carteira, start=date_str, end=date_end)['Adj Close']

    # Criar o DataFrame
    df = pd.DataFrame(data)

    # Redefinir índice para que 'Date' seja uma coluna regular
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'Date'}, inplace=True)

    # Usar a função melt para rearranjar o DataFrame
    df_melted = pd.melt(df, id_vars=['Date'], var_name='Nome_ação', value_name='Valor')

    # Remover as linhas com valores NaN, se necessário
    df_melted = df_melted.dropna(subset=['Valor'])

    # Configurar credenciais da AWS
    os.environ['AWS_ACCESS_KEY_ID'] = '<KEY_ID>'
    os.environ['AWS_SECRET_ACCESS_KEY'] = '<ACCESS_KEY>'

    # Inicialize o cliente S3
    s3_client = boto3.client('s3')

    # Bucket onde os dados serão armazenados
    bucket = 'tech-challenge-raw-bucket-bovespa'

    # Nome da pasta baseado na data de ontem
    folder = f'extracted_at={date_str}'

    # Configurar sistema de arquivos S3
    s3 = s3fs.S3FileSystem(anon=False)

    # Caminho completo do arquivo no S3
    file_path = f's3://{bucket}/{folder}/data.parquet'

    # Salvar DataFrame diretamente no S3 sem particionar por 'Date'
    df_melted.to_parquet(file_path, engine='pyarrow', filesystem=s3, index=False)

    print(f"Dados salvos no caminho: {file_path}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'collect_data_b3_dag',
    default_args=default_args,
    description='DAG para coletar dados da B3 e salvar no S3',
    schedule_interval='0 10 * * *',
    start_date=days_ago(1),
    catchup=False,
)

t1 = PythonOperator(
    task_id='collect_data_b3_task',
    python_callable=collect_data_b3,
    dag=dag,
)

t1
