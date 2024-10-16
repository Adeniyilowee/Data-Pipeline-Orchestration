from airflow.decorators import dag, task
from airflow.sensors.base import PokeReturnValue
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from include.stock_market.tasks import _extract_stock_prices, _store_prices
from datetime import datetime
import requests
SYMBOL = 'AAPL'
bucket_name = 'stock-market'

@dag(start_date=datetime(2024, 1, 1),
     schedule='@daily', catchup=False,
     tags=['stock_market'])
def stock_market():

    @task.sensor(poke_interval=30, timeout=300, mode='poke')
    def verify_api() -> PokeReturnValue:
        api = BaseHook.get_connection('stock_api')
        url = f"{api.host}{api.extra_dejson['endpoint']}"
        response = requests.get(url, headers=api.extra_dejson['headers'])
        condition = response.json()['finance']['result'] is None
        return PokeReturnValue(is_done=condition, xcom_value=url)

    extract_stock_prices = PythonOperator(task_id='extract_stock_prices',
                                      python_callable=_extract_stock_prices,
                                      op_kwargs={'url': '{{ task_instance.xcom_pull(task_ids="verify_api") }}', 'symbol': SYMBOL})

    store_prices = PythonOperator(task_id='store_prices',
                                  python_callable= _store_prices,
                                  op_kwargs={'stock': '{{ task_instance.xcom_pull(task_ids="extract_stock_prices") }}'})

    format_prices = DockerOperator(task_id='format_prices',
                                   image='airflow/stock-app',
                                   container_name='format_prices',
                                   api_version='auto',
                                   auto_remove=True,
                                   docker_url='tcp://docker-proxy:2375',
                                   network_mode='container:spark-master',
                                   tty=True,
                                   xcom_all=False,
                                   mount_tmp_dir=False,
                                   environment={'SPARK_APPLICATION_ARGS': '{{ task_instance.xcom_pull(task_ids="store_prices") }}'})

    verify_api() >> extract_stock_prices >> store_prices >> format_prices

stock_market()