from airflow.decorators import dag, task
from airflow.sensors.base import PokeReturnValue
from airflow.hooks.base import BaseHook
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

    verify_api()

stock_market()
