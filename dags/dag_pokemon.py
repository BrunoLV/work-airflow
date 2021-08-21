import random
import requests
import json
from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(14),
}

dag = DAG(
    dag_id="dag_pokemon",
    default_args=default_args,
    description="DAG para Processamento de pokémons aleatórios",
    schedule_interval="@daily"
)

inicio = DummyOperator(
    task_id="inicio",
    dag=dag
)


def _sortea_numero_pokemon(**context):
    numero = random.randint(0, 898)
    context["task_instance"].xcom_push(key="numero_pokemon", value=numero),


sortea_numero_pokemon = PythonOperator(
    task_id='sortea_numero_pokemon',
    python_callable=_sortea_numero_pokemon,
    provide_context=True,
    dag=dag
)

pesquisa_pokemon = BashOperator(
    task_id='pesquisa_pokemon',
    bash_command="curl -o /tmp/{{ds}}/pokemon/{{ti.xcom_pull(task_ids='sortea_numero_pokemon', key='numero_pokemon')}}/dados.json --create-dirs 'https://pokeapi.co/api/v2/pokemon/{{ti.xcom_pull(task_ids='sortea_numero_pokemon', key='numero_pokemon')}}'",
    dag=dag,
)


def _busca_sprite_pokemon_shiny(**context):
    path_arquivo = f"/tmp/{context['ds']}/pokemon/{context['task_instance'].xcom_pull(task_ids='sortea_numero_pokemon', key='numero_pokemon')}/dados.json"
    path_imagem = f"/tmp/{context['ds']}/pokemon/{context['task_instance'].xcom_pull(task_ids='sortea_numero_pokemon', key='numero_pokemon')}/image.png"
    with open(path_arquivo, 'r') as f:
        dado = json.load(f)
        print(dado["sprites"]["front_shiny"])
        response = requests.get(dado["sprites"]["front_shiny"])
        with open(path_imagem, 'wb') as f:
            f.write(response.content)


busca_sprite_pokemon_shiny = PythonOperator(
    task_id="busca_sprite_pokemon_shiny",
    python_callable=_busca_sprite_pokemon_shiny,
    provide_context=True,
    dag=dag,
)


def _gera_parametros_insert(**context):
    path_arquivo = f"/tmp/{context['ds']}/pokemon/{context['task_instance'].xcom_pull(task_ids='sortea_numero_pokemon', key='numero_pokemon')}/dados.json"
    path_sql = f"/tmp/{context['ds']}/pokemon/{context['task_instance'].xcom_pull(task_ids='sortea_numero_pokemon', key='numero_pokemon')}/insert.sql"
    with open(path_arquivo, 'r') as f:
        dado = json.load(f)
        context["task_instance"].xcom_push(key="id", value=dado["id"])
        context["task_instance"].xcom_push(key="nome", value=dado["name"])


gera_parametros_insert = PythonOperator(
    task_id="gera_parametros_insert",
    python_callable=_gera_parametros_insert,
    provide_context=True,
    dag=dag,
)

insere_pokemon_banco = PostgresOperator(
    task_id="insere_pokemon_banco",
    postgres_conn_id="pokemon_db",
    sql='''INSERT INTO pokemon (id, nome) VALUES ({{ti.xcom_pull(task_ids='gera_parametros_insert', key='id')}}, '{{ti.xcom_pull(task_ids='gera_parametros_insert', key='nome')}}')''',
    dag=dag,
)

fim = DummyOperator(
    task_id="fim",
    dag=dag,
)

inicio >> sortea_numero_pokemon >> pesquisa_pokemon

pesquisa_pokemon >> busca_sprite_pokemon_shiny
pesquisa_pokemon >> gera_parametros_insert

gera_parametros_insert >> insere_pokemon_banco >> fim

busca_sprite_pokemon_shiny >> fim
