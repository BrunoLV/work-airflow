import json
import os
import random

import requests
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(14),
}

dag = DAG(
    dag_id="dag_captura_pokemon",
    default_args=default_args,
    description="DAG para Processamento de captura de pokémons aleatórios",
    schedule_interval="@daily"
)

inicio = DummyOperator(
    task_id="inicio",
    dag=dag
)


def _obtem_xcom_value(taskIds, key, context):
    return context["ti"].xcom_pull(task_ids=taskIds, key=key)


def _publica_xcom_value(key, value, context):
    context["ti"].xcom_push(key=key, value=value)


def _deleta_registros_existentes_no_dia(ds):
    sql = f"DELETE FROM tb_captura_pokemon WHERE data_captura = '{ds}'"
    pg_hook = PostgresHook(postgres_conn_id="pokemon_db")
    pg_hook.run(sql, autocommit=True)


def _deleta_arquivos_existentes_no_dia(ds):
    dir_path = f"/tmp/captura/{ds}/pokemon/"
    if os.path.exists(dir_path) and os.path.isdir(dir_path):
        os.remove(dir_path)


def _prepara_captura_do_dia(**context):
    _deleta_registros_existentes_no_dia(context['ds'])
    _deleta_arquivos_existentes_no_dia(context['ds'])


prepara_captura_do_dia = PythonOperator(
    task_id='prepara_captura_do_dia',
    python_callable=_prepara_captura_do_dia,
    provide_context=True,
    dag=dag
)


def _sortea_numero_pokemon(**context):
    numero = random.randint(0, 898)
    _publica_xcom_value(key="numero_pokemon", value=numero, context=context)


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
    execution_data = context['ds']
    numero_pokemon = _obtem_xcom_value(taskIds='sortea_numero_pokemon', key='numero_pokemon', context=context)

    path_arquivo = f"/tmp/{execution_data}/pokemon/{numero_pokemon}/dados.json"
    path_imagem = f"/tmp/{execution_data}/pokemon/{numero_pokemon}/image.png"

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
    execution_date = context['ds']
    numero_pokemon = _obtem_xcom_value(taskIds='sortea_numero_pokemon', key='numero_pokemon', context=context)
    path_arquivo = f"/tmp/{execution_date}/pokemon/{numero_pokemon}/dados.json"

    with open(path_arquivo, 'r') as f:
        dado = json.load(f)
        _publica_xcom_value(key="nome", value=dado["name"], context=context)


gera_parametros_insert = PythonOperator(
    task_id="gera_parametros_insert",
    python_callable=_gera_parametros_insert,
    provide_context=True,
    dag=dag,
)

insere_captura_banco = PostgresOperator(
    task_id="insere_captura_banco",
    postgres_conn_id="pokemon_db",
    sql='''
        INSERT INTO tb_captura_pokemon (
            numero, 
            nome, 
            data_captura
        ) VALUES (
            {{ti.xcom_pull(task_ids='sortea_numero_pokemon', key='numero_pokemon')}}, 
            '{{ti.xcom_pull(task_ids='gera_parametros_insert', key='nome')}}', 
            '{{ds}}'
        )
        ''',
    dag=dag,
)

fim = DummyOperator(
    task_id="fim",
    dag=dag,
)

inicio >> prepara_captura_do_dia >> sortea_numero_pokemon >> pesquisa_pokemon

pesquisa_pokemon >> busca_sprite_pokemon_shiny
pesquisa_pokemon >> gera_parametros_insert

gera_parametros_insert >> insere_captura_banco >> fim

busca_sprite_pokemon_shiny >> fim
