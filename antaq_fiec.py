# Módulos do Airflow
from datetime import datetime, timedelta  
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.edgemodifier import Label
from airflow.operators.email_operator import EmailOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,    
    'start_date': datetime(2025, 1, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=120),
    'schedule_interval': '*/15 * * * *'
}

with DAG(    
    dag_id='antaq_fiec',
    default_args=default_args,
    schedule_interval=None,
    tags=['antaq'],
) as dag:    


    ingest = BashOperator(bash_command="cd $AIRFLOW_HOME/dags/ingest.py", task_id="extrair_dados")


    move_arq = BashOperator(bash_command="cd $AIRFLOW_HOME/dags/mover_arquivo.py", task_id="move_arq")    
    

    analyse = BashOperator(bash_command="cd $AIRFLOW_HOME/dags/transform.py", task_id="transforma_dados")    
    
    carga_interna = BashOperator(bash_command="cd $AIRFLOW_HOME/dags/carga_etl_interna.py", task_id="etl_interna")    
    
    carga_cliente = BashOperator(bash_command="cd $AIRFLOW_HOME/dags/carga_etl_cliente.py", task_id="etl_cliente")    

    valida_processo    = BashOperator (bash_command="",task_id="valida_carga") 
    finaliza_processo  = BashOperator (bash_command="",task_id="finaliza_processo") 
    send_email         = EmailOperator(
                                        task_id='send_email',
                                        to='atulio516@gmail.com',
                                        subject='Processo de carga',
                                        html_content='Erro na carga',
                                        dag=dag
                                        )
    
    ingest >> analyse >> [carga_interna, carga_cliente]
    ingest >> move_arq
    carga_interna >> valida_processo >> Label("Erro")    >> send_email
    carga_cliente >> valida_processo >> Label("Sucesso") >> finaliza_processo
    