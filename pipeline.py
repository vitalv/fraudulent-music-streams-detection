#! /usr/bin/python

#An Airflow pipeline is just a Python script that happens to define an Airflow DAG object. 
#Let's start by importing the libraries we will need.

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator

from airflow.operators.python_operator import PythonOperator

from fetch import fetch_data, move_rename, gunzip

from datetime import datetime, timedelta

from process import *




default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 5, 28),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5) 
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}



dag = DAG('my_pipeline', default_args=default_args, schedule_interval='@once')








t1 = PythonOperator(
	dag=dag,
	task_id='fetch_data',
	provide_context=False,
	python_callable=fetch_data
	#op_args=['arguments_passed_to_callable'],
	#op_kwargs={'keyword_argument':'which will be passed to function'}
	)


t2 = PythonOperator(
    dag=dag,
    task_id='move_rename',
    provide_context=False,
    python_callable=move_rename
    )


t3 = PythonOperator(
    dag=dag,
    task_id='unzip',
    provide_context=False,
    python_callable=gunzip
    )


t4 =  PythonOperator(
    task_id='read_data',
    dag=dag,
    provide_context=False,
    python_callable=read_streams_tracks_users
    )


t5 =  PythonOperator(
    task_id='get_nfsc',
    dag=dag,
    provide_context=False,
    python_callable=nfsc
    )

t6 =  PythonOperator(
    task_id='add_nfsc_to_streams',
    dag=dag,
    provide_context=False,
    python_callable=add_nfsc_to_streams
    )

t7 =  PythonOperator(
    task_id='Enrichment',
    dag=dag,
    provide_context=False,
    python_callable=batch_enrich
    )


t8 =  PythonOperator(
    task_id='save_results',
    dag=dag,
    provide_context=False,
    python_callable=mv_data_to_bucket
    )


#Minimum working Pipeline:
#NOTE: This has not been fully tested as of 06/03/2018

#1 Copy data from bucket, unzip ---------
t2.set_upstream(t1)
t3.set_upstream(t2)


#2 Read and process ---------------------
t4.set_upstream(t3) #read streams to dataframe
t5.set_upstream(t4) #estimate normalized frequencies
t6.set_upstream(t5) #add the nfsc data to the streams df
t8.set_upstream(t6) #mv results back to bucket

#3 Alternative 'Enrichment' Approach
t7.set_upstream(t2)
t8.set_upstream(t7) #mv results back to bucket






#t1 >> t2 

#t2 >> t3

# This means that t2 will depend on t1
# running successfully to run
# It is equivalent to
# t1.set_downstream(t2)

#t3.set_upstream(t1)

# all of this is equivalent to
# dag.set_dependency('print_date', 'sleep')
# dag.set_dependency('print_date', 'templated')



'''
Run one task 
airflow test my_pipeline fetch_data 2018-05-28
without the need of a running web server to check 

Run dag
airflow backfill my_pipeline -s 2018-01-01

'''