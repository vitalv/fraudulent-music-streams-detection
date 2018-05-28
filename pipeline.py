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


'''
We're about to create a DAG and some tasks,
we have the choice to explicitly pass a set of arguments to each task's constructor (which would become redundant), 
or (better!) we can define a dictionary of default parameters that we can use when creating tasks.
'''


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



'''
DAG object to nest the tasks into. 
Pass a string that defines the dag_id, a unique identifier for the DAG. 
Pass the default argument dictionary defined above and define a schedule_interval of 1 day for the DAG.

If the DAG is written to handle it's own catchup 
(IE not limited to the interval, but instead to "Now" for instance.), 
then you will want to turn catchup off (Either on the DAG itself with dag.catchup = False) 
'''

#dag = DAG('my_pipeline', default_args=default_args, catchup=False )
dag = DAG('my_pipeline', default_args=default_args, schedule_interval='@once')






'''
Tasks are generated when instantiating operator objects. 
An object instantiated from an operator is called a constructor. 
The first argument task_id acts as a unique identifier for the task.

'''


'''
t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag)

t2 = BashOperator(
    task_id='sleep',
    bash_command='sleep 5',
    retries=3,
    dag=dag)


templated_command = """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7) }}"
        echo "{{ params.my_param }}"
    {% endfor %}
"""

t3 = BashOperator(
    task_id='templated',
    bash_command=templated_command,
    params={'my_param': 'Parameter I passed in'},
    dag=dag)

'''


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



t2.set_upstream(t1)
t3.set_upstream(t2)

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
Run 
airflow test my_pipeline fetch_data 2018-05-28
without the need of a running web server to check 
'''