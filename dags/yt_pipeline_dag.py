from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from ml.pretrained_sentiment import main as run_sentiment
from app.main import main as ingest_data

# Default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 10, 8),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# Define the DAG
dag = DAG(
    dag_id='yt_comments_pipeline',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
    description='YouTube Comments ETL + Sentiment Analysis'
)




trigger_ingest = PythonOperator(
    task_id='trigger_ingest',
    python_callable=ingest_data,
    dag=dag
)


run_script_task = BashOperator(
        task_id='trigger_transform',
        bash_command="""python /home/airflow/gcs/dags/dataflow/pipeline.py --runner DataflowRunner --no_use_public_ips""",
		dag=dag
    )

# Step 2: Run pretrained sentiment analysis
pretrained_sentiment = PythonOperator(
    task_id='pretrained_sentiment',
    python_callable=run_sentiment,
    dag=dag
)

# Step 3: Define dependencies
trigger_ingest >>  run_script_task >> pretrained_sentiment