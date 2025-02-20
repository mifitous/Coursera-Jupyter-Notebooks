from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
import os

# Define DAG arguments
default_args = {
    'owner': 'dummy_user',
    'start_date': datetime.today(),
    'email': ['dummy@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['ETL']
)

tgz_file_dir = '/home/project/airflow/dags/finalassignment/'
extract_dir = tgz_file_dir + 'extracted_files'
staging_dir = '/home/project/airflow/dags/finalassignment/staging'

# Define the Bash command to unzip the data
unzip_data_command = f"""
mkdir -p {extract_dir}
tar -xvzf {os.path.join(tgz_file_dir, 'tolldata.tgz')} -C {extract_dir}
"""

# Define the BashOperator task to unzip the data
unzip_data_task = BashOperator(
    task_id='unzip_data',
    bash_command=unzip_data_command,
    dag=dag,
)

# Define the Bash command to extract data from vehicle-data.csv
extract_data_from_csv_command = f"""
csv_file={os.path.join(extract_dir, 'vehicle-data.csv')}
output_file={os.path.join(extract_dir, 'csv_data.csv')}
awk -F',' 'BEGIN {{OFS=","}} {{print $1, $2, $3, $4}}' $csv_file > $output_file
"""

# Define the BashOperator task to extract the data from CSV
extract_data_from_csv_task = BashOperator(
    task_id='extract_data_from_csv',
    bash_command=extract_data_from_csv_command,
    dag=dag,
)

# Define the Bash command to extract data from tollplaza-data.tsv
extract_data_from_tsv_command = f"""
tsv_file={os.path.join(extract_dir, 'tollplaza-data.tsv')}
output_file={os.path.join(extract_dir, 'tsv_data.csv')}
awk -F'\\t' 'BEGIN {{OFS=","}} {{print $5, $6, $7}}' $tsv_file > $output_file
"""

# Define the BashOperator task to extract the data from TSV
extract_data_from_tsv_task = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command=extract_data_from_tsv_command,
    dag=dag,
)

# Define the Bash command to extract data from the fixed-width file payment-data.txt
extract_data_from_fixed_width_command = f"""
fixed_width_file={os.path.join(extract_dir, 'payment-data.txt')}
output_file={os.path.join(extract_dir, 'fixed_width_data.csv')}
awk '{{print substr($0, 1, 7), substr($0, 8, 25), substr($0, 33, 8), substr($0, 41, 10), substr($0, 51, 4), substr($0, 55, 6)}}' $fixed_width_file > $output_file
"""

# Define the BashOperator task to extract the data from the fixed-width file
extract_data_from_fixed_width_task = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command=extract_data_from_fixed_width_command,
    dag=dag,
)

# Define the Bash command to consolidate the data
consolidate_data_command = f"""
paste -d',' {os.path.join(extract_dir, 'csv_data.csv')} \
{os.path.join(extract_dir, 'tsv_data.csv')} \
{os.path.join(extract_dir, 'fixed_width_data.csv')} > \
{os.path.join(extract_dir, 'extracted_data.csv')}
"""

# Define the BashOperator task to consolidate the data
consolidate_data_task = BashOperator(
    task_id='consolidate_data',
    bash_command=consolidate_data_command,
    dag=dag,
)

# Define the Bash command to transform the vehicle_type field to uppercase
transform_data_command = f"""
awk 'BEGIN {{FS=","; OFS=","}} {{ $4 = toupper($4); print }}' {os.path.join(extract_dir, 'extracted_data.csv')} > {os.path.join(staging_dir, 'transformed_data.csv')}
"""

# Define the BashOperator task to transform the data
transform_data_task = BashOperator(
    task_id='transform_data',
    bash_command=transform_data_command,
    dag=dag,
)

# Set the task sequence
unzip_data_task >> extract_data_from_csv_task >> extract_data_from_tsv_task >> extract_data_from_fixed_width_task >> consolidate_data_task >> transform_data_task