import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime
import pandas as pd
from sklearn.preprocessing import LabelEncoder

# Define the ETL functions
def extract_data():
    df = pd.read_csv('fraud test.csv')
    return df

def transform_data(df):
    # Perform data transformations
    transformed_df = df.copy()

    # Rename the target value
    transformed_df.rename(columns={'is_fraud': 'Target'}, inplace=True)
    
    # Convert specific columns to float
    columns_to_float = ['amt', 'merch_lat', 'merch_long','lat','long']  
    transformed_df[columns_to_float] = transformed_df[columns_to_float].astype(float)

    # Replace values in 'gender' column
    transformed_df['gender'] = transformed_df['gender'].replace({'M': 0, 'F': 1})

    # Convert specific columns to integer
    columns_to_integer = ['zip', 'gender', 'city_pop','unix_time','Target']  
    transformed_df[columns_to_integer] = transformed_df[columns_to_integer].astype(int)

    # Convert datetime column to datetime object
    transformed_df['trans_date_trans_time'] = pd.to_datetime(transformed_df['trans_date_trans_time'], format='%d/%m/%Y %H:%M', errors='coerce')
    transformed_df['dob'] = pd.to_datetime(transformed_df['dob'])
    
    # Extract date and time into separate columns
    transformed_df['trans_date'] = transformed_df['trans_date_trans_time'].dt.date
    transformed_df['trans_time'] = transformed_df['trans_date_trans_time'].dt.time

    # Concatenate 'city' and 'state' columns into 'location'
    transformed_df['location'] = transformed_df['city'] + ', ' + transformed_df['state']

    # Drop columns
    columns_to_drop = ['trans_date_trans_time', '_c0', 'city_pop','city','state']  
    transformed_df.drop(columns_to_drop, axis=1, inplace=True)


    # Perform label encoding on certain columns
    columns_to_encode = ['job', 'cc_num','merchant','street','category','location']  
    label_encoders = {}
    for col in columns_to_encode:
        label_encoders[col] = LabelEncoder()
        transformed_df[col] = label_encoders[col].fit_transform(transformed_df[col])

    
    return transformed_df

def load_data(transformed_df):
    # Load data into destination
    # Example: Write transformed data to a database
    transformed_df.to_sql('table_name', 'database_connection', if_exists='replace')

    # Send email notification
    send_email()

def send_email():
    subject = "ETL Process Completed"
    html_content = """
    <h3>Your ETL process has been completed successfully!</h3>
    <p>The data has been loaded into the destination.</p>
    """
    email_task = EmailOperator(
        task_id='Send_email',
        to='venkatyogesh003@gmail.com',
        subject=subject,
        html_content=html_content
    )
    email_task.execute(context=None)

# Define DAG
dag = DAG('ETL', 
          description='ETL process for CSV file', 
          schedule_interval='@daily',
          start_date=datetime(2024, 3, 20),
          catchup=False)

# Define tasks
extract_task = PythonOperator(task_id='extract_data', python_callable=extract_data, dag=dag)
transform_task = PythonOperator(task_id='transform_data', python_callable=transform_data, dag=dag)
load_task = PythonOperator(task_id='load_data', python_callable=load_data, dag=dag)

# Define task dependencies
extract_task >> transform_task >> load_task
