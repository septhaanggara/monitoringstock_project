import airflow.utils.dates
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.sensors.python import PythonSensor
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.operators.mysql_operator import MySqlOperator
from airflow.utils.dates import days_ago
from airflow.hooks.mysql_hook import MySqlHook
import pandas as pd



default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023,3,16),
    'depends_on_past': False,
    'retries': 0
}


dag = DAG(
    dag_id="monitoring",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval='*/1 * * * *',
    description="A batch workflow for ingesting supermarket promotions data, demonstrating the PythonSensor.",
)
def upload():
    data= {"sku": ["1","2","3"],
            "name": ["Ultra", "Indomie", "Rinso"],
            "stock": [6, 7, 8],}
    df=pd.DataFrame(data)

    # save to csv file
    df.to_csv('/home/airflow/product.csv', index=False) # default delimiter is comma
    print("data saved to csv")


product_data = PythonOperator(
    task_id='product_data',
    python_callable=upload,
    dag=dag,
)

create_table = MySqlOperator(
    sql="""
    CREATE TABLE product(sku, name VARCHAR(25), stock int); 
    """
    , task_id="CreateTable",
    mysql_conn_id="mysql_database",
    dag=dag
    )

insert_data = MySqlOperator(
    sql=""" 
    INSERT INTO product(sku, name, stock) VALUES(2,'indomie',15),(3,'rinso',15),(1,'ultra',-3); 
    """,
    task_id="InsertData",
    mysql_conn_id="mysql_database",
    dag=dag
    )


def datafinal():
    #open connection
    mysql_hook = MySqlHook(mysql_conn_id='mysql_database').get_conn()
    cursor = conn.cursor()
    i=0
    for i in range len(df):
        if df['sku'][i]== cursor.execute('select * from product where sku = %s', (df['sku'][i])):
            sql= """ 
            UPDATE product set stock = %s where sku = %s",
            """,(df['stock'], row['sku'])
        else:
            sql= """ 
           INSERT INTO product(sku, name, stock) VALUES(%s, %s, %s); 
            """,(row['sku'],df['name'],df['stock'])
        

datafinal = PythonOperator(
    task_id='datafinal',
    python_callable = datafinal,
    dag=dag
)


t1 = BashOperator(
    task_id = 'done',
    bash_command = 'echo "Downloaded JSON file"',
    dag = dag
)

product_data >> create_table >> insert_data >> datafinal >> t1