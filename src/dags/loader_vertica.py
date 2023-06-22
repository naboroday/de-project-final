from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import vertica_python
import psycopg2

start_date = datetime(2022, 10, 1)  # Начало периода загрузки данных
end_date = datetime(2022, 10, 6)  # Конец периода загрузки данных




# Функция для проверки наличия данных в PostgreSQL
def check_vertica_data(start_date, end_date):
    v_conn = vertica_python.connect(host='51.250.75.20', port='5433', database='dwh', user='st23052707', password='efRIi4ukdyD8sxu')
    v_cursor = v_conn.cursor()
    
    #yesterday = datetime.now() - timedelta(days=1)
    # Преобразование строки в объект datetime
    start_date = start_date
    end_date = end_date

    # Вычисление даты вчера
    #yesterday = start_date - timedelta(days=1)

    # Форматирование дат для запроса
    #yesterday_str = yesterday.strftime('%Y-%m-%d')

    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')

    # Использование отформатированных дат в запросе
    query = """SELECT count(a.operation_id) 
            FROM ST23052707__STAGING.transactions_1 a 
            LEFT JOIN ST23052707__STAGING.transactions_keys tk ON tk.operation_id = a.operation_id AND tk.transaction_dt = a.transaction_dt 
            WHERE tk.operation_id IS NULL AND tk.transaction_dt IS NULL 
              AND date(a.transaction_dt) >= %s
              AND date(a.transaction_dt) < %s
    """
    v_cursor.execute(query, (start_date, end_date))
    result = v_cursor.fetchone()
    count = result[0]
    
    v_cursor.close()
    v_conn.close()
    
    return count > 0



def transfer_data_to_vitrine(start_date, end_date):
    v_conn = vertica_python.connect(host='51.250.75.20', port='5433', database='dwh', user='st23052707', password='efRIi4ukdyD8sxu')
    v_cursor = v_conn.cursor()

    # Формирование даты
    start_date = start_date
    end_date = end_date


    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')

    # Использование отформатированной даты в запросе
    query = """
        INSERT INTO ST23052707__DWH.global_metrics (date_update, currency_from, amount_total, cnt_transactions, avg_transactions_per_account, cnt_accounts_make_transactions)
        SELECT b.date_update, b.currency_from,
            SUM(CASE WHEN b.amount < 0 THEN b.amount * (-1) ELSE b.amount * 1 END)/100 as amount_total,
            SUM(cnt_operation_id) as cnt_transactions,
            SUM(cnt_operation_id)/COUNT(account_number_from) as avg_transactions_per_account,
            COUNT(account_number_from) as cnt_accounts_make_transactions
        FROM (SELECT  DISTINCT date(transaction_dt) as date_update, currency_code as currency_from,
                  account_number_from, amount, count(operation_id) as cnt_operation_id
            FROM (SELECT a.transaction_dt, a.currency_code, a.account_number_from,a.amount,a.operation_id
                FROM ST23052707__STAGING.transactions_1 a
                LEFT JOIN ST23052707__STAGING.transactions_keys c
                  ON c.operation_id = a.operation_id AND c.transaction_dt = a.transaction_dt
                WHERE a.account_number_from>0 and c.operation_id IS NULL AND c.transaction_dt IS NULL 
                    AND date(a.transaction_dt) >= %s
                    AND date(a.transaction_dt) < %s
                )a
            GROUP BY date(transaction_dt), currency_code,account_number_from, amount
            ) b
        GROUP BY b.date_update, b.currency_from
    """
    v_cursor.execute(query, (start_date, end_date))
    v_conn.commit()

    v_cursor.close()
    v_conn.close()

def transfer_data_to_transactions_keys(start_date, end_date):
    v_conn = vertica_python.connect(host='51.250.75.20', port='5433', database='dwh', user='st23052707', password='efRIi4ukdyD8sxu')
    v_cursor = v_conn.cursor()

    # Формирование даты
    start_date = start_date
    end_date = end_date


    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')

    # Использование отформатированной даты в запросе
    query = """
        INSERT INTO ST23052707__STAGING.transactions_keys (operation_id, transaction_dt, date_load)
        SELECT a.operation_id, a.transaction_dt, cast(now() as timestamp) as date_load
        FROM ST23052707__STAGING.transactions_1 a
        LEFT JOIN ST23052707__STAGING.transactions_keys c
              ON c.operation_id = a.operation_id AND c.transaction_dt = a.transaction_dt
        WHERE a.account_number_from>0 and c.operation_id IS NULL AND c.transaction_dt IS NULL 
              AND date(a.transaction_dt) >= %s
              AND date(a.transaction_dt) < %s
    """
    v_cursor.execute(query, (start_date, end_date))
    v_conn.commit()

    v_cursor.close()
    v_conn.close()    

# Определение DAG
dag = DAG(
    'transfer_data',
    description='Transfer data from transactions_1 to vitrine',
    schedule_interval='@daily',
    start_date=datetime(2023, 6, 20),
    catchup=False
)

# Определение задачи
check_vertica_data = PythonOperator(
    task_id='check_vertica_data',
    python_callable=check_vertica_data,
    op_kwargs={'start_date': start_date, 'end_date': end_date},  # Передаем переменные в качестве именованных аргументов
    dag=dag
)

# Оператор для загрузки данных в Vertica
load_data_transactions_to_vitrina = PythonOperator(
    task_id='transfer_data_to_vitrine',
    python_callable=transfer_data_to_vitrine,
    op_kwargs={'start_date': start_date, 'end_date': end_date},  # Передаем переменные в качестве именованных аргументов
    dag=dag
)

# Оператор для загрузки данных в Vertica
transfer_data_to_transactions_keys = PythonOperator(
    task_id='transfer_data_to_transactions_keys',
    python_callable=transfer_data_to_transactions_keys,
    op_kwargs={'start_date': start_date, 'end_date': end_date},  # Передаем переменные в качестве именованных аргументов
    dag=dag
)


# Установка зависимостей
check_vertica_data >> load_data_transactions_to_vitrina >> transfer_data_to_transactions_keys

