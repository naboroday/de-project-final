from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import vertica_python


start_date = datetime(2022, 10, 1)  # Начало периода загрузки данных
end_date = datetime(2022, 10, 6)  # Конец периода загрузки данных




# Функция для проверки наличия данных в PostgreSQL
def check_postgres_data(start_date, end_date):
    pg_conn = psycopg2.connect(host='rc1b-w5d285tmxa8jimyn.mdb.yandexcloud.net', port='6432', dbname='db1', user='student', password='de_student_112022')
    pg_cursor = pg_conn.cursor()
    
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
    query = f"SELECT count(a.operation_id)  " \
            f"FROM public.transactions a " \
            f"LEFT JOIN public.transactions_keys tk ON tk.operation_id = a.operation_id AND tk.transaction_dt = a.transaction_dt " \
            f"WHERE tk.operation_id IS NULL AND tk.transaction_dt IS NULL AND a.transaction_dt >= '{start_date_str}' " \
            f"AND a.transaction_dt < '{end_date_str}'"
    pg_cursor.execute(query)
    result = pg_cursor.fetchone()
    count = result[0]
    
    pg_cursor.close()
    pg_conn.close()
    
    return count > 0

def load_data_transactions_to_vertica(start_date, end_date):
    pg_conn = psycopg2.connect(host='rc1b-w5d285tmxa8jimyn.mdb.yandexcloud.net', port='6432', dbname='db1', user='student', password='de_student_112022')
    pg_cursor = pg_conn.cursor()

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
    query = f"SELECT a.operation_id, a.transaction_dt, a.account_number_from, a.account_number_to, a.currency_code, " \
            f"a.country, a.status, a.transaction_type, a.amount " \
            f"FROM public.transactions a " \
            f"LEFT JOIN public.transactions_keys tk ON tk.operation_id = a.operation_id AND tk.transaction_dt = a.transaction_dt " \
            f"WHERE tk.operation_id IS NULL AND tk.transaction_dt IS NULL AND a.transaction_dt >= '{start_date_str}' " \
            f"AND a.transaction_dt < '{end_date_str}'"

    pg_cursor.execute(query)
    records = pg_cursor.fetchall()

    new_records = []

    batch_size = 1000
    for i in range(0, len(records), batch_size):
        batch_records = records[i:i + batch_size]
        csv_data = '\n'.join([','.join(map(str, record)) for record in batch_records])

        copy_query = """COPY ST23052707__STAGING.transactions_1 (operation_id, transaction_dt, account_number_from, 
                        account_number_to, currency_code, country, status, transaction_type, amount) 
                        FROM STDIN DELIMITER ',' ENCLOSED BY '\"' DIRECT ABORT ON ERROR"""

        v_cursor.copy(copy_query, csv_data)
        new_records.extend(batch_records)

    v_conn.commit()
    v_cursor.close()
    v_conn.close()

    pg_cursor.close()
    pg_conn.close()

def load_data_currencies_to_vertica(start_date, end_date):
    pg_conn = psycopg2.connect(host='rc1b-w5d285tmxa8jimyn.mdb.yandexcloud.net', port='6432', dbname='db1', user='student', password='de_student_112022')
    pg_cursor = pg_conn.cursor()

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
    query = f"SELECT a.date_update, a.currency_code, a.currency_code_with, a.currency_with_div " \
            f"FROM public.currencies a " \
            f"LEFT JOIN public.currencies_keys tk ON tk.date_update = a.date_update AND tk.currency_code = a.currency_code and tk.currency_code_with = a.currency_code_with " \
            f"WHERE tk.date_update IS NULL AND tk.currency_code IS NULL " \
            f"AND a.date_update >= '{start_date_str}' AND a.date_update < '{end_date_str}'"

    pg_cursor.execute(query)
    records = pg_cursor.fetchall()
 
    new_records = []
    batch_size = 1000

    for i in range(0, len(records), batch_size):
        batch_records = records[i:i + batch_size]
        csv_data = '\n'.join([','.join(map(str, record)) for record in batch_records])

        copy_query = """COPY ST23052707__STAGING.currencies (date_update, currency_code, currency_code_with, currency_with_div) 
                        FROM STDIN DELIMITER ',' ENCLOSED BY '\"' DIRECT"""

        v_cursor.copy(copy_query, csv_data)
        new_records.extend(batch_records)

    v_conn.commit()
    v_cursor.close()
    v_conn.close()

    pg_cursor.close()
    pg_conn.close()

def load_data_currencies_to_postgres(start_date, end_date):
    pg_conn = psycopg2.connect(host='rc1b-w5d285tmxa8jimyn.mdb.yandexcloud.net', port='6432', dbname='db1', user='student', password='de_student_112022')
    pg_cursor = pg_conn.cursor()

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

   

    # Подготовка данных для массовой вставки
    #data_to_insert = [(record[0], record[1], record[2], datetime.now()) for record in records]

    # Массовая вставка данных в PostgreSQL
    query_1 = """
        INSERT INTO public.currencies_keys (date_update, currency_code, currency_code_with, date_load)
        SELECT a.date_update, a.currency_code, a.currency_code_with, cast(now() as timestamp) as date_load
        FROM public.currencies a
        LEFT JOIN public.currencies_keys tk
            ON tk.date_update = a.date_update
            AND tk.currency_code = a.currency_code
            AND tk.currency_code_with = a.currency_code_with
        WHERE tk.date_update IS NULL
            AND tk.currency_code IS NULL
            AND a.date_update >= %s
            AND a.date_update < %s
    """

    pg_cursor.execute(query_1, (start_date, end_date))
        
    pg_conn.commit()  

    pg_cursor.close()
    pg_conn.close()

    return "Data loaded successfully" 

def load_data_transactions_to_postgres(start_date, end_date):
    pg_conn = psycopg2.connect(host='rc1b-w5d285tmxa8jimyn.mdb.yandexcloud.net', port='6432', dbname='db1', user='student', password='de_student_112022')
    pg_cursor = pg_conn.cursor()

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
    query = """INSERT INTO public.transactions_keys (operation_id, transaction_dt, date_load) 
            SELECT a.operation_id, a.transaction_dt, cast(now() as timestamp) as date_load 
            FROM public.transactions a 
            LEFT JOIN public.transactions_keys tk ON tk.operation_id = a.operation_id AND tk.transaction_dt = a.transaction_dt 
            WHERE tk.operation_id IS NULL 
                AND tk.transaction_dt IS NULL 
                AND a.transaction_dt >= %s 
                AND a.transaction_dt < %s
    """
    pg_cursor.execute(query, (start_date, end_date))
        
    pg_conn.commit()  

    pg_cursor.close()
    pg_conn.close()

    return "Data loaded successfully" 




# Создаем DAG
dag = DAG(
    'load_data_from_postgres_to_vertica',
    description='Load data from PostgreSQL to Vertica',
    schedule_interval='@daily',  # Запускать каждый день в полночь
    start_date=start_date,
    end_date=end_date
)

# Оператор для проверки наличия данных в PostgreSQL
check_data_task = PythonOperator(
    task_id='check_postgres_data',
    python_callable=check_postgres_data,
    op_kwargs={'start_date': start_date, 'end_date': end_date}, 
    dag=dag
)

# Оператор для загрузки данных из PostgreSQL в Vertica
load_data_currencies_task = PythonOperator(
    task_id='load_data_currencies_task',
    python_callable=load_data_currencies_to_vertica,
    op_kwargs={'start_date': start_date, 'end_date': end_date},  # Передаем переменные в качестве именованных аргументов
    dag=dag
)


# Оператор для загрузки данных из PostgreSQL в Vertica
load_data_currencies_to_postgres = PythonOperator(
    task_id='load_data_currencies_to_postgres',
    python_callable=load_data_currencies_to_postgres,
    op_kwargs={'start_date': start_date, 'end_date': end_date},  # Передаем переменные в качестве именованных аргументов
    dag=dag
)

# Оператор для загрузки данных из PostgreSQL в Vertica
load_data_transactions_to_vertica = PythonOperator(
    task_id='load_data_transactions_to_vertica',
    python_callable=load_data_transactions_to_vertica,
    op_kwargs={'start_date': start_date, 'end_date': end_date},  # Передаем переменные в качестве именованных аргументов
    dag=dag
)

# Оператор для загрузки данных из PostgreSQL в Vertica
load_data_transactions_to_postgres = PythonOperator(
    task_id='load_data_transactions_to_postgres',
    python_callable=load_data_transactions_to_postgres,
    op_kwargs={'start_date': start_date, 'end_date': end_date},  # Передаем переменные в качестве именованных аргументов
    dag=dag
)


# Устанавливаем зависимости между задачами
check_data_task >> load_data_currencies_task >> load_data_transactions_to_vertica >> (load_data_transactions_to_postgres, load_data_currencies_to_postgres)