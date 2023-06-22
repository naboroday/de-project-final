-- Создание таблиц для ключей в POSTGRES

CREATE TABLE public.currencies_keys (
	date_update timestamp NULL,
	currency_code int4 NULL,
	currency_code_with int4 NULL
);

CREATE TABLE public.transactions_keys (
	operation_id varchar(60) NULL,
	transaction_dt timestamp NULL,
	date_load timestamp NULL
);


-- Создание таблиц в VERTICA
CREATE TABLE ST23052707__STAGING.transactions_1
(
    operation_id varchar(60),
    transaction_dt varchar(30),
    account_number_from int,
    account_number_to int,
    currency_code int,
    country varchar(30),
    status varchar(30),
    transaction_type varchar(30),
    amount varchar(30)
);

CREATE TABLE ST23052707__STAGING.currencies
(
    date_update timestamp,
    currency_code int,
    currency_code_with int,
    currency_with_div numeric(5,3)
);

CREATE TABLE ST23052707__STAGING.transactions_keys
(
    operation_id varchar(60),
    transaction_dt varchar(30),
    date_load timestamp
);

CREATE TABLE ST23052707__DWH.global_metrics
(
    date_update date,
    currency_from varchar(3),
    amount_total numeric(10,2),
    cnt_transactions int,
    avg_transactions_per_account numeric(10,2),
    cnt_accounts_make_transactions int
);

-- Примеры создания запросов в METABASE
--Количества уникальных пользователей
SELECT DISTINCT
  a.currency_from,
  SUM(cnt_accounts_make_transactions) AS cnt_accounts_make_transactions,
  a.date_update
  FROM ST23052707__DWH.global_metrics as a
WHERE a.date_update >= {{start_date}} AND a.date_update <= {{end_date}}
  [[AND currency_from = {{currency_from}}]]
  GROUP BY currency_from,date_update;

-- Общий оборот компании в единой валюте
SELECT DISTINCT
  a.currency_code_with,
  SUM(a.all_amount_total) as all_amount_total,
  a.date_update
  FROM (SELECT NULL as  currency_from, NULL as amount_total,a.date_update,currency_from AS currency_code_with,amount_total AS all_amount_total
        FROM ST23052707__DWH.global_metrics as a
        WHERE
            a.date_update >= {{start_date}} AND a.date_update <= {{end_date}} 
        GROUP BY
         a.date_update,currency_code_with,amount_total
        UNION ALL
        SELECT  NULL as currency_from,NULL AS amount_total,a.date_update,CAST(c.currency_code_with as varchar(5)) as currency_code_with,(currency_with_div*amount_total) AS all_amount_total
        FROM ST23052707__DWH.global_metrics as a
        INNER JOIN ST23052707__STAGING.currencies as c ON a.date_update=c.date_update and a.currency_from=c.currency_code
        WHERE
            a.date_update >= {{start_date}} AND a.date_update <= {{end_date}} ) a 
WHERE 1=1
  [[AND currency_code_with = {{currency_code_with}}]]
GROUP BY a.date_update,a.currency_code_with;