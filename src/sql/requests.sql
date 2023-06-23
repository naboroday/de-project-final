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

--____________VERTICA____________ 
-- Создание таблиц в VERTICA

--transactions_1
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

CREATE PROJECTION ST23052707__STAGING.transactions_1 /*+createtype(L)*/ 
(
 operation_id,
 transaction_dt,
 account_number_from,
 account_number_to,
 currency_code,
 country,
 status,
 transaction_type,
 amount
)
AS
 SELECT transactions_1.operation_id,
        transactions_1.transaction_dt,
        transactions_1.account_number_from,
        transactions_1.account_number_to,
        transactions_1.currency_code,
        transactions_1.country,
        transactions_1.status,
        transactions_1.transaction_type,
        transactions_1.amount
 FROM ST23052707__STAGING.transactions_1
 ORDER BY transactions_1.operation_id,
          transactions_1.transaction_dt,
          transactions_1.account_number_from,
          transactions_1.account_number_to,
          transactions_1.currency_code,
          transactions_1.country,
          transactions_1.status,
          transactions_1.transaction_type
SEGMENTED BY hash(transactions_1.account_number_from, transactions_1.account_number_to, transactions_1.currency_code, transactions_1.transaction_dt, transactions_1.country, transactions_1.status, transactions_1.transaction_type, transactions_1.amount) ALL NODES KSAFE 1;


SELECT MARK_DESIGN_KSAFE(1);

--currencies
CREATE TABLE ST23052707__STAGING.currencies
(
    date_update timestamp,
    currency_code int,
    currency_code_with int,
    currency_with_div numeric(5,3)
);

CREATE PROJECTION ST23052707__STAGING.currencies /*+createtype(L)*/ 
(
 date_update,
 currency_code,
 currency_code_with,
 currency_with_div
)
AS
 SELECT currencies.date_update,
        currencies.currency_code,
        currencies.currency_code_with,
        currencies.currency_with_div
 FROM ST23052707__STAGING.currencies
 ORDER BY currencies.date_update,
          currencies.currency_code,
          currencies.currency_code_with,
          currencies.currency_with_div
SEGMENTED BY hash(currencies.date_update, currencies.currency_code, currencies.currency_code_with, currencies.currency_with_div) ALL NODES KSAFE 1;


SELECT MARK_DESIGN_KSAFE(1);
--transactions_keys
CREATE TABLE ST23052707__STAGING.transactions_keys
(
    operation_id varchar(60),
    transaction_dt varchar(30),
    date_load timestamp
);

CREATE PROJECTION ST23052707__STAGING.transactions_keys /*+createtype(L)*/ 
(
 operation_id,
 transaction_dt,
 date_load
)
AS
 SELECT transactions_keys.operation_id,
        transactions_keys.transaction_dt,
        transactions_keys.date_load
 FROM ST23052707__STAGING.transactions_keys
 ORDER BY transactions_keys.operation_id,
          transactions_keys.transaction_dt
SEGMENTED BY hash(transactions_keys.date_load, transactions_keys.transaction_dt, transactions_keys.operation_id) ALL NODES KSAFE 1;


SELECT MARK_DESIGN_KSAFE(1);

--global_metrics
CREATE TABLE ST23052707__DWH.global_metrics
(
    date_update date,
    currency_from varchar(3),
    amount_total numeric(10,2),
    cnt_transactions int,
    avg_transactions_per_account numeric(10,2),
    cnt_accounts_make_transactions int
);

CREATE PROJECTION ST23052707__DWH.global_metrics /*+createtype(L)*/ 
(
 date_update,
 currency_from,
 amount_total,
 cnt_transactions,
 avg_transactions_per_account,
 cnt_accounts_make_transactions
)
AS
 SELECT global_metrics.date_update,
        global_metrics.currency_from,
        global_metrics.amount_total,
        global_metrics.cnt_transactions,
        global_metrics.avg_transactions_per_account,
        global_metrics.cnt_accounts_make_transactions
 FROM ST23052707__DWH.global_metrics
 ORDER BY global_metrics.date_update,
          global_metrics.currency_from,
          global_metrics.amount_total,
          global_metrics.cnt_transactions,
          global_metrics.avg_transactions_per_account,
          global_metrics.cnt_accounts_make_transactions
SEGMENTED BY hash(global_metrics.date_update, global_metrics.currency_from, global_metrics.amount_total, global_metrics.cnt_transactions, global_metrics.avg_transactions_per_account, global_metrics.cnt_accounts_make_transactions) ALL NODES KSAFE 1;


SELECT MARK_DESIGN_KSAFE(1);

-- Запрос для формирования витрины
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


--____________METABASE____________ 	
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


