#1)Создаем Датабазы и таблицы а также обрабаьываем их для комфортного анализа
CREATE DATABASE Project_SQL;
DROP TABLE transactions_info;
CREATE TABLE transactions_info (
    date_new VARCHAR(10),
    ID_check INT,
    ID_client INT,
    Count_products INT,
    Sum_payment DECIMAL(10, 2)
);
SHOW VARIABLES LIKE 'local_infile';
SET GLOBAL local_infile = 1;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/transactions_info.csv'
INTO TABLE transactions_info
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT * FROM transactions_info LIMIT 10;
#1.1) Дата импортировалась не правильно и поэтому ее надо привести в нормальную форму: гггг-мм-дд
UPDATE transactions_info
SET date_new = STR_TO_DATE(date_new, '%d/%m/%Y');

ALTER TABLE transactions_info
MODIFY date_new DATE;





CREATE TABLE customer_info (
    Id_client INT,
    Total_amount DECIMAL(10, 2),
    Gender VARCHAR(10),
    Age INT,
    Count_city INT,
    Response_communication BOOLEAN,
    Communication_3month BOOLEAN,
    Tenure INT
);
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/customer_info.csv'
INTO TABLE customer_info
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
    Id_client,
    Total_amount,
    Gender,
    @Age,
    Count_city,
    Response_communication,
    Communication_3month,
    Tenure
)
SET Age = NULLIF(@Age, '');

SELECT * FROM customer_info LIMIT 10;
SELECT COUNT(*) AS row_count
FROM customer_info
WHERE Age IS NOT NULL;
#1.2) Во второй таблице имеются NULL значения и поэтому я решил поменять их на медиану вощрастов всех
SELECT Age
FROM customer_info
WHERE Age IS NOT NULL
ORDER BY Age
LIMIT 1 OFFSET 1196;

SELECT Age
FROM customer_info
WHERE Age IS NOT NULL
ORDER BY Age
LIMIT 1 OFFSET 1197;

UPDATE customer_info
SET Age = 39
WHERE Age IS NULL;

SELECT COUNT(*) AS row_count
FROM customer_info
WHERE Age IS NULL;


#2. список клиентов с непрерывной историей за год, то есть каждый месяц на регулярной основе без пропусков за указанный годовой период, средний чек за период с 01.06.2015 по 01.06.2016, средняя сумма покупок за месяц, количество всех операций по клиенту за период;
SELECT * FROM transactions_info LIMIT 10;
#2.1) Сначала все по отдельности выведем а потом обьеденеим в один запрос

#2.2) Список клиентов с непрерывной историей за год
SELECT ID_client	FROM transactions_info
		WHERE date_new BETWEEN '2015-06-01' AND '2016-06-01'
			GROUP BY ID_client
			HAVING COUNT(DISTINCT DATE_FORMAT(date_new, '%Y-%m')) = 12;

#2.3) Средний чек за период
SELECT AVG(Sum_payment) AS avg_check FROM transactions_info
		WHERE date_new BETWEEN '2015-06-01' AND '2016-06-01';

#2.4) Средняя сумма покупок за месяц
SELECT ID_client, AVG(monthly_sum) AS avg_monthly_sum FROM (
    SELECT ID_client,
           DATE_FORMAT(date_new, '%Y-%m') AS month,
           SUM(Sum_payment) AS monthly_sum
    FROM transactions_info
    WHERE date_new BETWEEN '2015-06-01' AND '2016-06-01'
    GROUP BY ID_client, month
) AS monthly_data
GROUP BY ID_client;

#2.5) Количество всех операций по клиенту за период
SELECT ID_client, COUNT(*) AS total_operations FROM transactions_info
	WHERE date_new BETWEEN '2015-06-01' AND '2016-06-01'
		GROUP BY ID_client;

#2.6) Обьединение всех запросов в одну
SELECT t1.ID_client,
       t2.avg_check,
       t3.avg_monthly_sum,
       t4.total_operations
FROM (
    SELECT ID_client
    FROM transactions_info
    WHERE date_new BETWEEN '2015-06-01' AND '2016-06-01'
    GROUP BY ID_client
    HAVING COUNT(DISTINCT DATE_FORMAT(date_new, '%Y-%m')) = 12
) AS t1 -- 2.2)
LEFT JOIN (
    SELECT AVG(Sum_payment) AS avg_check
    FROM transactions_info
    WHERE date_new BETWEEN '2015-06-01' AND '2016-06-01'
) AS t2 ON 1=1 -- 2.3) В гугле поискал как соеденить правильно запросы так как выходили ошибки и нашел способо 1 = 1
LEFT JOIN (
    SELECT ID_client, 
           AVG(monthly_sum) AS avg_monthly_sum
    FROM (
        SELECT ID_client,
               DATE_FORMAT(date_new, '%Y-%m') AS month,
               SUM(Sum_payment) AS monthly_sum
        FROM transactions_info
        WHERE date_new BETWEEN '2015-06-01' AND '2016-06-01'
        GROUP BY ID_client, month
    ) AS monthly_data -- 2.4)
    GROUP BY ID_client
) AS t3 ON t1.ID_client = t3.ID_client
LEFT JOIN (
    SELECT ID_client, COUNT(*) AS total_operations
    FROM transactions_info
    WHERE date_new BETWEEN '2015-06-01' AND '2016-06-01'
    GROUP BY ID_client
) AS t4 ON t1.ID_client = t4.ID_client;  -- 2.5)

# 3. Вывести информацию в разрезе месяцев:
# a) Средняя сумма чека в месяц
SELECT YEAR(date_new) AS year, MONTH(date_new) AS month, AVG(Sum_payment) AS avg_chec FROM transactions_info
		GROUP BY year, month
		ORDER BY year, month;


# b) Среднее количество операций в месяц
SELECT 
    DATE_FORMAT(date_new, '%Y-%m') AS month,
    COUNT(*) AS total_operations,
    COUNT(*) / COUNT(DISTINCT ID_client) AS avg_operations_per_month
FROM transactions_info
GROUP BY month
ORDER BY month;

# c) Среднее количество клиентов, которые совершали операции
SELECT 
    DATE_FORMAT(date_new, '%Y-%m') AS month,
    COUNT(DISTINCT ID_client) AS avg_clients_per_month
FROM transactions_info
GROUP BY month
ORDER BY month;

# d) Доля от общего количества операций за год и доля в месяц от общей суммы операций
SELECT 
    DATE_FORMAT(date_new, '%Y-%m') AS month,
    COUNT(*) AS monthly_operations,
    COUNT(*) / (SELECT COUNT(*) FROM transactions_info) * 100 AS operations_share_percentage
FROM transactions_info
GROUP BY month
ORDER BY month;
# e) % соотношение M/F/NA в каждом месяце с их долей затрат
SELECT 
    DATE_FORMAT(date_new, '%Y-%m') AS month,
    Gender,
    COUNT(*) AS operations_count,
    COUNT(*) / SUM(COUNT(*)) OVER(PARTITION BY DATE_FORMAT(date_new, '%Y-%m')) * 100 AS operations_share_percentage
FROM transactions_info t
JOIN customer_info c ON t.ID_client = c.Id_client
GROUP BY month, Gender
ORDER BY month, Gender;

# 4. возрастные группы клиентов с шагом 10 лет и отдельно клиентов, у которых нет данной информации, с параметрами сумма и количество операций за весь период, и поквартально - средние показатели и %.
# 4.1) По возрастным группам
SELECT 
    CASE 
        WHEN Age BETWEEN 0 AND 9 THEN '0-9'
        WHEN Age BETWEEN 10 AND 19 THEN '10-19'
        WHEN Age BETWEEN 20 AND 29 THEN '20-29'
        WHEN Age BETWEEN 30 AND 39 THEN '30-39'
        WHEN Age BETWEEN 40 AND 49 THEN '40-49'
        WHEN Age BETWEEN 50 AND 59 THEN '50-59'
        WHEN Age BETWEEN 60 AND 69 THEN '60-69'
        WHEN Age >= 70 THEN '70+'
    END AS age_group,
    COUNT(*) AS transaction_count,
    SUM(Sum_payment) AS total_spending
FROM transactions_info t
JOIN customer_info c ON t.ID_client = c.Id_client
GROUP BY age_group
ORDER BY age_group;
# 4.2) Поквартальный анализ суммы и количества операций
SELECT 
    CONCAT(YEAR(t.date_new), '-Q', QUARTER(t.date_new)) AS quarter,
    CASE
        WHEN Age BETWEEN 0 AND 9 THEN '0-9'
        WHEN Age BETWEEN 10 AND 19 THEN '10-19'
        WHEN Age BETWEEN 20 AND 29 THEN '20-29'
        WHEN Age BETWEEN 30 AND 39 THEN '30-39'
        WHEN Age BETWEEN 40 AND 49 THEN '40-49'
        WHEN Age BETWEEN 50 AND 59 THEN '50-59'
        WHEN Age BETWEEN 60 AND 69 THEN '60-69'
        WHEN Age >= 70 THEN '70+'
    END AS age_group,
    COUNT(*) AS transaction_count,
    SUM(Sum_payment) AS total_spending,
    AVG(Sum_payment) AS avg_spending
FROM transactions_info t
JOIN customer_info c ON t.ID_client = c.Id_client
GROUP BY quarter, age_group
ORDER BY quarter, age_group;
