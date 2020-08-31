-- Подготовка датасета
-- Создание таблиц

CREATE DATABASE IF NOT EXISTS karablinov;
     
create external table karablinov.kiva_loans 
(
    id INT
    , funded_amount FLOAT
    , loan_amount FLOAT
    , activity STRING
    , sector STRING
    , use STRING
    , country_code STRING
    , country STRING
    , region STRING
    , currency STRING
    , partner_id STRING
    , posted_time STRING
    , disbursed_time STRING
    , funded_time STRING
    , term_in_months FLOAT
    , lender_count INT
    , tags STRING
    , borrower_genders STRING
    , repayment_interval STRING
    , dt DATE
)       
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\"" )
LOCATION '/user/hive/warehouse/kiva_loans/'
tblproperties ("skip.header.line.count"="1")

--

create external table karablinov.kiva_mpi_region_locations 
(
      LocationName STRING
    , ISO STRING
    , country STRING
    , region STRING
    , world_region STRING
    , MPI FLOAT
    , geo STRING
    , lat FLOAT
    , lon FLOAT
)       
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\"")
LOCATION '/user/hive/warehouse/kiva_mpi_region_locations/'
tblproperties ("skip.header.line.count"="1");



-- 1. Разведочный анализ
-- число строк
select count(*)
from karablinov.kiva_loans
--  672461

-- max, min partition
select distinct dt
from karablinov.kiva_loans
order by dt

--NULLs in loan_amount
select count(*)
from karablinov.kiva_loans
where loan_amount is NULL or loan_amount == ''

-- 2. Пользователи Kiva 

-- Количество займов в разрезе стран

select country, count(*)
from karablinov.kiva_loans
group by country;

/*
Armenia 8627
8   Azerbaijan  1945
9   Bhutan  2
10  Burkina Faso    2452
11  Burundi 865
12  Cambodia    34835
*/

-- Количество займов в разрезе макрорегионов 
-- (Для этого воспользуйтесь файлом kiva_mpi_region_locations - JOIN)
select b.world_region, count(*) as cnt
from karablinov.kiva_loans as a
left join (select distinct world_region, country from kiva_mpi_region_locations) as b
on a.country = b.country
group by b.world_region;

/*
1   Arab States 9186
2   Europe and Central Asia 38811
3   Latin America and Caribbean 133295
4   South Asia  38812
5   Sub-Saharan Africa  162666
6   NULL    82774
7   East Asia and the Pacific   206917
*/

-- Люди какого пола обращаются за финансированием
Select count_of_genders, count(1)
from (select explode(split(borrower_genders,', '))  as count_of_genders
from karablinov.kiva_loans) t
group by count_of_genders;

/*
female  1068745
male    274399
*/

-- В каких странах подавляющее большинство заемщиков - женщины?

CREATE TABLE karablinov.genders as 
SELECT 
id, 
country,
region,
loan_amount,
CASE
WHEN borrower_genders like "female%" THEN "female"
WHEN borrower_genders like "male%" THEN "male"
ELSE "other" END as pure_gender 
from karablinov.kiva_loans;

CREATE TABLE karablinov.genders2 as
SELECT
country,
CASE WHEN pure_gender == "female" THEN 1
ELSE 0 END as col
from karablinov.genders;

SELECT country, avg(col) as agg FROM genders2
GROUP BY country
ORDER BY agg desc;
/*
    country     agg
1   Cote D'Ivoire   1
2   Virgin Islands  1
3   Afghanistan 1
4   Guam    1
5   Solomon Islands 1
6   Turkey  0.998825601879037
7   Nepal   0.9916317991631799
8   Samoa   0.987557479037057
9   India   0.9782840868636525
10  Pakistan    0.9617203500279278
*/

/* 3. Объем финансирования */
-- Задача: проанализировать финансирование в разных секторах и видах деятельности

-- * Объем займов в разрезе секторов
-- * Средние и медианные значения суммы займа по секторам


SELECT
sector,
sum(loan_amount) as sum_,
avg(loan_amount) as avg_,
percentile(cast(loan_amount as BIGINT), 0.5) as median_
FROM sinchenko.kiva_loans
GROUP BY sector
ORDER BY sum_ DESC;

/*
Agriculture   143067875   793.4902275071823   500
2   Food    121606150   889.8640391637457   450
3   Retail  98122900    788.1737272478995   425
4   Services    48057450    1064.6311475409836  550
5   Clothing    37300925    1139.2378290880215  600 
*/

-- Суммы займов по макрорегионам

SELECT
t2.world_region,
sum(t1.loan_amount) as sum_
FROM
karablinov.kiva_loans as t1
INNER JOIN karablinov.kiva_mpi_region_locations as t2
ON t1.region = t2.region
GROUP BY t2.world_region
ORDER BY sum_ DESC;

/*
Europe and Central Asia 946618900
3   Arab States 315708025
4   Latin America and Caribbean 271793350
5   East Asia and the Pacific   163321225
6   South Asia  157590575
7   Sub-Saharan Africa  88539700
*/






