
-- should be present in HDFS:
--
-- hdfs:///user/cloudera/product/
-- hdfs:///user/cloudera/CountryName/
-- hdfs:///user/cloudera/CountryIP/
-- hdfs:///user/cloudera/CountryIP.csv (used in udf)

-- in Local:
--
-- /home/cloudera/my/b-0.0.1.jar (udf)

-------------------------------------------------------------------------------
-- CREATE TABLES

--
-- table Product
--
drop table if exists product;
create external table product (
  name string,
  price float,
  dt1 timestamp,
  category string,
  ip string
)
PARTITIONED BY (dt string) 
row format delimited fields terminated by ','
LOCATION '/user/cloudera/product/';

--ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--WITH SERDEPROPERTIES (
--  "separatorChar" = ",",
--  "quoteChar"     = "`",
--  "escapeChar"    = "\\"
--)

--load data local inpath '/home/cloudera/input.txt' overwrite into table product;

--create table product partitioned by (dt date);
ALTER TABLE product ADD PARTITION (dt = '2010-10-15');
ALTER TABLE product ADD PARTITION (dt = '2010-10-16');
ALTER TABLE product ADD PARTITION (dt = '2010-10-17');
ALTER TABLE product ADD PARTITION (dt = '2010-10-18');
ALTER TABLE product ADD PARTITION (dt = '2010-10-19');
ALTER TABLE product ADD PARTITION (dt = '2010-10-20');
ALTER TABLE product ADD PARTITION (dt = '2010-10-21');



--
-- table CountryName
--

--49518,en,AF,Africa,RW,Rwanda,0

drop table if exists CountryName;
create external table CountryName (
  geoname_id int,
  locale_code string,
  continent_code string,
  continent_name string,
  country_iso_code string,
  country_name string,
  is_in_e boolean
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = ",",
  "quoteChar"     = "\"",
  "escapeChar"    = "\\"
)
LOCATION '/user/cloudera/CountryName/';


--
-- table CountryIP
--

--1.0.0.0/24,2077456,2077456,,0,0

drop table if exists CountryIP;
create external table CountryIP (
  network string,
  geoname_id int,
  registered_country_geoname_id int,
  represented_country_geoname_id int,
  is_anonymous_proxy boolean,
  is_satellite_provider boolean
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = ",",
  "quoteChar"     = "\"",
  "escapeChar"    = "\\"
)
LOCATION '/user/cloudera/CountryIP/';


select * from product limit 10;
select * from CountryName limit 10;
select * from CountryIP limit 10;


-------------------------------------------------------------------------------
-- TASKS 51, 52, 63


-- 5.1
DROP TABLE IF EXISTS table51;
CREATE TABLE table51 AS
SELECT category, COUNT(*) as cnt FROM product GROUP BY category ORDER BY cnt DESC LIMIT 10;

-- show decisions
select * from table51;

-- 5.2
DROP TABLE IF EXISTS table52;
CREATE TABLE table52 AS
SELECT tp.name, tp.category, count(*) as cnt FROM product tp inner join
(select category, count(*) as c from product group by category order by c desc) tcat
ON tp.category = tcat.category
GROUP BY tp.name, tp.category ORDER BY cnt DESC LIMIT 10;

-- show decisions
select * from table52;

-- 6.3
-- using UDF (change according your jar-name)
delete jar /home/cloudera/my/b-0.0.1.jar;
add jar /home/cloudera/my/b-0.0.1.jar;
create temporary function getGeoIdFromIP as 'a.udf.GetGeoIdFromIP';

DROP TABLE IF EXISTS tproduct;
CREATE TABLE IF NOT EXISTS tproduct AS 
select price, ip, GetGeoIdFromIP(ip, "hdfs:///user/cloudera/CountryIP.csv") as geo_id from product;

DROP TABLE IF EXISTS table63;
CREATE TABLE IF NOT EXISTS table63 AS 
select sum(price) su, geo_id, tcn.country_name
from tproduct left join countryname tcn on tcn.geoname_id = geo_id
where geo_id != 0
group by geo_id, tcn.country_name order by su desc limit 10;

-- show decisions
select * from table51;
select * from table52;
select * from table63;
