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

-- show
select * from table63;
