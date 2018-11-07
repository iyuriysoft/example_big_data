-- using UDF (change according your jar-name)
delete jar /home/cloudera/my/b-0.0.1.jar;
add jar /home/cloudera/my/b-0.0.1.jar;
create temporary function getGeoId as 'a.udf.GetGeoIdFromIP';

-- 6.3
DROP TABLE IF EXISTS table63;
CREATE TABLE table63 AS
SELECT SUM(tp.price) as summ, count(*) as cnt, tcn.geoname_id, tcn.country_name
FROM (SELECT price, GetGeoIdFromIP(ip, "CountryIP.csv") as geo_id FROM product) tp
INNER JOIN country_name tcn ON tp.geo_id = tcn.geoname_id
GROUP BY tcn.geoname_id, tcn.countryname ORDER BY summ DESC LIMIT 10
