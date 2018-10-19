-- using UDF (change according your jar-name)
delete jar /home/cloudera/my/b-0.0.1.jar;
add jar /home/cloudera/my/b-0.0.1.jar;
create temporary function getStartIP as 'a.udf.GetStartIP';
create temporary function getEndIP as 'a.udf.GetEndIP';
create temporary function getIP as 'a.udf.GetIP';

-- 1 create view Product with ip as long
create view if not exists vproduct as 
select tb.ip, getIP(tb.ip) as iip, tb.p as sump
from (SELECT t.ip, sum(t.price) sump FROM product t group by t.ip order by sump desc limit 10) tb;

-- 2 create view CountryIP with startip as long and endip as long
create view if not exists vcountryip as 
select tb.geoname_id, getStartIP(tb.network) as startip, getEndIP(tb.network) as endip
from countryip tb;

-- 3 create extra view with link product - geoid
create view if not exists vproduct_geo as 
SELECT tp.sump, tp.ip, tc.geoname_id
FROM (select sump, ip, iip from vproduct) tp,
(select geoname_id, startip, endip from vcountryip) tc
WHERE tp.iip < tc.endip AND tp.iip > tc.startip;


-- 6.3 task: show country name
DROP TABLE IF EXISTS table63;
CREATE TABLE table63 AS
SELECT SUM(t.sump) as summ, count(*) as cnt, t.geoname_id, tcn.country_name
FROM vproduct_geo t
INNER JOIN countryname tcn ON t.geoname_id = tcn.geoname_id
GROUP BY t.geoname_id, tcn.country_name ORDER BY summ DESC LIMIT 10;
