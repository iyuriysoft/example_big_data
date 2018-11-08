
-- 5.1
DROP TABLE IF EXISTS table51;
CREATE TABLE table51 AS
SELECT category, COUNT(*) as cnt FROM product GROUP BY category ORDER BY cnt DESC LIMIT 10;

-- 5.2
DROP TABLE IF EXISTS table52;
CREATE TABLE table52 AS
SELECT tp.name, tp.category, count(*) as cnt FROM product tp inner join
(select category, count(*) as c from product group by category order by c desc) tcat
ON tp.category = tcat.category
GROUP BY tp.name, tp.category ORDER BY cnt DESC LIMIT 10;

-- show
select * from table51;
select * from table52;
