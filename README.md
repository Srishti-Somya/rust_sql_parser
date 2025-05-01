# rust_sql_parser
A Rust-based SQL query parser 
Run the following, after cloning:
- cargo build
- cargo run
- enter sql statements, for example the below implementation (ignore the warnings😬)
- give all the input( numbers, string literals) in single quotes ('')
![execution](image-1.png)

<br />
update : implemented little: 
SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, ALTER TABLE(ADD, drop, modify), DROP TABLE, ORDER BY,  GROUP BY, JOIN (INNER, LEFT, RIGHT, FULL, CROSS) , HAVING, Aggregate Functions (SUM, COUNT, AVG, MIN, MAX)
<br />
Next to do:
LIMIT/OFFSET,  DISTINCT keyword in SELECT
<br />
Test : <br />
CREATE TABLE users (id INT, name TEXT, age INT);<br />

ALTER TABLE users ADD email;<br />
ALTER TABLE users DROP email;<br />
ALTER TABLE users MODIFY age TEXT; <br />
ALTER TABLE users MODIFY age INT;<br />
<br />
INSERT INTO users (id, name, age) VALUES ('1', 'srishti', '30');<br />
INSERT INTO users (id, name, age) VALUES ('2', 'srijan', '25');<br />
INSERT INTO users (id, name, age) VALUES ('3', 'tanish’, '22');<br />
<br />
SELECT * FROM users;<br />
SELECT id, name FROM users;<br />
SELECT * FROM users WHERE name = 'Srishti';<br />
SELECT * FROM users WHERE age = '22';<br />
SELECT * FROM users ORDER BY age DESC;<br />
SELECT age, COUNT(*) FROM users GROUP BY age;<br />
SELECT age, COUNT(*) FROM users GROUP BY age HAVING COUNT(*) > 1;<br />
SELECT age, COUNT(*) FROM users GROUP BY age HAVING COUNT(*) = 2;<br />
SELECT COUNT(*) FROM users;<br />
SELECT COUNT(age) FROM users;<br />
SELECT SUM(age) FROM users;<br />
SELECT AVG(age) FROM users;<br />
SELECT MAX(age) FROM users;<br />
SELECT MIN(age) FROM users;<br />
<br />
UPDATE users SET age = '40' WHERE name = 'Srijan';<br />
<br />
CREATE TABLE orders (id INT, user_id INT, total INT);<br />
<br />
INSERT INTO orders (id, user_id, total) VALUES ('101', '1', '200');<br />
INSERT INTO orders (id, user_id, total) VALUES ('102', '1', '150');<br />
INSERT INTO orders (id, user_id, total) VALUES ('103', '2', '300');<br />
INSERT INTO orders (id, user_id, total) VALUES ('104', '4', '100');<br />
<br />
-- INNER JOIN<br />
SELECT users.name, orders.total FROM users JOIN orders ON users.id = orders.user_id;<br />
<br />
-- LEFT JOIN<br />
SELECT users.name, orders.total FROM users LEFT JOIN orders ON users.id = orders.user_id;<br />
<br />
-- RIGHT JOIN<br />
SELECT users.name, orders.total FROM users RIGHT JOIN orders ON users.id = orders.user_id;<br />
<br />
-- FULL JOIN<br />
SELECT users.name, orders.total FROM users FULL JOIN orders ON users.id = orders.user_id;<br />
<br />
– cross join<br />
Select * from users cross join orders;<br />
<br />
DELETE FROM users WHERE name = 'Srishti';<br />
DELETE FROM users WHERE age = '20';<br />
DROP TABLE users;<br />


