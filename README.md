# rust_sql_parser
A Rust-based SQL query parser 
Run the following, after cloning:
- cargo build
- cargo run
- enter sql statements, for example the below implementation (ignore the warnings😬)
- give all the input( numbers, string literals) in single quotes ('')
![execution](image-1.png)


update : implemented little: 
SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, ALTER TABLE(ADD, drop, modify), DROP TABLE, ORDER BY,  GROUP BY, JOIN (INNER, LEFT, RIGHT, FULL, CROSS) , HAVING, Aggregate Functions (SUM, COUNT, AVG, MIN, MAX)

Next to do:
LIMIT/OFFSET,  DISTINCT keyword in SELECT,

Test : 
CREATE TABLE users (id INT, name TEXT, age INT);

ALTER TABLE users ADD email;
ALTER TABLE users DROP email;
ALTER TABLE users MODIFY age TEXT; 
ALTER TABLE users MODIFY age INT;

INSERT INTO users (id, name, age) VALUES ('1', 'srishti', '30');
INSERT INTO users (id, name, age) VALUES ('2', 'srijan', '25');
INSERT INTO users (id, name, age) VALUES ('3', 'tanish’, '22');

SELECT * FROM users;
SELECT id, name FROM users;
SELECT * FROM users WHERE name = 'Srishti';
SELECT * FROM users WHERE age = '22';
SELECT * FROM users ORDER BY age DESC;
SELECT age, COUNT(*) FROM users GROUP BY age;
SELECT age, COUNT(*) FROM users GROUP BY age HAVING COUNT(*) > 1;
SELECT age, COUNT(*) FROM users GROUP BY age HAVING COUNT(*) = 2;
SELECT COUNT(*) FROM users;
SELECT COUNT(age) FROM users;
SELECT SUM(age) FROM users;
SELECT AVG(age) FROM users;
SELECT MAX(age) FROM users;
SELECT MIN(age) FROM users;

UPDATE users SET age = '40' WHERE name = 'Srijan';

CREATE TABLE orders (id INT, user_id INT, total INT);

INSERT INTO orders (id, user_id, total) VALUES ('101', '1', '200');
INSERT INTO orders (id, user_id, total) VALUES ('102', '1', '150');
INSERT INTO orders (id, user_id, total) VALUES ('103', '2', '300');
INSERT INTO orders (id, user_id, total) VALUES ('104', '4', '100');

-- INNER JOIN
SELECT users.name, orders.total FROM users JOIN orders ON users.id = orders.user_id;

-- LEFT JOIN
SELECT users.name, orders.total FROM users LEFT JOIN orders ON users.id = orders.user_id;

-- RIGHT JOIN
SELECT users.name, orders.total FROM users RIGHT JOIN orders ON users.id = orders.user_id;

-- FULL JOIN
SELECT users.name, orders.total FROM users FULL JOIN orders ON users.id = orders.user_id;

– cross join
Select * from users cross join orders;

DELETE FROM users WHERE name = 'Srishti';
DELETE FROM users WHERE age = '20';
DROP TABLE users;


