/*
Creating the table and loading the dataset
*/

DROP TABLE IF EXISTS ratings;
CREATE TABLE ratings (userid INT, temp1 VARCHAR(10),  movieid INT , temp3 VARCHAR(10),  rating REAL, temp5 VARCHAR(10), timestamp INT);
COPY ratings FROM 'test_data1.txt' DELIMITER ':';
ALTER TABLE ratings DROP COLUMN temp1, DROP COLUMN temp3, DROP COLUMN temp5, DROP COLUMN timestamp;

-- Do not change the above code except the path to the dataset.
-- make sure to change the path back to default provided path before you submit it.

-- Part A
/* Write the queries for Part A*/

--Query 1 -- select query
SELECT * from ratings WHERE movieid > 100 and movieid < 200 and rating > 3.5;

--Query 2 -- count query
SELECT COUNT(rating) from ratings where movieid > 100;

--Query 3 -- minimum query
SELECT MIN(rating) FROM ratings;

--Query 4 -- max of records
SELECT MAX(rating) FROM ratings;

--Query 5 -- avg of records
SELECT AVG(rating) FROM ratings WHERE movieid>400;

-- Part B
/* Create the fragmentations for Part B1 */
-- drop the table if already created
DROP TABLE IF EXISTS fragment1_1;
DROP TABLE IF EXISTS fragment1_2;
DROP TABLE IF EXISTS fragment1_3;
DROP TABLE IF EXISTS fragment1_union;

CREATE TABLE fragment1_1 AS(SELECT * FROM ratings where rating < 3.5);
CREATE TABLE fragment1_2 AS(SELECT * FROM ratings where rating > 3.5);
CREATE TABLE fragment1_3 AS(SELECT * FROM ratings where rating > 2.5 AND rating < 4.5);

/* Write reconstruction query/queries for Part B1 */
SELECT * INTO fragment1_union FROM fragment1_1 UNION SELECT * FROM fragment1_2 UNION SELECT * FROM fragment1_3;

/* Write your explanation as a comment */

/*
Fragment Queries:
Fragment 1: Contains tuples with ratings between 0 and 3.0
Fragment 2: Contains tuples with ratings between 4.0 and 5.0
Fragment 3: Contains tuples with ratings between 3.0 and 4.0

The above queries satisfy Reconstruction and Completeness and do not satisfy Disjointness in the following way:
1. Completeness : No data loss is found. Each and every tuple/record is present in atleast one fragment. 
				  As the fragments are created on the criteria of rating which span over the entire range (fragment created in the range of [0,5]), 
				  complete data is captured by the fragments.

2. Reconstruction : When we reconstruct the table from fragments fragment1_1, fragment1_2, fragment1_3 we get a complete table-fragment1_union 
					with perfectly same number of rows as the original database (test_data1)

3. Disjointness : On performing UNION ALL on all fragments, we will notice that a lot of rows/tuples are repeated.
				  SELECT * FROM fragment1_union;
				  Hence Disjointness is not satisfied.
*/

/* Create the fragmentations for Part B2 */
DROP TABLE IF EXISTS fragment2_1;
DROP TABLE IF EXISTS fragment2_2;
DROP TABLE IF EXISTS fragment2_3;

SELECT userid INTO fragment2_1 FROM ratings;
SELECT movieid INTO fragment2_2 FROM ratings;
SELECT rating INTO fragment2_3 FROM ratings;

/* Write your explanation as a comment */
/*
Fragment Queries:
Fragment 1: Contains userid column from ratings table.
Fragment 2: Contains movieid column from ratings table.
Fragment 3: Contains rating column from ratings table.

The above queries satisfy Completeness, Disjointness and do not satisfy Reconstruction in the following way:
1. Completeness : ratings table contains 3 columns: userid, movieid and rating. 
				  These are encapsulated in 3 fragments: fragment2_1, fragment2_2 and fragment2_3 respectively. 
				  If these fragments were to be joined, we will get the original ratings table.

2. Reconstruction : As there is not foreign key amongst the fragments, there is no possible manner to construct the original table.
					Hence Reconstruction is not satisfied.

3. Disjointness : If the 3 fragments were to be combined, we will obtain the original ratings table without any repetition as no columns have been repeated. 

*/

/* Create the fragmentations for Part B3 */
DROP TABLE IF EXISTS f1;
DROP TABLE IF EXISTS f2;
DROP TABLE IF EXISTS f3;
DROP TABLE IF EXISTS funion;

CREATE TABLE f1 AS(SELECT * FROM ratings where rating <= 2.5);
CREATE TABLE f2 AS(SELECT * FROM ratings where rating > 4.0);
CREATE TABLE f3 AS(SELECT * FROM ratings where rating >= 3.0 AND rating <= 4.0);

/* Write reconstruction query/queries for Part B3 */
SELECT * INTO funion FROM f1 UNION SELECT * FROM f2 UNION SELECT * FROM f3;

/* Write your explanation as a comment */

/*
Fragment Queries:
Fragment 1: Contains tuples with ratings between 0 and 2.5 both inclusive
Fragment 2: Contains tuples with ratings between 4.5 and 5.0 both inclusive
Fragment 3: Contains tuples with ratings between 3.0 and 4.0 both inclusive

The above queries satisfy Reconstruction, Completeness and Disjointness in the following way:
1. Completeness : No data loss is found. Each and every tuple/record is present in atleast one fragment. 
				  As the fragments are created on the criteria of rating which span over the entire range (fragment created in the range of [0,5]), 
				  complete data is captured by the fragments.

2. Reconstruction : When we reconstruct the table from fragments f1, f2, f3 we get a complete table-fragment1_union 
					with perfectly same number of rows as the original database (test_data1)

3. Disjointness : On performing UNION ALL on all fragments, we will notice that none of rows/tuples are repeated.
				  SELECT * FROM funion;
*/

-- Part C
/* Write the queries for Part C */

-- for f1
--Query 1 -- select query
SELECT * from f1 WHERE movieid > 100 and movieid < 200 and rating > 3.5;

--Query 2 -- count query
SELECT COUNT(rating) from f1 where movieid > 100;

--Query 3 -- minimum query
SELECT MIN(rating) FROM f1;

--Query 4 -- max of records
SELECT MAX(rating) FROM f1;

--Query 5 -- avg of records
SELECT AVG(rating) FROM f1 WHERE movieid>400;

-- for f2
--Query 1 -- select query
SELECT * from f2 WHERE movieid > 100 and movieid < 200 and rating > 3.5;

--Query 2 -- count query
SELECT COUNT(rating) from f2 where movieid > 100;

--Query 3 -- minimum query
SELECT MIN(rating) FROM f2;

--Query 4 -- max of records
SELECT MAX(rating) FROM f2;

--Query 5 -- avg of records
SELECT AVG(rating) FROM f2 WHERE movieid>400;

-- for f3
--Query 1 -- select query
SELECT * from f3 WHERE movieid > 100 and movieid < 200 and rating > 3.5;

--Query 2 -- count query
SELECT COUNT(rating) from f3 where movieid > 100;

--Query 3 -- minimum query
SELECT MIN(rating) FROM f3;

--Query 4 -- max of records
SELECT MAX(rating) FROM f3;

--Query 5 -- avg of records
SELECT AVG(rating) FROM f3 WHERE movieid>400;