hive

CREATE DATABASE assignment;

USE assignment;

# Part 1

## TASK 1

CREATE EXTERNAL TABLE CameraTable (marketplace string, customer_id string, review_id string, product_id string, product_parent string, product_title string, product_category string, star_rating int, helpful_votes int, total_votes int, vine string, verified_purchase string, review_headline string, review_body string, review_date date) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/home/mdm77/Code/PIG/amazon_reviews_us_Camera_v1_00.tsv' INTO TABLE CameraTable;

## TASK 2

SELECT COUNT(review_id), AVG(star_rating) FROM CameraTable WHERE marketplace != 'US' GROUP BY marketplace, product_category;

# Part 2

## TASK 1

Create Table CameraTablePart (marketplace string, customer_id string, review_id string, product_id string, product_parent string, product_title string, product_category string, helpful_votes int, total_votes int, vine string, verified_purchase string, review_headline string, review_body string, review_date date)
Partitioned By (star_rating int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

INSERT OVERWRITE TABLE CameraTablePart PARTITION (star_rating=4) SELECT marketplace,customer_id,review_id,product_id,product_parent,product_title,product_category,helpful_votes,total_votes,vine,verified_purchase,review_headline,review_body,review_date FROM CameraTable WHERE star_rating=4;

INSERT OVERWRITE TABLE CameraTablePart PARTITION (star_rating=2) SELECT marketplace,customer_id,review_id,product_id,product_parent,product_title,product_category,helpful_votes,total_votes,vine,verified_purchase,review_headline,review_body,review_date FROM CameraTable WHERE star_rating=2;

# TASK 2

SELECT SUM(helpful_votes), SUM(total_votes) AS SumOfTotalVotes FROM CameraTablePart GROUP BY star_rating ORDER BY SumOfTotalVotes DESC;

# Part 3

## TASK 1

set hive.enforce.bucketing = true;

CREATE TABLE CameraTableBuck(marketplace string, customer_id string, review_id string, product_id string, product_parent string, product_title string, product_category string, star_rating int, helpful_votes int, total_votes int, vine string, verified_purchase string, review_headline string, review_body string, review_date date) CLUSTERED BY (review_date) INTO 4 BUCKETS ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

INSERT OVERWRITE TABLE CameraTableBuck SELECT * FROM CameraTable;

SELECT MIN(review_date), MAX(review_date) FROM CameraTableBuck Tablesample(BUCKET 1 OUT OF 4 ON review_date);

SELECT MIN(review_date), MAX(review_date) FROM CameraTableBuck Tablesample(BUCKET 2 OUT OF 4 ON review_date);

SELECT MIN(review_date), MAX(review_date) FROM CameraTableBuck Tablesample(BUCKET 3 OUT OF 4 ON review_date);

SELECT MIN(review_date), MAX(review_date) FROM CameraTableBuck Tablesample(BUCKET 4 OUT OF 4 ON review_date);

## TASK 2

SELECT AVG(helpful_votes) AS AvgHelpFulVotes, AVG(total_votes) FROM CameraTable GROUP BY product_id HAVING AvgHelpFulVotes > 2;
