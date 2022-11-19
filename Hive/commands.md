hive

CREATE DATABASE assignment;

USE assignment;

CREATE TABLE CameraTable (
    marketplace string, customer_id bigint, review_id string, product_id string, product_parent string, 
    product_title string, product_category string, star_rating int, helpful_votes int, total_votes int, vine string, verified_purchase string, review_headline string, review_body string, review_date date
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

Load Data Local Inpath '/home/mdm77/Code/PIG/amazon_reviews_us_Camera_v1_00.tsv' Into Table CameraTable;


