-- Task 1.1
amazon_reviews = LOAD '/home/mdm77/Code/PIG/amazon_reviews_us_Camera_v1_00.tsv' USING PigStorage('\t') AS (marketplace: chararray, customer_id: long, review_id: chararray, product_id: chararray, product_parent: chararray, product_title: chararray, product_category: chararray, star_rating: int, helpful_votes: int, total_votes: int, vine: chararray, verified_purchase: chararray, review_headline: chararray, review_body: chararray, review_date: Datetime);

-- Task 1.2
grouped_reviews = GROUP amazon_reviews BY (marketplace, product_category);

result_P1_1 = FOREACH grouped_reviews GENERATE FLATTEN(group), COUNT(amazon_reviews.review_id) AS total_number_of_reviews, AVG(amazon_reviews.star_rating) AS average_rating;

DUMP result_P1_1;

STORE result_P1_1 INTO '/home/mdm77/Code/PIG/output/task_1_1' USING PigStorage();

-- Task 1.2.1

filtered_market_place = FILTER amazon_reviews BY marketplace != 'US';

grouped_reviews = GROUP filtered_market_place BY (marketplace, product_category);

result_P1_2 = FOREACH grouped_reviews GENERATE FLATTEN(group), COUNT(filtered_market_place.review_id) AS total_number_of_reviews, AVG(filtered_market_place.star_rating) AS average_rating;

DUMP  result_P1_2;

STORE result_P1_2 INTO '/home/mdm77/Code/PIG/output/task_1_2' USING PigStorage();

-- Task 2.1

grouped_review_date = GROUP amazon_reviews BY review_date;

result_P2_1 = FOREACH grouped_review_date GENERATE FLATTEN(group), COUNT(amazon_reviews.review_id);

DUMP result_P2_1;

STORE result_P2_1 INTO '/home/mdm77/Code/PIG/output/task_2_1' USING PigStorage();

-- Task 2.2

grouped_product_id = GROUP amazon_reviews BY product_id;

result_P2_2_1 = FOREACH grouped_product_id GENERATE FLATTEN(group), AVG(amazon_reviews.helpful_votes), AVG(amazon_reviews.total_votes) AS average_total_votes;

DUMP result_P2_2_1;

STORE result_P2_2_1 INTO '/home/mdm77/Code/PIG/output/task_2_2_1' USING PigStorage();

-- Task 2.2.1

sorted_result_P2_2_1 = ORDER result_P2_2_1 BY average_total_votes DESC;

result_p2_2_2 = LIMIT sorted_result_P2_2_1 10;

DUMP result_p2_2_2;

STORE result_p2_2_2 INTO '/home/mdm77/Code/PIG/output/task_2_2_2' USING PigStorage();

-- Task 3.1

filtered_verified_purchase = FILTER amazon_reviews BY verified_purchase == 'Y';

-- Task 3.1.1

grouped_reviews = GROUP filtered_verified_purchase BY product_category;

result_p3_1 = FOREACH grouped_reviews GENERATE FLATTEN(group), COUNT(filtered_verified_purchase.product_id);

DUMP result_p3_1;

STORE result_p3_1 INTO '/home/mdm77/Code/PIG/output/task_3_1' USING PigStorage();

-- Task 3.1.2

grouped_reviews = GROUP filtered_verified_purchase BY star_rating;

result_p3_2 = FOREACH grouped_reviews GENERATE FLATTEN(group), SUM(filtered_verified_purchase.helpful_votes) , SUM(filtered_verified_purchase.total_votes),  ((double)SUM(filtered_verified_purchase.helpful_votes) /  SUM(filtered_verified_purchase.total_votes)) as average_helpful_votes;

DUMP result_p3_2;

STORE result_p3_2 INTO '/home/mdm77/Code/PIG/output/task_3_2' USING PigStorage();

-- Task 3.1.2.1

result_p3_2_1 = ORDER result_p3_2 BY average_helpful_votes;

DUMP result_p3_2_1;

STORE result_p3_2_1 INTO '/home/mdm77/Code/PIG/output/task_3_3' USING PigStorage();