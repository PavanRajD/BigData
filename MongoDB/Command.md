# Part 1:
## Task 1
>> mongoimport --db assignment --collection ProductReview --type json --file '/home/mdm77/Downloads/Electronics_5.json' 

>> mongosh

test> show dbs

test> use assignment

assignment> show collections
### op: ProductReview

## Task 2
assignment> db.ProductReview.find({$or: [{"overall": 1.0}, {"overall": 3.0}, {"overall": 5.0}]}, {_id: 0, "summary": 1, "overall": 1}).pretty()
### Output count: 1260008

## Task 3
assignment> db.ProductReview.find({$and: [{"overall": {$gt: 1.0}}, {"overall": {$lt: 4.0}}]}, {_id: 0, "reviewerName": 1, "overall": 1}).sort({"reviewerName": 1}).pretty()
### Output count: 224396

# Part 2:

## Task 1
db.ProductReview.find({"reviewText": {$regex: "awesome"}})
### Output count: 38135

## Task 2
assignment> db.ProductReview.find({"summary": {$regex: "[^a-zA-Z\d\s]"}}, {_id:0, summary: 1})
assignment>  db.reviews.find({summary:/[^A-Za-z\d\s]/})
### Output count: 1608085

## Task 3
assignment> db.ProductReview.find({"overall": {$nin:[2.0, 4.0]}}, {_id:0, "summary": 1, "overall": 1}).pretty()
### Output count: 1260008

## Task 4
assignment> db.ProductReview.find({}, {_id: 0, "reviewerID": 1, "asin": 1, "reviewerName": 1}).sort({"reviewerID": 1}).limit(10).pretty()

# PART 3:

## Task 1
assignment> db.ProductReview.aggregate([{$group: {_id: "$reviewerName", min_value: {$min: "$overall"}}}, {$limit: 10}]).pretty()

## Task 2
assignment> db.ProductReview.aggregate([{ $group: { _id: "$helpful", total_helpful: { $sum: 1 }}}, { $sort: { 'total_helpful': 1 }}]).pretty()

assignment> db.ProductReview.aggregate([{$unwind: "$helpful"}, {$group: {_id: "$helpful", total_helpful: {$sum: "$helpful" }}}, {$sort: { 'total_helpful': 1 }}]).pretty()
### Output count: 1305


## Task 3
assignment> db.ProductReview.find({"unixReviewTime": {$gte: 1252800000}}).count()

assignment> db.ProductReview.find({"unixReviewTime": {$gt: ISODate("2021-01-01")}}).count()

assignment> db.ProductReview.find({"unixReviewTime": {$gt: new Date().getTime()/1000-3600}})

