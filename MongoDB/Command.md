mongoimport --db assignment --collection ProductReview --type json --file '/home/mdm77/Downloads/Electronics_5.json' 

mongosh

show dbs

use assignment

assignment> show collections
>> ProductReview

assignment> db.ProductReview.find({})
1689188

Part 1:

assignment> db.ProductReview.find({$or: [{"overall": 1.0}, {"overall": 3.0}, {"overall": 5.0}]}, {_id: 0, "summary": 1, "overall": 1}).pretty()
1260008

assignment> db.ProductReview.find({$and: [{"overall": {$gt: 1.0}}, {"overall": {$lt: 4.0}}]}, {_id: 0, "reviewerName": 1, "overall": 1}).sort({"reviewerName": 1}).pretty()
224396

Part 2:

db.ProductReview.find({"reviewText": {$regex: "awesome"}})
38135

assignment> db.ProductReview.find({"summary": {$regex: "[^a-zA-Z\d\s:]"}}, {_id:0, summary: 1})
1608085

assignment> db.ProductReview.find({$and: [{"overall": {$nin: [2.0, 4.0]}}]}, {_id: 0, "summary": 1, "overall": 1}).pretty()
1260008

assignment> db.ProductReview.find({}, {_id: 0, "reviewerID": 1, "asin": 1, "reviewerName": 1}).sort({"reviewerID": 1}).limit(10).pretty()


PART 3:

assignment> db.ProductReview.aggregate([{$group: {_id: "$reviewerName", min_value: {$min: "$overall"}}}]).limit(10).pretty()

assignment> db.ProductReview.aggregate([{$group: {_id: "$reviewerName", min_value: {$min: "$overall"}}}, {$limit: 10}]).pretty()

assignment> db.ProductReview.aggregate([{ $group: { _id: "$helpful", total: { $sum: 1 } } }, { $sort: { 'total': -1 }}]).pretty()

assignment> db.collection.find({"unixReviewTime": {$gt: ISODate("2021-01-01")}}).count()

assignment> db.collection.find({"unixReviewTime": {$gt: 1252800000}}).count()

assignment> db.collection.find({"unixReviewTime": {$gt: new Date().getTime()/1000-3600}})

db.collection.aggregate([
  {
    $project: { ts: { $toDate: { $multiply: ['$unix_timestamp', 1000] } } }
  }
]).pretty();
