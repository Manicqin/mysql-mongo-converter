from pymongo import MongoClient
import datetime

client = MongoClient()

db = client.test_database
collection = db.test_collection

post = {"id" : "0" , "keyword_id" : "0" , "ads_id" : 0 , "ipaddress" : "0.0.0.0" , "time_stamp" : datetime.datetime.utcnow() , "page" : "www.reimage.com" , "browser" : "firefox" , "geo" : "france" , "region" : "blabla" , "city" : "paris" , "browser_version" : "30" , "os" : "windows" , "refsrc" : "blabla" , "visitnum" : "10" , "download" : "1" , "cookie" : "1111" }

posts = db.posts
post_id = posts.insert(post)
print (post_id , db.collection_names())

