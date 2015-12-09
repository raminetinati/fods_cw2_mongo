# -*- coding: utf-8 -*-

from __future__ import division


import os
import json
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')

db = client['microblogging']
tweets = db['tweets']

# ● id - a unique identifier of the tuple
# ● id_member - a unique identifier of the user who posted the message
# ● timestamp - a UTC timestamp of when the message was published
# ● text - the microblog message that was published
# ● geo_lat - the latitude coordinate of where the message was posted from
# ● geo_lng - the longitude coordinate of where the message was posted from.

# distinct_users = tweets.distinct('id_member')
# distinct_users_number = len(distinct_users)
#
# print(distinct_users_number)

# pipeline = [
#     {"$group": {"_id": "$id_member", "count": {"$sum": 1}}},
#     {"$sort": {"count": -1}},
#     {"$limit": 10}
# ]
#
# top_users = list(tweets.aggregate(pipeline));
# sum_of_tweets = 0
# for user in top_users:
#     sum_of_tweets += user['count']
#
tweets_number = tweets.count()
#
# percentage = 100*sum_of_tweets/tweets_number
# print(percentage)

# earliest_date = tweets.find({}, {"timestamp":1}).sort("timestamp", 1).limit(1)
# latest_date = tweets.find({}, {"timestamp":1}).sort("timestamp", -1).limit(1)
#
# print(earliest_date.next()['timestamp'])
# print(latest_date.next()['timestamp'])
#

all_tweets = tweets.find()

total_text_size = 0
for tweet in all_tweets:
    if 'text' in tweet:
        total_text_size += len(str(tweet['text']))

average_length = total_text_size/tweets_number
print(average_length)