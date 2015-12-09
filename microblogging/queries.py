# -*- coding: utf-8 -*-
import os
import json
from pymongo import MongoClient
from bson.code import Code
from bson.son import SON

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
#     {"$unwind": "$id_member"},
#     {"$group": {"_id": "$id_member", "count": {"$sum": 1}}},
#     {"$sort": SON([("count", -1), ("_id", -1)])}
# ]
#
# top_users = list(db.things.aggregate(pipeline));

# question 5

# mapper = Code("""
#        function () {
#            emit(this.id, this.text.length);
#        }
#        """)
# # The reduce function sums over all of the emitted values for a given key:
#
# reducer = Code("""
#             function(k, v) {
#                 var i, sum = 0;
#                 for (i in v) {
#                     sum += v[i];
#                 }
#             return sum;
#             }
#         """)
#

LEVEL = os.path.abspath(os.path.dirname(__file__))
print(LEVEL)

# mapper = Code(open(os.path.join(LEVEL, 'mapper.js'), 'r').read())
# reducer = Code(open(os.path.join(LEVEL, 'reducer.js'), 'r').read())

#
# mapper = Code("""
#    function() {
#     text = this.text;
#     if (text) {
#         arrWords = text.toString().toLowerCase().replace(/[^a-zA-Z0-9]+/g, " ").split(" ");
#         for (var i = arrWords.length - 1; i >= 0; i--) {
#             word = arrWords[i].trim();
#             if (word) {
#                 emit(word, 1);
#             }
#         }
#     }
# }
# """)
#
# reducer = Code("""
#     function( key, values ) {
#         return Array.sum(values);
#     };
# """)
#
# unigram_results = tweets.map_reduce(mapper, reducer, 'mapRedResults');

# mapper = Code("""
#    function() {
#     text = this.text.toString();
#     if (text) {
#         countOfHashtags = (text.match(/#/g) || []).length;
#         emit(this.id, countOfHashtags);
#     }
# }
# """)
#
# reducer = Code("""
#     function( key, values ) {
#         return Array.sum(values);
#     };
# """)
#
# no_of_hashtags_results = tweets.map_reduce(mapper, reducer, 'countsOfHashtags')

# pipeline = [
#     {"$group":
#          {"_id": "null", "avgHashTags":
#              {
#                  "$avg": "$value"
#              }
#           }
#      },
# ]
#
# average_hashtags = list(db.countsOfHashtags.aggregate(pipeline));
# print average_hashtags[0]['avgHashTags']