# -*- coding: utf-8 -*-
import os

from pymongo import MongoClient
from bson.code import Code

client = MongoClient('mongodb://localhost:27017/')

db = client['microblogging']
tweets = db['data']

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
print LEVEL

mapper = Code(open(os.path.join(LEVEL,'mapper.js'), 'r').read())
reducer = Code(open(os.path.join(LEVEL,'reducer.js'), 'r').read())

#
# mapper = Code("""
#    function() {
#     getUnigrams = function (words) {
#         words = words.toString().split(" ").replace(/[.,]/g,"");
#         var arrNewWords = [];
#
#         for (var i = 0; i < arrWords.length; i++) {
#             var wordOccurence = arrWords[i];
#             if(typeof arrNewWords[wordOccurence] === 'undefined') {
#                 var wordObj = {wordOccurence: 1};
#                 arrUnigrams.push(wordObj);
#             } else {
#                 arrNewWords[arrWords[i]] = arrNewWords[wordOccurence]+1;
#             }
#         }
#         return arrNewWords;
#     };
#     var unigrams = getUnigrams(this.text);
#     emit(this.id, {unigrams: unigrams});
# }
# """)
#
# reducer = Code("""
#     function(key, unigramArrays){
#         var totalArray = [];
#
#         for (var i = 0; i < unigramArrays.length; i++) {
#             var arrayOfUnigramValues = unigramArrays[i];
#             for (var j = 0; j < arrayOfUnigramValues.length; i++) {
#                 var wordObject = arrayOfUnigramValues[j];
#                 var unigram = Object.keys(wordObject)[0];
#
#                 if(typeof totalArray[unigram] === 'undefined') {
#                     totalArray[unigram] = wordObject;
#                 } else {
#                     var objToIncrement = totalArray[unigram];
#                     objToIncrement[unigram] = objToIncrement[unigram]+1;
#                     totalArray[unigram] = objToIncrement;
#                 }
#             }
#         }
#
#         return { unigrams:totalArray};
#     };
# """)
try:
    res = tweets.map_reduce(mapper, reducer, 'unigramsCollection')
except Exception as ex:
    print ex.details

# result  = db.stories.mapReduce( map,
#           reduce,
#           {query:{author:{$exists:true},mr_status:"inprocess"},
#            out: {reduce:"authors_unigrams"}
#           });



# res = tweets.map_reduce(mapper, reducer, 'mapRedResults');
# total_sum = db["mapRedResults"].find().limit(1)
# for aoua in total_sum:
#     print aoua
# for item in result.find():
#     print(item)