# -*- coding: utf-8 -*-

from __future__ import division
import calendar
import datetime
from pymongo import MongoClient
from bson.code import Code


class TweetApp(object):
    def __init__(self):
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client['microblogging']
        self.tweets = self.db['tweets']
        self.tweets_no = self.get_tweets_number()

    def get_tweets_number(self):
        return self.tweets.count()

    # Question 1
    def get_distinct_users(self):
        distinct_users = self.tweets.distinct('id_member')
        distinct_users_number = len(distinct_users)

        seq = ("\nThere are", str(distinct_users_number), "unique users.")
        return " ".join(seq)

    # Question 2
    def get_top_ten_users_tweet_percentage(self):
        pipeline = [
            {"$group": {"_id": "$id_member", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 10}
        ]

        top_users = list(self.tweets.aggregate(pipeline))
        sum_of_tweets = 0
        for user in top_users:
            sum_of_tweets += user['count']

        tweets_number = self.tweets_no
        percentage = 100 * sum_of_tweets / tweets_number

        seq = ("\nThe top 10 users published", str(percentage), "% of the self.tweets.")
        return " ".join(seq)

    def get_earliest_and_latest_date(self):
        earliest_date_cursor = self.tweets.find({}, {"timestamp": 1}).sort("timestamp", 1).limit(1)
        for item in earliest_date_cursor:
            earliest_date = item['timestamp']
        latest_date_cursor = self.tweets.find({}, {"timestamp": 1}).sort("timestamp", -1).limit(1)
        for item in latest_date_cursor:
            latest_date = item['timestamp']

        return [earliest_date, latest_date]

    # Question 3
    def get_earliest_and_latest_date_result(self):
        dates = self.get_earliest_and_latest_date()

        seq = ("\nThe earliest date is: ", dates[0], "and the latest date is: ", dates[1])
        return " ".join(seq)

    # Question 4
    def get_mean_time_deltas(self):
        dates = self.get_earliest_and_latest_date()
        earlier_date = datetime.datetime.strptime(dates[0], "%Y-%m-%d %H:%M:%S")
        latest_date = datetime.datetime.strptime(dates[1], "%Y-%m-%d %H:%M:%S")

        earlier_ts = calendar.timegm(earlier_date.utctimetuple())
        latest_ts = calendar.timegm(latest_date.utctimetuple())

        mean_td = (latest_ts - earlier_ts) / (self.get_tweets_number() - 1)

        seq = ("\nThe mean time delta between all messages is", str(mean_td))
        return " ".join(seq)

    # question 5

    def get_mean_length_of_msg(self):
        mapper = Code("""
           function() {
            text = this.text.toString();
            if (text) {
                emit(this.id, text.length);
            }
        }
        """)
        reducer = Code("""
            function( key, values ) {
                return Array.sum(values);
            };
        """)
        mean_length_of_msg = self.tweets.map_reduce(mapper, reducer, 'meanLengths')
        pipeline = [
            {"$group":
                 {"_id": "null", "meanLen":
                     {
                         "$avg": "$value"
                     }
                  }
             },
        ]

        mean_lgth = list(self.db.meanLengths.aggregate(pipeline))
        for item in mean_lgth:
            result = item['meanLen']

        seq = ("\nThe mean length of all the tweets is", str(result))
        return " ".join(seq)

    # Question 6_1
    def get_unigrams(self):
        mapper = Code("""
           function() {
            text = this.text;
            if (text) {
                var removePunc = text.toString().toLowerCase().replace(/[.,-\/#!$%\^&\*;:{}=\-_`~()]/g,"");
                var finalText = removePunc.replace(/\s{2,}/g, " ");
                var arrWords = finalText.split(" ");
                for (var i = arrWords.length - 1; i >= 0; i--) {
                    word = arrWords[i].trim();
                    if (word) {
                        emit(word, 1);
                    }
                }
            }
        }
        """)

        reducer = Code("""
            function( key, values ) {
                return Array.sum(values);
            };
        """)

        unigram_results = self.tweets.map_reduce(mapper, reducer, 'unigramResults')

        pipeline = [
            {"$sort": {"value": -1}},
            {"$limit": 10}
        ]

        result_string = "\nThe top resulting bigrams are: \n"
        counter = 1
        for item in list(self.db.unigramResults.aggregate(pipeline)):
            result_string += str(counter) + ". " + item['_id'] + " : " + str(item['value']) + "\n"
            counter += 1
        return result_string

    # Question 6_2
    def get_bigrams(self):
        mapper = Code("""
            function() {
                text = this.text;
                if (text) {
                    var removePunc = text.toString().toLowerCase().replace(/[.,-\/#!$%\^&\*;:{}=\-_`~()]/g,"");
                    var finalText = removePunc.replace(/\s{2,}/g, " ");
                    var arrWords = finalText.split(" ");

                    for (var i=0; i<arrWords.length-1; i++) {
                        var word = arrWords[i];
                        var next_word = arrWords[i+1];
                        var bigram = word + " " + next_word;

                        if (bigram) {
                            emit(bigram, 1);
                        }
                    }
                }
            };
        """)

        reducer = Code("""
            function( key, values ) {
                return Array.sum(values);
            };
        """)
        bigrams = self.tweets.map_reduce(mapper, reducer, 'bigramResults')

        pipeline = [
            {"$sort": {"value": -1}},
            {"$limit": 10}
        ]

        result_string = "\nThe top resulting bigrams are: \n"
        counter = 1
        for item in list(self.db.bigramResults.aggregate(pipeline)):
            result_string += str(counter) + ". " + item['_id'] + " : " + str(item['value']) + "\n"
            counter += 1
        return result_string

    # Question 7
    def get_average_hashtags(self):
        mapper = Code("""
           function() {
            text = this.text.toString();
            if (text) {
                countOfHashtags = (text.match(/#/g) || []).length;
                emit(this.id, countOfHashtags);
            }
        }
        """)

        reducer = Code("""
            function( key, values ) {
                return Array.sum(values);
            };
        """)

        no_of_hashtags_results = self.tweets.map_reduce(mapper, reducer, 'countsOfHashtags')

        pipeline = [
            {"$group":
                 {"_id": "null", "avgHashTags":
                     {
                         "$avg": "$value"
                     }
                  }
             },
        ]

        average_hashtags = list(self.db.countsOfHashtags.aggregate(pipeline))

        seq = ("\nThe average number of hashtags in a message is:", str(average_hashtags[0]['avgHashTags']))
        return " ".join(seq)

    # Question 8
    def get_most_popular_area(self):
        mapper = Code("""
            function() {
                var maxLat = 59.32;
                var minLat = 49.42;
                var maxLng = 1.8607;
                var minLng = -7.6333;

                var cenLat = (maxLat+minLat)/2;
                var cenLng = (maxLng+minLng)/2;
                var lng = this.geo_lng;
                var lat = this.geo_lat;

                var coord = "";
                if (lng >= cenLng) {
                    if (lat > cenLat) {
                        coord = "NEast";
                    } else {
                        coord = "SEast";
                    }
                } else {
                    if (lat >= cenLat) {
                        coord = "NWest";
                    } else {
                        coord = "SWest";
                    }
                }
                emit(coord, 1);
            }
        """)

        reducer = Code("""
            function(key, result) {
                return Array.sum(result);
            }
        """)

        locs = {
            'SEast': 'South East area',
            'NEast': 'North East area',
            'SWest': 'South West area',
            'NWest': 'North West area'
        }

        topCoordinates = self.tweets.map_reduce(mapper, reducer, 'topCoordinates')
        topLocation = self.db.topCoordinates.find().sort("value", -1).limit(1)

        for item in topLocation:
            result = item

        seq = ("\nThe most popular area in the UK is the", locs[result['_id']], "with", str(result['value']), "tweets")
        return " ".join(seq)


def main(*agrs):
    app = TweetApp()
    print("It will take some time....Please note!")

    print(app.get_distinct_users())
    print(app.get_top_ten_users_tweet_percentage())
    print(app.get_earliest_and_latest_date_result())
    print(app.get_mean_time_deltas())
    print(app.get_mean_length_of_msg())
    print(app.get_unigrams())
    print(app.get_bigrams())
    print(app.get_most_popular_area())


if __name__ == '__main__': main()
