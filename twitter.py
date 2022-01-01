

import tweepy


class Twitter:
    def __init__(self):
        self.auth = tweepy.OAuthHandler("",
                                        "")
        self.auth.set_access_token("",
                                   "")
        self.api = tweepy.API(self.auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    def _meet_basic_tweet_requirements(self):
        '''
        Add basic tweet requirements logic, like language, country, covid type etc.
        :return: boolean
        '''
        return True

    def get_tweets_by_poi_screen_name(self, screen_name, tweet_count):
        '''
        Use user_timeline api to fetch POI related tweets, some postprocessing may be required.
        :return: List
        '''
        # return self.api.user_timeline(screen_name = screen_name, count = tweet_count, tweet_mode='extended')
        tweets = tweepy.Cursor(self.api.user_timeline, screen_name=screen_name, count=tweet_count,
                               tweet_mode='extended')
        tweet_list = []
        for tweet in tweets.items(tweet_count):
            tweet_list.append(tweet)
        return tweet_list

    def get_tweets_by_lang_and_keyword(self, keyword, tweet_count, language, country):
        '''
        Use search api to fetch keywords and language related tweets, use tweepy Cursor.
        :return: List
        '''
        '''query = keyword
        if country != None:
            places = self.api.geo_search(query=country, granularity="country")
            place_id = places[0].id
            query = keyword + (" place:%s" % place_id)'''

        tweets = tweepy.Cursor(self.api.search, q=keyword, lang=language, tweet_mode='extended')
        tweet_list = []
        for tweet in tweets.items(tweet_count):
            tweet_list.append(tweet)
        return tweet_list

    def get_replies(self):
        '''
        Get replies for a particular tweet_id, use max_id and since_id.
        For more info: https://developer.twitter.com/en/docs/twitter-api/v1/tweets/timelines/guides/working-with-timelines
        :return: List
        '''
        raise NotImplementedError