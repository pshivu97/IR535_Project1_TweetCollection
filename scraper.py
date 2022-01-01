

import json
import datetime
import pandas as pd
from twitter import Twitter
from tweet_preprocessor import TWPreprocessor
from indexer import Indexer
import pickle
import re

reply_collection_knob = False


def read_config():
    with open("config.json") as json_file:
        data = json.load(json_file)
    return data


def write_config(data):
    with open("config.json", 'w') as json_file:
        json.dump(data, json_file)


def save_file(data, filename):
    df = pd.DataFrame(data)
    df.to_pickle("data/" + filename)


def save_raw_file(data, filename):
    outfile = open('data/raw_tweets/' + filename, 'wb')
    pickle.dump(data, outfile)
    outfile.close()


def save_raw_processed_file(data, filename):
    outfile = open('data/raw_processed_tweets/' + filename, 'wb')
    pickle.dump(data, outfile)
    outfile.close()


def read_file(type, id):
    return pd.read_pickle(f"data/{type}_{id}.pkl")


def check_if_retweet(tweet):
    pattern = '^RT'
    return re.match(pattern, tweet.get("full_text"))


def check_if_verified(tweet):
    return tweet.get('user').get('verified')


def read_keyword_file():
    with open("keywordFile.json") as json_file:
        data = json.load(json_file)
    return data


def get_keywords_list():
    keyword_list = read_keyword_file()
    covid_keywords_list = keyword_list['covid']
    vaccine_keywords_list = keyword_list['vaccine']
    covid_keywords_list.extend(vaccine_keywords_list)
    return covid_keywords_list


def main():
    config = read_config()
    indexer = Indexer()
    twitter = Twitter()

    pois = config["pois"]
    keywords = config["keywords"]

    total_processed_tweets = 0

    keywords_list = get_keywords_list()

    for i in range(len(pois)):
        max_tweet_count = 150
        total_retweets = 0
        if pois[i]["finished"] == 0:
            curnt_poi = pois[i]
            print(f"---------- collecting tweets for poi: {curnt_poi['screen_name']}")

            raw_tweets = twitter.get_tweets_by_poi_screen_name(curnt_poi['screen_name'],
                                                               curnt_poi['count'])  # pass args as needed

            save_raw_file(raw_tweets, f"poi_{pois[i]['id']}.pkl")

            covid_tweet_ids = []
            processed_tweets = []
            for tw in raw_tweets:
                if check_if_retweet(tw._json):
                    if total_retweets < max_tweet_count:
                        processed_tweets.append(TWPreprocessor.preprocess(tw._json, curnt_poi))
                        total_retweets += 1
                else:
                    processed_tweets.append(TWPreprocessor.preprocess(tw._json, curnt_poi))

                if any(word in tw._json.get("full_text") for word in keywords_list):
                    covid_tweet_ids.append(tw._json.get("id"))

            indexer.create_documents(processed_tweets)

            pois[i]["finished"] = 1
            pois[i]["collected"] = len(processed_tweets)
            pois[i]["covid_tweet_ids"].extend(covid_tweet_ids)
            pois[i]["covid_tweets_length"] += len(covid_tweet_ids)

            write_config({
                "pois": pois, "keywords": keywords
            })

            total_processed_tweets += len(processed_tweets)

            save_raw_processed_file(processed_tweets, f"poi_{pois[i]['id']}.pkl")
            save_file(processed_tweets, f"poi_{pois[i]['id']}.pkl")

            print("Raw Tweets Count : ", len(raw_tweets))
            print("Processed Tweets Count : ", len(processed_tweets))
            print("Retweets Discarded Count : ", (len(raw_tweets) - len(processed_tweets)))
            print("Covid Tweets Count : ", len(covid_tweet_ids))
            print("------------ process complete -----------------------------------")

    for i in range(len(keywords)):
        max_tweet_count = 150
        total_retweets = 0
        if keywords[i]["finished"] == 0:
            curnt_keyword = keywords[i]
            print(f"---------- collecting tweets for keyword: {curnt_keyword['name']}")

            raw_tweets = twitter.get_tweets_by_lang_and_keyword(curnt_keyword.get('name'), curnt_keyword.get('count'),
                                                                curnt_keyword.get('lang'),
                                                                curnt_keyword.get('country'))  # pass args as needed

            save_raw_file(raw_tweets, f"keywords_{keywords[i]['id']}.pkl")

            processed_tweets = []
            for tw in raw_tweets:
                if not check_if_verified(tw._json):
                    if check_if_retweet(tw._json):
                        if total_retweets < max_tweet_count:
                            processed_tweets.append(TWPreprocessor.preprocess(tw._json, curnt_keyword))
                            total_retweets += 1
                    else:
                        processed_tweets.append(TWPreprocessor.preprocess(tw._json, curnt_keyword))

            indexer.create_documents(processed_tweets)

            keywords[i]["finished"] = 1
            keywords[i]["collected"] = len(processed_tweets)

            write_config({
                "pois": pois, "keywords": keywords
            })

            save_raw_processed_file(processed_tweets, f"keywords_{keywords[i]['id']}.pkl")
            save_file(processed_tweets, f"keywords_{keywords[i]['id']}.pkl")

            total_processed_tweets += len(processed_tweets)

            print("Raw Tweets Count : ", len(raw_tweets))
            print("Processed Tweets Count : ", len(processed_tweets))
            print("Retweets Discarded Count : ", (len(raw_tweets) - len(processed_tweets)))
            print("------------ process complete -----------------------------------")

    if reply_collection_knob:
        # Write a driver logic for reply collection, use the tweets from the data files for which the replies are to collected.

        raise NotImplementedError

    print("Total Tweet Count : ", total_processed_tweets)
    print("Total Re - Tweet Count : ", total_retweets)


if __name__ == "__main__":
    main()
