'''
@author: Souvik Das
Institute: University at Buffalo
'''

import demoji, re, datetime
import preprocessor


# demoji.download_codes()


class TWPreprocessor:
    @classmethod
    def preprocess(cls, tweet, config_obj):
        '''
        Do tweet pre-processing before indexing, make sure all the field data types are in the format as asked in the project doc.
        :param tweet:
        :return: dict
        '''

        tweet_obj = {
            'id': tweet.get('id'),
            'country': config_obj.get('country'), 'tweet_lang': tweet.get('lang'),
            'tweet_text': tweet.get('full_text'), 'tweet_date': _get_tweet_date(tweet.get('created_at')),
            'verified': tweet.get('user').get('verified'), 'hashtags': _get_entities(tweet, 'hashtags'),
            'mentions': _get_entities(tweet, 'mentions'), 'tweet_urls': _get_entities(tweet, 'urls'),
            'poi_id': tweet.get('user').get('id'), 'poi_name': tweet.get('user').get('screen_name')
        }

        if tweet.get('geo') is not None:
            tweet_obj['geolocation'] = tweet.get('geo').get('coordinates')

        tweet_obj['text_' + tweet.get('lang')], tweet_obj['tweet_emoticons'] = _text_cleaner(tweet.get('full_text'))

        return tweet_obj


def _get_entities(tweet, entity_type):
    result = []
    if entity_type == 'hashtags':
        hashtags = tweet['entities']['hashtags']

        for hashtag in hashtags:
            result.append(hashtag['text'])
    elif entity_type == 'mentions':
        mentions = tweet['entities']['user_mentions']

        for mention in mentions:
            result.append(mention['screen_name'])
    elif entity_type == 'urls':
        urls = tweet['entities']['urls']

        for url in urls:
            result.append(url['url'])

    return result


def _text_cleaner(text):
    emoticons_happy = list([
        ':-)', ':)', ';)', ':o)', ':]', ':3', ':c)', ':>', '=]', '8)', '=)', ':}',
        ':^)', ':-D', ':D', '8-D', '8D', 'x-D', 'xD', 'X-D', 'XD', '=-D', '=D',
        '=-3', '=3', ':-))', ":'-)", ":')", ':*', ':^*', '>:P', ':-P', ':P', 'X-P',
        'x-p', 'xp', 'XP', ':-p', ':p', '=p', ':-b', ':b', '>:)', '>;)', '>:-)',
        '<3'
    ])
    emoticons_sad = list([
        ':L', ':-/', '>:/', ':S', '>:[', ':@', ':-(', ':[', ':-||', '=L', ':<',
        ':-[', ':-<', '=\\', '=/', '>:(', ':(', '>.<', ":'-(", ":'(", ':\\', ':-c',
        ':c', ':{', '>:\\', ';('
    ])
    all_emoticons = emoticons_happy + emoticons_sad

    emojis = list(demoji.findall(text).keys())
    clean_text = demoji.replace(text, '')

    for emo in all_emoticons:
        if (emo in clean_text):
            clean_text = clean_text.replace(emo, '')
            emojis.append(emo)

    clean_text = preprocessor.clean(text)
    # preprocessor.set_options(preprocessor.OPT.EMOJI, preprocessor.OPT.SMILEY)
    # emojis= preprocessor.parse(text)

    return clean_text, emojis


def _get_tweet_date(tweet_date):
    return _hour_rounder(datetime.datetime.strptime(tweet_date, '%a %b %d %H:%M:%S +0000 %Y')).strftime(
        '%Y-%m-%dT%H:%M:%SZ')


def _hour_rounder(t):
    # Rounds to nearest hour by adding a timedelta hour if minute >= 30
    return (t.replace(second=0, microsecond=0, minute=0, hour=t.hour)
            + datetime.timedelta(hours=t.minute // 30))
