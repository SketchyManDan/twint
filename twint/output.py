from datetime import datetime

from . import format
from .tweet import Tweet
from .user import User
from .storage import db, elasticsearch, write, panda

import logging
logger = logging.getLogger(__name__)

follows_list = []
tweets_list = []
users_list = []

author_list = {''}
author_list.pop()

# used by Pandas
_follows_object = {}


def _formatDateTime(datetimestamp):
    try:
        return int(datetime.strptime(datetimestamp, "%Y-%m-%d %H:%M:%S").timestamp())
    except ValueError:
        return int(datetime.strptime(datetimestamp, "%Y-%m-%d").timestamp())


def _clean_follow_list():
    logger.debug(':clean_follow_list')
    global _follows_object
    _follows_object = {}


def clean_lists():
    logger.debug(':clean_lists')
    global follows_list
    global tweets_list
    global users_list
    follows_list = []
    tweets_list = []
    users_list = []


def datecheck(datetimestamp, config):
    logger.debug(':datecheck')
    if config.Since:
        logger.debug(':datecheck:SinceTrue')

        d = _formatDateTime(datetimestamp)
        s = _formatDateTime(config.Since)

        if d < s:
            return False
    if config.Until:
        logger.debug(':datecheck:UntilTrue')

        d = _formatDateTime(datetimestamp)
        s = _formatDateTime(config.Until)

        if d > s:
            return False
    logger.debug(':datecheck:dateRangeFalse')
    return True


# TODO In this method we need to delete the quoted tweets, because twitter also sends the quoted tweets in the
#  `tweets` list along with the other tweets
def is_tweet(tw):
    try:
        tw["data-item-id"]
        logger.debug(':is_tweet:True')
        return True
    except:
        logger.critical(':is_tweet:False')
        return False


def _output(obj, output, config, **extra):
    logger.debug(':_output')
    if config.Lowercase:
        if isinstance(obj, str):
            logger.debug(':_output:Lowercase:username')
            obj = obj.lower()
        elif obj.__class__.__name__ == "user":
            logger.debug(':_output:Lowercase:user')
            pass
        elif obj.__class__.__name__ == "tweet":
            logger.debug(':_output:Lowercase:tweet')
            obj.username = obj.username.lower()
            author_list.update({obj.username})
            for dct in obj.mentions:
                for key, val in dct.items():
                    dct[key] = val.lower()
            for i in range(len(obj.hashtags)):
                obj.hashtags[i] = obj.hashtags[i].lower()
            for i in range(len(obj.cashtags)):
                obj.cashtags[i] = obj.cashtags[i].lower()
        else:
            logger.info('_output:Lowercase:hiddenTweetFound')
            print("[x] Hidden tweet found, account suspended due to violation of TOS")
            return
    if config.Output != None:
        if config.Store_csv:
            try:
                write.Csv(obj, config)
                logger.debug(':_output:CSV')
            except Exception as e:
                logger.critical(f':_output:CSV:Error:{e}')
                print(str(e) + " [x] output._output")
        elif config.Store_json:
            write.Json(obj, config)
            logger.debug(':_output:JSON')
        else:
            write.Text(output, config.Output)
            logger.debug(':_output:Text')

    if config.Elasticsearch:
        logger.debug(':_output:Elasticsearch')
        print("", end=".", flush=True)
    else:
        if not config.Hide_output:
            try:
                print(output.replace('\n', ' '))
            except UnicodeEncodeError:
                logger.critical(':_output:UnicodeEncodeError')
                print("unicode error [x] output._output")


async def checkData(tweet, config, conn):
    logger.debug(':checkData')
    tweet = Tweet(tweet, config)
    if not tweet.datestamp:
        logger.critical(':checkData:hiddenTweetFound')
        print("[x] Hidden tweet found, account suspended due to violation of TOS")
        return
    if datecheck(tweet.datestamp + " " + tweet.timestamp, config):
        output = format.Tweet(config, tweet)
        if config.Database:
            logger.debug(':checkData:Database')
            db.tweets(conn, tweet, config)
        if config.Pandas:
            logger.debug(':checkData:Pandas')
            panda.update(tweet, config)
        if config.Store_object:
            logger.debug(':checkData:Store_object')
            if hasattr(config.Store_object_tweets_list, 'append'):
                config.Store_object_tweets_list.append(tweet)
            else:
                tweets_list.append(tweet)
        if config.Elasticsearch:
            logger.debug(':checkData:Elasticsearch')
            elasticsearch.Tweet(tweet, config)
        _output(tweet, output, config)
    # else:
    #     logger.critical(':checkData:copyrightedTweet')


async def Tweets(tweets, config, conn):
    logger.debug(':Tweets')
    if config.Favorites or config.Location:
        logger.debug(':Tweets:fav+full+loc')
        for tw in tweets:
            await checkData(tw, config, conn)
    elif config.TwitterSearch or config.Profile:
        logger.debug(':Tweets:TwitterSearch')
        await checkData(tweets, config, conn)
    else:
        logger.debug(':Tweets:else')
        if int(tweets["data-user-id"]) == config.User_id or config.Retweets:
            await checkData(tweets, config, conn)


async def Users(u, config, conn):
    logger.debug(':User')
    global users_list

    user = User(u)
    output = format.User(config.Format, user)

    if config.Database:
        logger.debug(':User:Database')
        db.user(conn, config, user)

    if config.Elasticsearch:
        logger.debug(':User:Elasticsearch')
        _save_date = user.join_date
        _save_time = user.join_time
        user.join_date = str(datetime.strptime(user.join_date, "%d %b %Y")).split()[0]
        user.join_time = str(datetime.strptime(user.join_time, "%I:%M %p")).split()[1]
        elasticsearch.UserProfile(user, config)
        user.join_date = _save_date
        user.join_time = _save_time

    if config.Store_object:
        logger.debug(':User:Store_object')

        if hasattr(config.Store_object_follow_list, 'append'):
            config.Store_object_follow_list.append(user)
        elif hasattr(config.Store_object_users_list, 'append'):
            config.Store_object_users_list.append(user)
        else:
            users_list.append(user)  # twint.user.user

    if config.Pandas:
        logger.debug(':User:Pandas+user')
        panda.update(user, config)

    _output(user, output, config)


async def Username(username, config, conn):
    logger.debug(':Username')
    global _follows_object
    global follows_list
    follow_var = config.Following * "following" + config.Followers * "followers"

    if config.Database:
        logger.debug(':Username:Database')
        db.follow(conn, config.Username, config.Followers, username)

    if config.Elasticsearch:
        logger.debug(':Username:Elasticsearch')
        elasticsearch.Follow(username, config)

    if config.Store_object:
        if hasattr(config.Store_object_follow_list, 'append'):
            config.Store_object_follow_list.append(username)
        else:
            follows_list.append(username)  # twint.user.user

    if config.Pandas:
        logger.debug(':Username:object+pandas')
        try:
            _ = _follows_object[config.Username][follow_var]
        except KeyError:
            _follows_object.update({config.Username: {follow_var: []}})
        _follows_object[config.Username][follow_var].append(username)
        if config.Pandas_au:
            logger.debug(':Username:object+pandas+au')
            panda.update(_follows_object[config.Username], config)
    _output(username, username, config)
