# -*- coding: utf-8 -*-
import json
import time

import oauth2 as oauth
import scrapy
from scrapy import Request
import MySQLdb
from scrapy.exceptions import CloseSpider
import csv

FIELDS = [
    'influencer_twitter_id',
    'id',
    'name',
    'screen_name',
    'location',
    'url',
    'description',
    'protected',
    'verified',
    'followers_count',
    'friends_count',
    'listed_count',
    'favourites_count',
    'statuses_count',
    'created_at',
    'utc_offset',
    'time_zone',
    'geo_enabled',
    'lang',
    'contributors_enabled',
    'profile_background_color',
    'profile_background_image_url',
    'profile_background_image_url_https',
    'profile_background_tile',
    'profile_banner_url',
    'profile_image_url',
    'profile_image_url_https',
    'profile_link_color',
    'profile_sidebar_border_color',
    'profile_sidebar_fill_color',
    'profile_text_color',
    'profile_use_background_image',
    'default_profile',
    'default_profile_image',
    'withheld_in_countries',
    'withheld_scope',
]

TWEET_FIELDS = [
    'id',
    'created_at',
    'text',
    'source',
    'retweet_count',
    'favorite_count',
    'lang',
    'id_str'
]


class TwitterSpider(scrapy.Spider):
    name = 'twitter'
    user_info_endpoint = "https://api.twitter.com/1.1/users/show.json?user_id={user_id}"
    user_info_by_screen_name_endpoint = "https://api.twitter.com/1.1/users/show.json?screen_name={screen_name}"
    user_tweet_endpoint = "https://api.twitter.com/1.1/statuses/user_timeline.json?user_id={user_id}&count=50"
    influencer_table = 'twitter_influencer'
    follower_table = 'twitter_follower'
    session_index = 0

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.conn = MySQLdb.connect('192.185.155.213', 'ncom_mmiia', 'mmiiad6%ghdjHf', 'ncom_mmiia', charset="utf8",
                                    use_unicode=True)
        self.cursor = self.conn.cursor()
        self.id = ''
        self.username = self.username
        self.session_file = self.session_file
        with open(self.session_file, newline='') as f:
            self.sessions = list(csv.DictReader(f))
        if len(self.sessions) < 1:
            raise CloseSpider(reason='You need at least 1 API keys')

    @staticmethod
    def get_follower_url(screen_name, cursor=-1):
        return "https://api.twitter.com/1.1/followers/ids.json?cursor={cursor}&screen_name={screen_name}&count=5000".format(
            cursor=cursor, screen_name=screen_name)

    def next_session(self):
        session = self.sessions[self.session_index]
        self.session_index = self.session_index + 1
        if self.session_index >= len(self.sessions):
            self.session_index = 0
        return session

    def create_authorization_header(self, endpoint):
        session = self.next_session()
        consumer = oauth.Consumer(key=session['consumer_key'], secret=session['consumer_secret'])
        token = oauth.Token(key=session['access_key'], secret=session['access_secret'])
        params = {
            'oauth_consumer_key': session['consumer_key'],
            'oauth_nonce': oauth.generate_nonce(),
            'oauth_timestamp': str(int(time.time())),
            'oauth_version': "1.0",
        }
        req = oauth.Request(method="GET", url=endpoint, parameters=params)
        req.sign_request(oauth.SignatureMethod_HMAC_SHA1(), consumer, token)
        header = req.to_header()
        return header

    def start_requests(self):
        user_info_endpoint = self.user_info_by_screen_name_endpoint.format(screen_name=self.username)
        yield Request(user_info_endpoint, method="GET",
                      headers=self.create_authorization_header(user_info_endpoint))

    def user_exists(self, twitter_id, table_name):
        result_count = self.cursor.execute("""SELECT * FROM {table_name} WHERE id=%s """.format(table_name=table_name),
                                           (twitter_id,))
        return result_count > 0

    def scraped_before_6_months(self, twitter_id, table_name):
        result_count = self.cursor.execute(
            """SELECT * FROM {table_name} WHERE scraped_at > NOW() - INTERVAL 6 MONTH AND id=%s""".format(
                table_name=table_name), (twitter_id,))
        return result_count > 0

    def connect_exists(self, follower_id, influencer_id):
        count = self.cursor.execute(
            "SELECT * FROM twitter_follower_influencer WHERE follower_id=%s AND influencer_id=%s",
            (follower_id, influencer_id))
        return count > 0

    def connect_follower_influencer(self, follower_id, influencer_id):
        if not self.connect_exists(follower_id, influencer_id):
            self.cursor.execute("INSERT INTO twitter_follower_influencer (follower_id, influencer_id) VALUES (%s, %s)",
                                (follower_id, influencer_id))

    def insert_user(self, item):
        table_name = self.follower_table
        if item['id'] == self.id:
            table_name = self.influencer_table
        if 'update' not in item or not item['update']:
            if 'update' in item:
                del item['update']
            placeholder = ", ".join(["%s"] * len(item))
            self.cursor.execute("insert into `{table}` ({columns}) values ({values});".format(table=table_name,
                                                                                              columns=",".join(
                                                                                                  item.keys()),
                                                                                              values=placeholder),
                                list(item.values()))
        else:
            del item['update']
            set_value = ''
            for k, v in item.items():
                set_value += ' {key}=%s,'.format(key=k)
            set_value = set_value.strip(',')
            statement = "UPDATE `{table_name}` SET {set_value} WHERE id={follower_id}".format(table_name=table_name,
                                                                                              set_value=set_value,
                                                                                              follower_id=item['id'])
            self.cursor.execute(statement, list(item.values()))

    def insert_tweet(self, tweet):
        placeholder = ", ".join(["%s"] * len(tweet))
        self.cursor.execute("insert into twitter_tweet ({columns}) values ({values});".format(columns=",".join(
            tweet.keys()),
            values=placeholder),
            list(tweet.values()))

    def parse(self, response):
        json_response = json.loads(response.body_as_unicode())
        self.id = json_response['id']
        update = False
        if self.user_exists(self.id, self.influencer_table):
            if self.scraped_before_6_months(self.id, self.influencer_table):
                update = True
            else:
                raise CloseSpider(reason='Username %s already in database' % self.username)
        user_info_endpoint = self.user_info_endpoint.format(user_id=self.id)
        yield Request(user_info_endpoint, method="GET", meta={'item': {
            'update': update
        }},
                      callback=self.parse_user_info,
                      headers=self.create_authorization_header(user_info_endpoint))
        follower_endpoint = self.get_follower_url(self.username)
        yield Request(follower_endpoint, method="GET", callback=self.get_followers,
                      headers=self.create_authorization_header(follower_endpoint))

    def get_followers(self, response):
        json_response = json.loads(response.body_as_unicode())
        print(json_response)
        for follower_id in json_response['ids']:
            item = {'update': False}
            if self.user_exists(follower_id, self.follower_table):
                if self.scraped_before_6_months(follower_id, self.follower_table):
                    item['update'] = True
                else:
                    self.connect_follower_influencer(follower_id, self.id)
                    continue
            self.connect_follower_influencer(follower_id, self.id)
            user_info_endpoint = self.user_info_endpoint.format(user_id=follower_id)

            yield Request(user_info_endpoint, method="GET",
                          meta={'item': item},
                          headers=self.create_authorization_header(user_info_endpoint),
                          callback=self.parse_user_info)

        if json_response['next_cursor'] != 0:
            follower_endpoint = self.get_follower_url(self.username, cursor=json_response['next_cursor'])
            yield Request(follower_endpoint, method="GET",
                          headers=self.create_authorization_header(follower_endpoint), callback=self.get_followers)

    def parse_user_info(self, response):
        json_response = json.loads(response.body_as_unicode())
        item = response.meta['item']
        for k, v in json_response.items():
            if k in FIELDS:
                item[k] = v
        self.insert_user(item)
        tweet_endpoint = self.user_tweet_endpoint.format(user_id=item['id'])
        yield Request(tweet_endpoint, method="GET",
                      headers=self.create_authorization_header(tweet_endpoint),
                      meta={'item': item},
                      callback=self.parse_tweets)

    def parse_tweets(self, response):
        json_response = json.loads(response.body_as_unicode())
        count = 0
        for tweet in json_response:
            tweet_item = {'user_id': response.meta['item']['id']}
            for k, v in tweet.items():
                if k in TWEET_FIELDS:
                    tweet_item['tweet_' + k] = v
            count += 1
            if count >= 25:
                return
            self.insert_tweet(tweet_item)
