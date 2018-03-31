# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy.exceptions import DropItem
import MySQLdb
import traceback


class SocialmediaPipeline(object):
    def process_item(self, item, spider):
        return item


class DuplicatesPipeline(object):
    def __init__(self):
        self.ids_seen = set()

    def process_item(self, item, spider):
        if spider.name == 'instagram':
            if item['code'] in self.ids_seen:
                raise DropItem('Duplicate item found: %s' % item)
            else:
                self.ids_seen.add(item['code'])
        return item


class MyItemPipeline(object):
    def __init__(self):
        self.conn = MySQLdb.connect('192.185.155.213', 'ncom_mmiia', 'mmiiad6%ghdjHf', 'ncom_mmiia', charset="utf8",
                                    use_unicode=True)
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        if spider.name == 'instagram':
            try:
                if 'copy' in item:
                    self.cursor.execute(
                        'SELECT * FROM insta_follower WHERE post_id=(SELECT DISTINCT post_id FROM insta_follower WHERE id={follower_id})',
                        follower_id=item['id'])
                    followers = self.cursor.fetchAll()
                    for follower in followers:
                        follower['influencer_id'] = item['influencer_id']
                        placeholder = ", ".join(["%s"] * len(follower))
                        stmt = "insert into `insta_follower` ({columns}) values ({values});".format(
                            columns=",".join(follower.keys()),
                            values=placeholder)
                        self.cursor.execute(stmt, list(follower.values()))
                else:
                    if not 'update' in item or item['update']:
                        del item['update']
                        table_name = 'insta_influencer'
                        if 'influencer_id' in item:
                            table_name = 'insta_follower'
                        placeholder = ", ".join(["%s"] * len(item))
                        stmt = "insert into `{table}` ({columns}) values ({values});".format(table=table_name,
                                                                                             columns=",".join(
                                                                                                 item.keys()),
                                                                                             values=placeholder)
                        self.cursor.execute(stmt, list(item.values()))
                    else:  # update
                        del item['update']
                        set_value = ''
                        for k, v in item.items():
                            set_value += ' {key}=%s,'.format(key=k)
                        set_value = set_value.strip(',')
                        statement = "UPDATE insta_follower SET {set_value} WHERE id={follower_id}".format(
                            set_value=set_value, follower_id=item['id'])
                        self.cursor.execute(statement, list(item.values()))
            except Exception as e:
                traceback.print_exc()
                print('Exception {}'.format(e))
        elif spider.name == "twitter":
            try:
                if 'copy' in item:
                    self.cursor.execute(
                        'SELECT * FROM twitter_follower WHERE tweet_id=(SELECT DISTINCT tweet_id FROM twitter_followers WHERE id={follower_id})',
                        follower_id=item['id'])
                    followers = self.cursor.fetchAll()
                    for follower in followers:
                        follower['influencer_id'] = item['influencer_id']
                        placeholder = ", ".join(["%s"] * len(follower))
                        stmt = "insert into twitter_follower ({columns}) values ({values});".format(
                            columns=",".join(follower.keys()),
                            values=placeholder)
                        self.cursor.execute(stmt, list(follower.values()))
                else:
                    if 'update' not in item or not item['update']:
                        if 'update' in item:
                            del item['update']
                        table_name = 'twitter_influencers'
                        if 'influencer_twitter_id' in item:
                            table_name = 'twitter_followers'
                        placeholder = ", ".join(["%s"] * len(item))
                        
                        stmt = "insert into `{table}` ({columns}) values ({values});".format(table=table_name,
                                                                                             columns=",".join(
                                                                                                 item.keys()),
                                                                                             values=placeholder)
                        self.cursor.execute(stmt, list(item.values()))
                    else:  # update
                        del item['update']
                        set_value = ''
                        for k, v in item.items():
                            set_value += ' {key}=%s,'.format(key=k)
                        set_value = set_value.strip(',')
                        statement = "UPDATE twitter_follower SET {set_value} WHERE id={follower_id}".format(
                            set_value=set_value, follower_id=item['id'])
                        self.cursor.execute(statement, list(item.values()))
            except Exception as e:
                traceback.print_exc()
                print('Exception {}'.format(e))
        return item
