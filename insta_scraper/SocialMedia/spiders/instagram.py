import scrapy
from scrapy import Request
from scrapy.exceptions import CloseSpider
import json
import MySQLdb
import csv


class InstagramSpider(scrapy.Spider):
    name = 'instagram'
    allowed_domains = ['instagram.com']
    id = None
    follower_num = 5000
    session_index = 0
    sessions = []
    follower_table = 'instagram_follower'
    influencer_table = 'instagram_influencer'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.username = self.username
        self.session_file = self.session_file
        self.conn = MySQLdb.connect('127.0.0.1', 'root', 'root', 'rootdb', charset="utf8",
                                    use_unicode=True)
        self.cursor = self.conn.cursor()

    def user_exists(self, instagram_id, table_name):
        result_count = self.cursor.execute("""SELECT * FROM {table_name} WHERE id=%s """.format(table_name=table_name),
                                           (instagram_id,))
        return result_count > 0

    def scraped_before_6_months(self, instagram_id, table_name):
        result_count = self.cursor.execute(
            """SELECT * FROM {table_name} WHERE scraped_at > NOW() - INTERVAL 6 MONTH AND id=%s""".format(
                table_name=table_name), (instagram_id,))
        return result_count > 0

    def connect_exists(self, follower_id, influencer_id):
        count = self.cursor.execute(
            "SELECT * FROM instagram_follower_influencer WHERE follower_id=%s AND influencer_id=%s",
            (follower_id, influencer_id))
        return count > 0

    def connect_follower_influencer(self, follower_id, influencer_id):
        if not self.connect_exists(follower_id, influencer_id):
            self.cursor.execute(
                "INSERT INTO instagram_follower_influencer (follower_id, influencer_id) VALUES (%s, %s)",
                (follower_id, influencer_id))

    def insert_user(self, item):
        table_name = self.follower_table
        if item['id'] == self.id:
            table_name = self.influencer_table
        if 'update' not in item or not item['update']:
            if 'update' in item:
                del item['update']
            placeholder = ", ".join(["%s"] * len(item))
            self.cursor.execute("INSERT INTO `{table}` ({columns}) VALUES ({values});".format(table=table_name,
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

    def insert_post(self, post):
        placeholder = ", ".join(["%s"] * len(post))
        self.cursor.execute("insert into instagram_post ({columns}) values ({values})".format(columns=",".join(
            post.keys()),
            values=placeholder),
            list(post.values()))

    def next_session(self):
        session = self.sessions[self.session_index]
        self.session_index = self.session_index + 1
        if self.session_index >= len(self.sessions):
            self.session_index = 0
        return session

    def start_requests(self):
        with open(self.session_file, 'r') as f:
            self.sessions = [session[0] for session in list(csv.reader(f))[1:]]
        yield Request('https://www.instagram.com/', cookies={
            'sessionid': self.next_session(),
        },
                      callback=self.parse,
                      dont_filter=True
                      )

    def parse(self, response):
        url = 'https://www.instagram.com/{}/?__a=1'.format(self.username)
        request = Request(url=url, cookies={
            'sessionid': self.next_session(),
        }, callback=self.parse_profile)
        request.meta['id'] = ''
        yield request

    def parse_profile(self, response):
        profile_dict = json.loads(response.text)['graphql']
        print(profile_dict)
        update = False
        if self.username == profile_dict['user']['username']:
            self.id = profile_dict['user']['id']
            if self.user_exists(self.id, self.influencer_table):
                if self.scraped_before_6_months(self.id, self.influencer_table):
                    update = True
                else:
                    raise CloseSpider(reason='Username %s already in database' % self.username)
        else:
            update = response.meta['update']
        item = {'id': profile_dict['user']['id'], 'username': profile_dict['user']['username'],
                'full_name': profile_dict['user']['full_name'], 'external_url': profile_dict['user']['external_url'],
                'is_private': profile_dict['user']['is_private'], 'is_verified': profile_dict['user']['is_verified'],
                'profile_pic_url': profile_dict['user']['profile_pic_url'],
                'followed_by': profile_dict['user']['edge_followed_by']['count'],
                'follows': profile_dict['user']['edge_follow']['count'],
                'number_of_posts': profile_dict['user']['edge_owner_to_timeline_media']['count'],
                'connected_fb_page': profile_dict['user']['connected_fb_page'],
                'update': update}
        if profile_dict['user']['biography']:
            item['biography'] = str(profile_dict['user']['biography'])
        self.insert_user(item)
        post_url = 'https://www.instagram.com/graphql/query/?query_hash=472f257a40c653c64c666ce877d59d2b&' \
                   'variables=%7B%22id%22%3A%22{}%22%2C%22first%22%3A25%7D'.format(item['id'])
        request = Request(url=post_url, meta={'username': item['username'], 'id': item['id']},callback=self.parse_post)
        yield request

    def parse_post(self, response):
        post_dict = json.loads(response.text)
        item = {'user_id': response.meta['id']}
        posts = post_dict['data']['user']['edge_owner_to_timeline_media']['edges']
        for post in posts:
            item['post_id'] = post['node']['id']
            item['thumbnail_src'] = post['node']['thumbnail_src']
            item['code'] = post['node']['shortcode']
            try:
                item['caption'] = str(post['node']['edge_media_to_caption']['edges'][0]['node']['text'])
            except Exception:
                item['caption'] = ''
            item['comments'] = post['node']['edge_media_to_comment']['count']
            item['likes'] = post['node']['edge_media_preview_like']['count']
            # item['engagement'] = (item['comments'] + item['likes']) / item['followed_by']
            item['is_video'] = post['node']['is_video']
            self.insert_post(item)

        if self.username == response.meta['username']:
            print('parsing follower')
            follower_url = 'https://www.instagram.com/graphql/query/?query_id=17851374694183129&variables=' \
                           '%7B%22id%22%3A%22{}%22%2C%22first%22%3A{}%7D'.format(self.id, self.follower_num)
            request = Request(follower_url, cookies={
                'sessionid': self.next_session(),
            }, callback=self.parse_follower)
            yield request

    def parse_follower(self, response):
        follower_dict = json.loads(response.text)
        has_next = follower_dict['data']['user']['edge_followed_by']['page_info']['has_next_page']
        followers = follower_dict['data']['user']['edge_followed_by']['edges']

        for follower in followers:
            follower_id = follower['node']['id']
            update = False
            if self.user_exists(follower_id, self.follower_table):
                if self.scraped_before_6_months(follower_id, self.follower_table):
                    update = True
                else:
                    self.connect_follower_influencer(follower_id, self.id)
                    continue
            self.connect_follower_influencer(follower_id, self.id)
            url = 'https://www.instagram.com/{}/?__a=1'
            request = Request(url=url.format(follower['node']['username']), cookies={
                'sessionid': self.next_session(),
            }, callback=self.parse_profile)
            request.meta['update'] = update
            yield request

        if has_next:
            end_cursor = follower_dict['data']['user']['edge_followed_by']['page_info']['end_cursor']
            url = 'https://www.instagram.com/graphql/query/?query_id=17851374694183129&variables=%7B%22id%22%3A%22{}%22%2C%22first%22%3A{}%2C%22after%22%3A%22{}%22%7D'.format(
                id, self.follower_num, end_cursor)
            request = Request(url, cookies={
                'sessionid': self.next_session(),
            }, callback=self.parse_follower)
            request.meta['id'] = id
            yield request
