# -*- coding: utf-8 -*-
import scrapy
from scrapy import Request
from urllib.parse import urljoin, urlencode
from bs4 import BeautifulSoup

class RottentomatoesSpider(scrapy.Spider):
    name = 'rottentomatoes'
    allowed_domains = ['rottentomatoes.com', 'google.com']

    def start_requests(self):
        start_url = 'https://www.google.com/search?'
        query = {'q': 'site:rottentomatoes.com {}'.format(self.movie)}
        yield Request(url=start_url+urlencode(query), callback=self.parse)

    def parse(self, response):
        url = response.css('cite._Rm::text').extract_first().strip()
        yield Request(url=url, callback=self.parse_movie)

    def parse_movie(self, response):
        item = {}
        item['title'] = response.css('h1.title::text').extract_first().strip()
        item['id'] = response.url.split('/m/')[1].strip('/')
        item['tomatometer_rating'] = BeautifulSoup(response.css('div#scoreStats>div.superPageFontColor').extract_first(), 'lxml').text.split('Rating:')[1].strip()
        item['audience_rating'] = BeautifulSoup(response.css('div.audience-info div').extract_first(), 'lxml').text.split('Rating:')[1].strip()
        item['poster'] = response.css('div#movie-image-section img::attr(src)').extract_first()
        yield item