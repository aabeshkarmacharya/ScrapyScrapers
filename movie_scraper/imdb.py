# -*- coding: utf-8 -*-
import scrapy
from scrapy import Request
from urllib.parse import urlencode, urljoin

class MovieSpider(scrapy.Spider):
    name = 'imdb'
    allowed_domains = ['imdb.com', 'google.com']

    def start_requests(self):
        start_url = 'https://www.google.com/search?'
        query = {'q': 'site:imdb.com {}'.format(self.movie)}
        yield Request(url=start_url+urlencode(query), callback=self.parse)

    def parse(self, response):
        url = response.css('cite._Rm::text').extract_first().strip()
        yield Request(url=url, callback=self.parse_movie)

    def parse_movie(self, response):
        item = {}
        item['title'] = response.css('div.title_wrapper>h1::text').extract_first()
        item['id'] = response.url.split('title/')[1].strip('/')
        item['rating'] = response.css('div.ratingValue span::text').extract_first()
        item['trailer'] = urljoin('http://www.imdb.com',response.css('div.slate>a::attr(href)').extract_first())
        item['poster'] = response.css('div.poster img::attr(src)').extract_first()
        yield item


