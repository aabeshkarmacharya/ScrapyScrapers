# -*- coding: utf-8 -*-
import scrapy
from scrapy import Request
from urllib.parse import urlencode
# when I search movie "Avatar". I need original title, ID, rating, trailers links, photos link... for this movie.
# It works very well but sometimes, it is not reliable, so I would to convert this work with scrappy.

class MovieSpider(scrapy.Spider):
    name = 'movie'
    allowed_domains = ['imdb.com', 'rottentomatoes.com', 'allocine.fr', 'google.com']
    start_urls = ['https://www.google.com/']

    def start_requests(self):
        start_url = 'https://www.google.com/search?'
        query_imdb = {'q': 'site:imdb.com avatar'}
        query_rotten = {'q':'site: rottentomatoes.com avatar'}
        query_allocine = {'q': 'site:allocine.fr avatar'}
        yield Request(url=start_url+urlencode(query_imdb), callback=self.parse)

    def parse(self, response):
        url = response.css('cite._Rm::text').extract_first().strip()
        yield {'url': url}
