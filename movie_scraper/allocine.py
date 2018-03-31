import re
import scrapy
from scrapy import Request
from urllib.parse import urlencode

class AllocineSpider(scrapy.Spider):
    name = 'allocine'
    allowed_domains = ['allocine.fr', 'google.com']

    def start_requests(self):
        start_url = 'https://www.google.com/search?'
        query = {'q': 'site:http://www.allocine.fr {} critique'.format(self.movie)}
        yield Request(url=start_url+urlencode(query), callback=self.parse)

    def parse(self, response):
        url = response.css('h3.r>a::attr(data-href)').extract_first()
        yield Request(url=url, callback=self.parse_movie)

    def parse_movie(self, response):
        trailer_url = response.urljoin(response.css('nav.third-nav a.trailer::attr(href)').extract_first())
        casting_url = response.urljoin(response.css('a.end-section-link[title="Casting complet et équipe technique"]::attr(href)').extract_first())
        photo_url = casting_url.split('/')
        photo_url.pop()
        photo_url.pop()
        photo_url = '/'.join(photo_url) + '/photos/'
        yield Request(casting_url, self.parse_casting, meta={
            'trailer_url':trailer_url,
            'photo_url':photo_url
        })

    def parse_casting(self, response):
        actors = response.css('strong[itemprop=actor] a span[itemprop=name]::text').extract()
        item = {
            'actors': '|'.join(actors)
        }
        meta = response.meta
        meta['item'] = item
        yield Request(meta['trailer_url'], self.parse_trailor,meta=meta)

    def parse_trailor(self, response):
        text = response.css('figure.player.js-player::attr(data-model)').extract_first()
        video_link = 'http:' + re.search('"medium":"([^"]*)"', text).group(1).replace('\\','')
        meta = response.meta
        meta['item']['trailer'] = video_link
        yield Request(meta['photo_url'], self.parse_photos,meta=meta)

    def parse_photos(self, response):
        images = response.css('img.shot-img::attr(data-src)').extract()
        images = '|'.join(images)
        item = response.meta['item']
        item['images'] = images
        yield item
