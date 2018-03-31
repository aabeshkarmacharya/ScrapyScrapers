import re
import scrapy
from scrapy import Request


class AllocineSpider(scrapy.Spider):
    name = 'allocine'
    base_url = 'http://www.allocine.fr/film/fichefilm-61282'
    home_url = 'http://www.allocine.fr/film/fichefilm_gen_cfilm=61282.html'

    def start_requests(self):
        yield Request(self.home_url)

    def parse(self, response):
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
