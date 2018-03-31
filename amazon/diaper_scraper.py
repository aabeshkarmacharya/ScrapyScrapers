# Scrapes diaper data from amazon.com

import re 
import time
from decimal import Decimal
from re import sub

import scrapy

date = time.strftime('%m-%d-%Y %I:%M%p')


# find a number in the text that is followed by the phrase
def find_number(in_text, followed_by_phrases):
    m = re.search('([0-9]+)\s*(?=' + '|'.join(followed_by_phrases) + ')', in_text, re.IGNORECASE)
    if m:
        return m.group(1)


class DiapersSpider(scrapy.Spider):
    name = "diapers"
    start_urls = [
        'https://www.amazon.com/s/ref=lp_166764011_nr_n_9?fst=as%3Aoff'
        '&rh=n%3A165796011%2Cn%3A%21165797011%2Cn%3A166764011%2Cn%3A166772011'
        '&bbn=166764011&ie=UTF8&qid=1507817280&rnid=166764011']

    def parse(self, response):
        for item in response.css('li.a-carousel-card'):
            title = item.css('a::attr(title)').extract_first()
            unit_count = find_number(in_text=title,
                                     followed_by_phrases=['count', 'sheets', 'Pads', 'Pieces', 'Pcs', 'Pants', 'shts'])
            price = item.css('div.acs_product-price span.acs_product-price__buying::text').extract_first()
            if not price:
                price = item.css('div.gvprices .amt::text').extract_first()

            image_url = item.css('img::attr(data-src)').extract_first()
            if not image_url:
                image_url = item.css('img::attr(src)').extract_first()

            size_re = re.search('(?<=Size)\s*([0-9N]+)|(newborn)', title, re.IGNORECASE)
            size = None
            if size_re:
                size = size_re.group(1) if size_re.group(1) and size_re.group(1)else ('N' if size_re.group(
                    2) else None)
            if unit_count and size and (size == 'N' or int(size) < 3):
                yield {
                    'site': re.search('(?:https?://)([^/]+)', response.url).group(1),
                    'timestamp': date,
                    'brand': 'Pampers',
                    'size': size,
                    'title': title.strip(),
                    'url': 'https://www.amazon.com' + item.css('a::attr(href)').extract_first(),
                    'image': image_url,
                    'price': price.strip() if price else None,
                    'unitcount': unit_count,
                    'pricePerUnit': Decimal(sub(r'[^\d.]', '', price)).__float__() / float(
                        unit_count) if unit_count and price else 0
                }
