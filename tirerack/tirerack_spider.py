# Scrapes products from www.tirerack.com
# {MPC, RTCPC, Brand, Product URL, Zipcode, Quantity, ListPrice, RawPrice, Shipping}

import time

import scrapy
from scrapy import Request
import csv
import re

date = time.strftime('%m-%d-%Y %I:%M%p')


class TirerackSpider(scrapy.Spider):
    name = 'tirerack_spider'

    def start_requests(self):
        with open('data.tsv', 'r') as tsvin:
            tsvin = csv.reader(tsvin, delimiter='\t')
            next(tsvin)
            for i, row in enumerate(tsvin):
                request = Request(row[3],
                                  callback=self.parse,
                                  meta={'cookiejar': i},
                                  dont_filter=True)
                request.meta['old_row'] = row
                yield request

    def parse(self, response):
        # cookies = []
        # for cookie in response.headers.getlist('Set-Cookie'):
        #     cookie = str(cookie).split(";")[0].split("=")
        #     cookie_name = cookie[0].strip("b'")
        #     cookie_value = cookie[1]
        #     cookies.append({cookie_name: cookie_value})
        zipcode = response.meta['old_row'][4]
        quantity = response.css("select.fullW option:checked::text").extract_first()
        listprice = response.css("li.priceTag span[itemprop=price]::text").extract_first()
        rawprice = response.css("li.priceTag div.dPriceStrike span:nth-of-type(2)::text").extract_first()
        if not rawprice:
            rawprice = listprice
            listprice = ''

        request = Request("https://www.tirerack.com/shippingquote/getZip.jsp?newZip=y", self.set_zip,
                          meta={'cookiejar': response.meta['cookiejar']}, dont_filter=True)
        request.meta['old_row'] = response.meta['old_row']
        request.meta['quantity'] = quantity
        request.meta['listprice'] = listprice
        request.meta['rawprice'] = rawprice
        yield request

    def set_zip(self, response):
        request = Request("https://www.tirerack.com/shippingquote/SetZip.jsp?zip=" + response.meta['old_row'][4],
                          self.get_shipping,
                          dont_filter=True,
                          meta={'cookiejar': response.meta['cookiejar']})
        request.meta['old_row'] = response.meta['old_row']
        request.meta['quantity'] = response.meta['quantity']
        request.meta['listprice'] = response.meta['listprice']
        request.meta['rawprice'] = response.meta['rawprice']
        yield request

    def get_shipping(self, response):
        print(response)
        shipping_cost = response.css("div.SQcol4::text").extract_first()
        yield {
            'MPC': response.meta['old_row'][0],
            'RTCPC': response.meta['old_row'][1],
            'Brand': response.meta['old_row'][2],
            'Product URL': response.meta['old_row'][3],
            'Zipcode': response.meta['old_row'][4],
            'Quantity': response.meta['quantity'],
            'ListPrice': response.meta['listprice'],
            'RawPrice': response.meta['rawprice'],
            'Shipping': shipping_cost,
            'Discount': '',
            'AddtoCart': ''
        }
