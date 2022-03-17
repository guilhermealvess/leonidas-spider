from operator import le
from os import link
from turtle import title
import scrapy
import pandas as pd

class MlSpider(scrapy.Spider):
    name = 'ml'
    allowed_domains = ['mercadolivre.com']
    start_urls = [f'https://www.mercadolivre.com.br/ofertas?page={i}' for i in range(1, 42)]

    def parse(self, response, **kwargs):
        items = response.xpath('//li[@class="promotion-item default"]')
        old_prices = list()
        prices = list()
        titles = list()
        images = list()
        links = list()

        for i in range(41):
            for item in items:
                old_prices.append(item.xpath('.//p[@class="promotion-item__oldprice"]//text()').get())
                prices.append(item.xpath('.//span[@class="promotion-item__price"]//text()').get())
                titles.append(item.xpath('.//p[@class="promotion-item__title"]//text()').get())
                images.append(item.xpath('.//img/@src').get())
                links.append(item.xpath('./a/@href').get())


            df_content = {'OLD_PRICES': old_prices, 'PRICES': prices, 'TITLE': titles, 'IMAGE': images, 'LINK': links}
            df = pd.DataFrame(data=df_content)
            df.to_csv(f'dataset/result_{i}_ml.csv')
