# -*- coding: utf-8 -*-
import re

import scrapy
from fang.items import NewHouseItem, ESFHouseItem
from scrapy_redis.spiders import RedisSpider


class SoufangspiderSpider(RedisSpider):
    name = 'soufangSpider'
    allowed_domains = ['fang.com']
    # start_urls = ['https://www.fang.com/SoufunFamily.htm']
    # 从redis sfw的start_url开始读
    redis_key = 'fang:start_urls'

    def parse(self, response):
        trs = response.xpath('//div[@class="outCont"]//tr')
        province = None
        for tr in trs:
            tds = tr.xpath('.//td[not(@class)]')
            province_id = tds[0]
            province_text = province_id.xpath('.//text()').get()
            province_text = re.sub(r'\s', '', province_text)
            if province_text:
                province = province_text

            # 不爬取海外信息
            if province == '其它':
                continue

            city_td = tds[1]
            city_links = city_td.xpath('.//a')
            for city_link in city_links:
                city = city_link.xpath('.//text()').get()
                city_url = city_link.xpath('.//@href').get()

                if city == '北京':
                    # 构建新房的url链接
                    url_new_module = 'https://newhouse.fang.com/house/s/'
                    # 构建二手房url
                    url_esf_module = 'https://esf.fang.com/'
                else:
                    # 构建新房的url链接
                    url_new_module = city_url.replace('fang.com/', 'newhouse.fang.com/house/s/')
                    # 构建二手房url
                    url_esf_module = city_url.replace('fang.com/', 'esf.fang.com/')
                yield scrapy.Request(url_new_module, callback=self.parse_newhouse, meta={'info': (province, city)})
                yield scrapy.Request(url_esf_module, callback=self.parse_esf, meta={'info': (province, city)})

    '''
        解析新房的所有数据
    '''

    def parse_newhouse(self, response):
        province, city = response.meta.get('info')
        lis = response.xpath('//div[contains(@class,"nl_con")]/ul/li')
        for li in lis:
            # 名字
            name = li.xpath('.//div[@class="nlcd_name"]/a/text()').get()
            if name:
                name = name.strip()
                # 介绍：几居
                house_type_list = li.xpath('.//div[contains(@class,"house_type")]//a/text()').getall()
                house_type_list = list(map(lambda x: x.replace(" ", ""), house_type_list))
                room = list(filter(lambda x: x.endswith('居'), house_type_list))
                # 面积
                area = ''.join(li.xpath('.//div[contains(@class,"house_type")]/text()').getall())
                area = re.sub('\s|－|/', '', area)
                # 地址
                address = li.xpath('.//div[@class="address"]/a/@title').get()
                # 位置
                district_text = ''.join(li.xpath('.//div[@class="address"]/a//text()').getall())
                district = re.search(r'.*\[(.+)\].*', district_text).group(1)
                # 是否在售
                sale = li.xpath('.//div[contains(@class,"fangyuan")]/span/text()').get()
                # 价格
                price = ''.join(li.xpath('.//div[@class="nhouse_price"]//text()').getall())
                price = re.sub(r'\s|广告', '', price)
                # 详细url
                origin_url = li.xpath('.//div[@class="nlcd_name"]/a/@href').get()
                origin_url = 'http:' + origin_url
                item = NewHouseItem(province=province, city=city, name=name, rooms=room, area=area, address=address,
                                    district=district, sale=sale, price=price, origin_url=origin_url)
                yield item
        next_url = response.xpath('//a[@class="next"]/@href').get()
        if next_url:
            yield scrapy.Request(url=response.urljoin(next_url), callback=self.parse_newhouse,
                                 meta={'info': (province, city)})

    '''
        解析二手房的所有数据
    '''

    def parse_esf(self, response):
        province, city = response.meta.get('info')
        dls = response.xpath('//div[@class="shop_list shop_list_4"]/dl')
        for dl in dls:
            item = ESFHouseItem()
            item['province'] = province
            item['city'] = city

            item['name'] = dl.xpath('.//p[@class="add_shop"]/a/@title').get()

            infos = dl.xpath('.//p[@class="tel_shop"]/text()').getall()
            infos = list(map(lambda x: re.sub(r'\s', '', x), infos))

            for info in infos:
                if '厅' in info:
                    item['rooms'] = info
                elif '层' in info:
                    item['floor'] = info
                elif '向' in info:
                    item['toward'] = info
                elif '年' in info:
                    item['year'] = info
                elif '㎡' in info:
                    item['area'] = info

            item['address'] = dl.xpath('.//p[@class="add_shop"]/span/text()').get()
            # 总价
            item['price'] = ''.join(dl.xpath('.//dd[@class="price_right"]/span[1]//text()').getall())
            # 单价
            item['unit'] = ''.join(dl.xpath('.//dd[@class="price_right"]/span[2]//text()').getall())

            detail_url = dl.xpath('.//h4[@class="clearfix"]/a/@href').get()
            item['origin_url'] = response.urljoin(detail_url)
            yield item
            print(item)
            print('==' * 40)

        next_url = response.xpath('//div[@id="list_D10_15"]/p[1]/a/@href').get()
        if next_url:
            yield scrapy.Request(url=response.urljoin(next_url), callback=self.parse_esf,
                                 meta={'info': (province, city)})
