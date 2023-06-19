# -*- coding: utf-8 -*-
# @Time : 2023/6/6 23:11
# @Author : DanYang
# @File : Crawler.py
# @Software : PyCharm
import os
import csv
import re
import random
import asyncio
import json
from collections import defaultdict, Counter
from datetime import datetime, timedelta
from itertools import zip_longest

from aioretry import retry, RetryPolicyStrategy, RetryInfo
from pyppeteer import launch
from bs4 import BeautifulSoup
from pymongo import MongoClient


class Crawler:
    def __init__(self):
        self.proxy = "127.0.0.1:4780"
        self.browser = None
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client['TwitterComments']
        self.url = "https://twitter.com/search?q={q}%20until%3A{year1}-{month1}-{date1}%20since%3A{" \
                   "year2}-{month2}-{date2}&src=typed_query&f=top "

        self.article_xpath = "//div[@class='css-1dbjc4n r-18u37iz r-1q142lx']/a"
        self.date_xpath = self.article_xpath + "/time"

    def _retry_policy(self, info: RetryInfo) -> RetryPolicyStrategy:
        return False, (info.fails - 1) % 3 * 0.1

    async def _before_retry(self, info: RetryInfo) -> None:
        await self.page.close()

    async def __connect__(self):
        if self.browser is None:
            self.browser = await launch(headless=False, args=["--proxy-server=" + self.proxy], ignoreHTTPSErrors=True)
        self.page = await self.browser.newPage()
        await self.page.setViewport({
            "width": 1920,
            "height": 1080
        })

    async def __set_cookies__(self):
        cookies = json.load(open("cookies.json", 'rb'))
        for cookie in cookies:
            await self.page.setCookie(cookie)

    async def _slow_scroll(self, scroll_y=300):
        results = defaultdict(set)
        while True:
            position1 = await self.page.evaluate("() => {return window.pageYOffset}")
            await self.page.evaluate(f"window.scrollBy(0, {scroll_y})")
            position2 = await self.page.evaluate("() => {return window.pageYOffset}")
            if position1 == position2:
                break
            await asyncio.sleep(0.5)
            hrefs, dates = await self._get_href_date()
            for href, date in zip(hrefs, dates):
                results[date].add(href)
            answers = dict()
            for key, value in results.items():
                answers[key] = list(value)
        return answers

    async def _get_href_date(self):
        href_elements = await self.page.xpath(self.article_xpath)
        href_values = [await element.getProperty("href") for element in href_elements]
        hrefs = [await value.jsonValue() for value in href_values]

        date_elements = await self.page.xpath(self.date_xpath)
        dates = [await element.getProperty("outerHTML") for element in date_elements]
        dates = [await date.jsonValue() for date in dates]
        dates = [BeautifulSoup(date, 'lxml') for date in dates]
        dates = [date.find("time").get("datetime") for date in dates]
        dates = [date[:10] for date in dates]

        return hrefs, dates

    @retry("_retry_policy")
    async def crawler_main_page(self, q, semaphore, years=(2013, 2023), months=(1, 12), dates=(1, 31)):
        data = {
            'q': q,
            'year1': years[1],
            'year2': years[0],
            'month1': months[1],
            'month2': months[0],
            'date1': dates[1],
            'date2': dates[0]
        }
        url = self.url.format(**data)
        async with semaphore:
            await self.__connect__()
            await self.__set_cookies__()

            await self.page.goto(url)
            await self.page.waitForXPath(self.article_xpath)
            results = await self._slow_scroll()

            collection = self.db[f"{years[0]}"]
            for key, value in results.items():
                collection.insert_one({key: value})
            await self.page.close()

    def create_datetime(self, start_date=datetime(2013, 1, 1), end_date=datetime(2023, 6, 11)):
        date_list = []
        current_date = start_date

        while current_date <= end_date:
            next_date = current_date + timedelta(days=1)
            date_list.append(((current_date.year, next_date.year), (current_date.month, next_date.month),
                              (current_date.day, next_date.day)))
            current_date = next_date
        return date_list

    def load_file(self, name, selector=None, save_path="./data.json"):
        def is_json_csv_file(file_path, method):
            _, file_extension = os.path.splitext(file_path)
            return file_extension.lower() == method
        json_data = self.db[name].find(selector)
        documents = []
        nums = 0
        for data in json_data:
            del data['_id']
            nums += len(list(data.values())[0])
            documents.append(data)
        print(f"Total document: {nums}")

        if is_json_csv_file(save_path, '.json'):
            with open(save_path, 'w') as file:
                json.dump(documents, file, indent=3)
        elif is_json_csv_file(save_path, '.csv'):
            field_names = [list(document.keys())[0] for document in documents]
            with open(save_path, 'w', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=field_names)
                writer.writeheader()
                for document in zip_longest(*[list(document.values())[0] for document in documents]):
                    writer.writerow(dict(zip(field_names, document)))

    @retry("_retry_policy")
    async def parse_detail_page(self, url, semaphore):
        async with semaphore:
            xpath_data = "//div[@class='css-1dbjc4n r-1s2bzr4']/div[@data-testid='tweetText']"
            await self.page.goto(url)
            await self.page.waitForXPath(xpath_data)
            text = await self.page.xpath(xpath_data)
            text = text[0] if text else None
            lang = await self.page.evaluate('(element) => element.getAttribute("lang")', text)
            content = await self.page.evaluate('(element) => element.textContent', text)
        return {lang: content}

    async def main(self):
        dates = self.create_datetime(start_date=datetime(2019, 8, 15), end_date=datetime(2022, 12, 31))
        semaphore = asyncio.Semaphore(1)
        tasks = [self.crawler_main_page('习近平', semaphore, *date) for date in dates]
        await asyncio.gather(*tasks)

    async def main_detail(self, year: str):
        semaphore = asyncio.Semaphore(1)
        datas = [list(value.values())[1:] for value in list(self.db[year].find())]
        n_datas = []
        for data in datas:
            n_datas.extend(data[0])
        medias = [re.search("twitter\.com/(.*?)/", media).group(1) for media in n_datas]
        medias = Counter(medias)
        top_medias = list(medias.keys())[:10]
        final_datas = []
        for tm in top_medias:
            m = [data for data in n_datas if re.search("twitter\.com/(.*?)/", data).group(1) == tm]
            final_datas.extend(random.choices(m, k=50))
        await self.__connect__()
        await self.__set_cookies__()
        tasks = [self.parse_detail_page(data, semaphore) for data in final_datas]
        results = await asyncio.gather(*tasks)
        with open(f"{year}.json", 'w') as file:
            json.dump(results, file, indent=2)


if __name__ == '__main__':
    crawler = Crawler()
    # print(crawler.search_difference("2023.json"))
    # crawler.load_file('2013', save_path='data.csv')
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(crawler.main_detail("2023"))
