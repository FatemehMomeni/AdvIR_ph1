from pyexcel import get_sheet
import requests
from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch
from selenium.webdriver.chrome.options import Options
from selenium import webdriver


options = Options()
options.add_argument('--headless')
options.add_argument('--disable-gpu')
browser = webdriver.Chrome(r"F:\Mine\softwares\chrome\chrome_driver\chromedriver.exe", options=options)

ted_list = []

path = 'data.csv'

ted_sheet = get_sheet(file_name=path, name_columns_by_row=0)

for title, author, date, views, likes, link in zip(
        ted_sheet.column['title'],
        ted_sheet.column['author'],
        ted_sheet.column['date'],
        ted_sheet.column['views'],
        ted_sheet.column['likes'],
        ted_sheet.column['link'],
):
    #link = link.strip()
    talk_dict = {
        'title': title,
        'author': author,
        'date': date,
        'views': views,
        'likes': likes,
        'link': link,
        'transcript': ''
    }

    # crawling in the pages and scraping data
    complete_text = ''
    talk_url = link + '/transcript'
    browser.get(talk_url)

    html = browser.page_source
    soup = BeautifulSoup(html, 'lxml')
    for i in soup.findAll("span", {"class": "cursor-pointer"}):
        complete_text += i.text

    complete_text = complete_text.replace("\n", " ")

    talk_dict['transcript'] = complete_text
    ted_list.append(talk_dict)

# Create the client instance
es = Elasticsearch("http://localhost:9200")

es.indices.create(index='ted_index', body={
    'settings': {
        'analysis': {
            'analyzer': {
                'my_analyzer': {
                    'tokenizer': 'standard',
                    "filter": ["lowercase", "my_stop"]
                }
            },
            'filter': {
                'my_stop': {
                    "type": "stop",
                    "stopwords":  "_english_"
                }
            }
        }
    },
    'mappings': {
        "properties": {
            'title': {
                "type": "text",
                "index": "true",
                "store": 'true'
            },
            'author': {
                "type": "text",
                "index": "true",
                "store": 'true'
            },
            'date': {
                "type": "text",
                "index": "true",
                "store": 'true'
            },
            'views': {
                "type": "text",
                "index": "true",
                "store": 'true'
            },
            'likes': {
                "type": "text",
                "index": "true",
                "store": 'true'
            },
            'link': {
                "type": "text",
                "index": "true",
                "store": 'true'
            },
            'transcript': {
                "type": "text",
                "index": "true",
                "store": 'true',
                "analyzer": "my_analyzer"
            },
        }
    }
})

bulk_data = []
i = 0
for each in ted_list:
    op_dict = {
        "index": {
            "_index": 'ted_index',
            "_id": i
        }
    }
    i = i+1
    data_dict = each

    bulk_data.append(op_dict)
    bulk_data.append(data_dict)

res = es.bulk(index='ted_index', body=bulk_data)
