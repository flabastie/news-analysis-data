# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import logging
from elasticsearch import Elasticsearch, exceptions
import json

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

class ElasticSearchPipeline(object):
    ''' Import data to ElasticSerach db '''
    index_name = "news_analysis"
    #index_name = "last_days_covid"
    es = None

    def open_spider(self, spider):
        '''
            Connection to ElasticSerach db
            :param spider: spider
            :type spider: object
            :return: None
        '''
        logging.warning("SPIDER OPENED FROM PIPELINE")

        # load credentials
        with open('config.json') as fconfig:
            credentials = json.load(fconfig)
            domain = credentials['elk']['DOMAIN']
            user = credentials['elk']['USER']
            password = credentials['elk']['PASSWORD']
            port = credentials['elk']['PORT']

        self.es = Elasticsearch(
            [domain],
            http_auth=(user, password),
            scheme="https", 
            port=port,)

        # Confirming there is a valid connection to Elasticsearch
        try:
            # use the JSON library's dump() method for indentation
            info = json.dumps(self.es.info(), indent=4)
            # pass client object to info() method
            print ("Elasticsearch client info():", info)

        except exceptions.ConnectionError as err:
            # print ConnectionError for Elasticsearch
            print ("\nElasticsearch info() ERROR:", err)
            print ("\nClient host is invalid or cluster is not running")
            # change the client's value to 'None' if ConnectionError
            self.es = None

    def close_spider(self, spider):
        ''' 
            Close connection to ElasticSerach db
            :param spider: spider
            :type spider: object
            :return: None
        '''
        logging.warning("SPIDER CLOSED FROM PIPELINE")

    def process_item(self, item, spider):
        ''' 
            Insert data to ElasticSerach db 
            :param item: data document
            :type item: object
            :param spider: spider
            :type spider: object
            :return: None
        '''
        self.es.index(index=self.index_name, body=item)
        return item