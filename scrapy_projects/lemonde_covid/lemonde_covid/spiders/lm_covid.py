import scrapy
from scrapy import FormRequest
import re
from datetime import datetime
import nltk
from nltk.corpus import stopwords
from nltk import FreqDist
from nltk.stem.snowball import FrenchStemmer
import json

# scrapy crawl lm_covid

class LmScraperSpider(scrapy.Spider):
    ''' Spider Scraper for LeMonde.fr '''
    date_start = '16/12/2020'
    date_end = 'xx/xx/2020'
    page_number = 1
    url_list = f"https://www.lemonde.fr/recherche/?search_keywords=covid&start_at={date_start}&end_at={date_end}&search_sort=date_asc&page="
    name = 'lm_covid'
    allowed_domains = ['lemonde.fr']
    start_urls = ['https://secure.lemonde.fr/sfuser/connexion']
    custom_sw = []
    credentials = ''
    user = ''
    password = ''

    def __init__(self):
        '''
            Create a spider scraper
            :param: None
            :return: None
        '''
        # load custom stopwords
        with open('custom_sw.json') as f:
            self.custom_sw = json.load(f)
        # load credentials
        with open('config.json') as fconfig:
            self.credentials = json.load(fconfig)
        
        # Confirming credentials loaded
        try:
            self.user = self.credentials['lemonde']['user']
            self.password = self.credentials['lemonde']['password']

        except exceptions.ConnectionError as err:
            # print ConnectionError for Elasticsearch
            print ("\nError loading credentials")

    # -------------------------
    # Function document_cleaner
    # -------------------------

    def document_cleaner(self, raw_content):
        '''
            document_cleaner
            :param raw_content: raw_content
            :type raw_content: str
            :return cleaned_string: cleaned_string
            :rtype: str
        '''
        cleaned_list =[]
        cleanr = re.compile('<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});')
        # iterate in raw_content list
        for paragraph in raw_content:
            # remove html tags from paragraph (string)
            cleantext = re.sub(cleanr, '', paragraph)
            cleaned_list.append(cleantext)
        # list concatenation
        cleaned_string = ' '.join([str(elem) for elem in cleaned_list])
        # return string
        return cleaned_string

    # ------------------------------
    # Function tokenizer_punctuation
    # ------------------------------

    def tokenizer_punctuation(self, sample_text):
        '''
            tokenizer_punctuation : removes punctuation
            :param sample_text: text content
            :type sample_text: str
            :return tokenizer: tokenized words
            :rtype: list
        '''
        # tokenizer definition
        tokenizer = nltk.RegexpTokenizer(r'\w+')
        # return text without punctuation
        return tokenizer.tokenize(sample_text)

    # ------------------------------
    # Function remove_stopwords
    # ------------------------------

    def remove_stopwords(self, custom_sw, content):
        '''
            remove_stopwords : removes stopwords
            :param custom_sw: stopwords
            :type custom_sw: list
            :param content: content
            :type content: list
            :return doc_token: words without stopwords
            :rtype: list
        '''
        temp_tokens = []
        doc_token = []
        # tokenise content
        temp_tokens = self.tokenizer_punctuation(content.lower())
        doc_token += [w for w in temp_tokens if not w in self.custom_sw]
        return doc_token

    # ------------------------------
    # Function french_stemmer
    # ------------------------------

    def french_stemmer(self, doc_token):
        '''
            french_stemmer : stemes stopwords
            :param doc_token: tokenized words
            :type doc_token: list
            :return doc_stem: stemmed words
            :rtype: list
        '''
        # french stemmer
        stemmer = FrenchStemmer()
        doc_stem = []
        # stemming
        doc_stem += [stemmer.stem(w) for w in doc_token]
        # return stemmed tokens
        return doc_stem

    # -------------------------
    # Function parse
    # -------------------------

    def parse(self, response):
        '''
            parse : parse function
            :param response: response
            :type response: object
            :return yield: iterator
            :rtype: iterator
        '''
        print('\n')
        csrf_token = response.xpath("//input[@id='connection__token']/@value").get()
        yield FormRequest.from_response(response,
          formxpath="//form[@name='connection']",
          formdata={'connection[_token]':csrf_token, 
         'connection[mail]':self.user, 
         'connection[password]':self.password, 
         'connection[stay_connected]':'1'},
          callback=(self.after_login))

    # -------------------------
    # Function after_login
    # -------------------------

    def after_login(self, response):
        '''
            after_login : after login processinng
            :param response: response
            :type response: object
            :return response: response object
            :rtype: object
        '''
        '''if response.xpath("//span[@class='login-info']/text()").get():
            print('\n---------------')
            print('  logged in')
            print(f"  User-Agent : {response.request.headers['User-Agent']}")
            print('---------------\n')
        else:
            print('\n---------------')
            print('  ERROR')
            print('---------------\n')'''
        print('URL: ' + response.request.url)
        url = self.url_list + str(self.page_number)
        return response.follow(url, self.parse_links_list)

    # -------------------------
    # Function parse_links_list
    # -------------------------

    def parse_links_list(self, response):
        '''
            parse_links_list : process scraped links
            :param response: response
            :type response: object
            :return response: response object
            :rtype: object
        '''
        last_page = response.xpath("(//a[@class='river__pagination river__pagination--page-search ' ])[last()]/text()").get()
        rows = response.xpath("//section[@class='teaser teaser--inline-picture ']")
        for item in rows:
            document_title = item.xpath(".//h3[@class='teaser__title']/text()").get()
            document_link = item.xpath(".//a[@class='teaser__link teaser__link--kicker']/@href").get()
            document_teaser = item.xpath(".//p[@class='teaser__desc']/text()").get()
            splitted_link = re.split('/', document_link)
            document_date = datetime(int(splitted_link[5]), int(splitted_link[6]), int(splitted_link[7]))
            document_section = splitted_link[3]
            document_type = splitted_link[4]
            document_author = item.xpath(".//span[@class='meta__author meta__author--page']/text()").get()
            yield response.follow(url=document_link, callback=(self.parse_document),
              meta={
                'link':document_link, 
                'date':document_date, 
                'section':document_section, 
                'type':document_type, 
                'title':document_title, 
                'teaser':document_teaser, 
                'author':document_author})

        if self.page_number < int(last_page):
            print('\n--------------------')
            print(f"    Page {self.page_number}")
            print('--------------------\n')
            self.page_number += 1
            new_url = self.url_list + str(self.page_number)
            yield scrapy.Request(url=new_url, callback=(self.parse_links_list))
        else:
            print('\n--------------------')
            print(f" {self.page_number} pages scrapped")
            print('--------------------\n')

    # -------------------------
    # Function parse_document
    # -------------------------

    def parse_document(self, response):
        '''
            parse_document : process scraped document
            :param response: response
            :type response: object
            :return yield: iterator
            :rtype: iterator
        '''
        document_link = response.request.meta['link']
        document_date = response.request.meta['date']
        document_section = response.request.meta['section']
        document_type = response.request.meta['type']
        document_title = response.request.meta['title']
        document_teaser = response.request.meta['teaser']
        document_author = response.request.meta['author']
        document_html = response.xpath('//article/p | //article/h2').getall()
        if len(document_html) == 0:
            document_html = response.xpath("//article[@class='article article--longform  article--content']/section[@class='article__content']/p | //article[@class='article article--longform  article--content']/section[@class='article__content']/h2 | //article[@class='article article--longform  article--content']/section[@class='article__content']/blockquote").getall()
        if len(document_html) == 0:
            document_html = response.xpath("//article[@class='article article--longform article--longform-nocover  article--content']/section[@class='article__content']/p | //article[@class='article article--longform article--longform-nocover  article--content']/section[@class='article__content']/h2 | //article[@class='article article--longform article--longform-nocover  article--content']/section[@class='article__content']/blockquote").getall()
        
        # remove html-tags from document_html and concatenate
        doc_text = self.document_cleaner(document_html)
        # concatenate title + teaser + doc_text
        doc_all = document_title + '. ' + document_teaser + ' ' + doc_text
        # empty field tokens
        doc_token = self.remove_stopwords(self.custom_sw, doc_all)
        # empyty field stemming
        doc_stem = self.french_stemmer(doc_token)
        
        yield {
            'link':document_link, 
            'date':document_date, 
            'section':document_section, 
            'type':document_type, 
            'title':document_title, 
            'teaser':document_teaser, 
            'author':document_author, 
            'content_html':document_html, 
            'content_text':doc_text, 
            'content_all':doc_all,
            'doc_token': doc_token,
            'doc_stem': doc_stem
            }

