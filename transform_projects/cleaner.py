# coding: utf-8

import pymongo
import pprint
import re
from bson.objectid import ObjectId
import nltk
# nltk.download('punkt')
# nltk.download('stopwords')
# nltk.download('averaged_perceptron_tagger')
from nltk.corpus import stopwords
from nltk import FreqDist
import json
from nltk.stem.snowball import FrenchStemmer
import config as config

class Cleaner():

    client = None
    db = None
    collection = None
    user = config.user
    password = config.password
    cluster = config.cluster
    custom_sw = None

    def __init__(self, collection_name):
        # mongodb connection
        self.client = pymongo.MongoClient(f"mongodb+srv://{self.user}:{self.password}@{self.cluster}/<dbname>?retryWrites=true&w=majority")
        self.db = self.client["news_analysis"]
        self.collection = self.db[collection_name]

        # load custom stopwords
        with open('stop_words/custom_sw.json') as f:
            self.custom_sw = json.load(f)

    # ------------------------------
    # Function clean_all
    # ------------------------------

    def clean_all_test(self):
        
        print("\nList of collections \n-------------------")
        #list the collections
        for coll in self.db.list_collection_names():
            print(coll)
        print("-------------------\n")
        # close connection
        self.client.close()

    # -------------------------
    # Function document_cleaner
    # -------------------------

    def document_cleaner(self, raw_content):
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
        # tokenizer definition
        tokenizer = nltk.RegexpTokenizer(r'\w+')
        # return text without punctuation
        return tokenizer.tokenize(sample_text)

    # ------------------------------
    # Function clean_all
    # ------------------------------

    def clean_all(self):
        # counter for visual checking
        counter = 0
        # french stemmer
        stemmer = FrenchStemmer()
        # var for stemming
        tokens = []
        doc_token = []
        doc_stem = []
        # iterate in all documents
        for document in self.collection.find():
            # remove html-tags from document_html and concatenate
            doc_text = self.document_cleaner(document['document_html'])
            # concatenate title + teaser + doc_text
            doc_all = document['document_title'] + '. ' + document['document_teaser'] + ' ' + doc_text
            # tokenise doc_all
            tokens = self.tokenizer_punctuation(doc_all.lower())
            doc_token += [w for w in tokens if not w in self.custom_sw]
            # stemming
            doc_stem += [stemmer.stem(w) for w in doc_token]
            # update document
            self.collection.update_one({'_id': document['_id']}, 
                                            {"$set": {"document_text": doc_text, 
                                            "document_all": doc_all, 
                                            "doc_token": doc_token,
                                            "doc_stem": doc_stem} }, upsert=False)
            # counter for visual checking
            counter += 1
            print(counter, ' - ', document['_id'] )

        # close connection
        self.client.close()


# -------------------------
# main
# -------------------------

# instanciation
cleaner = Cleaner('3months_covid')
# function clean_all()
#cleaner.clean_all_test()
cleaner.clean_all()



