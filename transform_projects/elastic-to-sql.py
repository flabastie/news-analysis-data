import pprint
import re
import nltk
#nltk.download('punkt')
#nltk.download('stopwords')
#nltk.download('averaged_perceptron_tagger')
from nltk.corpus import stopwords
from nltk import FreqDist
import json
from nltk.stem.snowball import FrenchStemmer
from elasticsearch import Elasticsearch, exceptions
from datetime import datetime
import pyodbc



class TransformationData():

    '''Data transformationData : Tokenizer and stopwords'''
    es = None
    index_name = None
    conn = None
    
    def __init__(self, index_name):

        '''
            Create an Elasticsearch connection object
            :param index_name: index name
            :type index_name: string
            :return: null
        '''
        
         # load credentials
        with open('./config.json') as fconfig:
            # Elastic
            credentials = json.load(fconfig)
            domain = credentials['elk']['DOMAIN']
            user = credentials['elk']['USER']
            password = credentials['elk']['PASSWORD']
            port = credentials['elk']['PORT']
            # Azure SQL
            server_sql = credentials['azure_sql']['server']
            database_sql = credentials['azure_sql']['database']
            username_sql = credentials['azure_sql']['username']
            password_sql = credentials['azure_sql']['password']   
            driver_sql= '{ODBC Driver 17 for SQL Server}'

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
            print ("\nElasticsearch client info():", info)

        except exceptions.ConnectionError as err:
            # print ConnectionError for Elasticsearch
            print ("\nElasticsearch info() ERROR:", err)
            print ("\nClient host is invalid or cluster is not running")
            # change the client's value to 'None' if ConnectionError
            self.es = None

        self.index_name = index_name

        # azure sql
        self.conn = pyodbc.connect('DRIVER='+driver_sql+
            ';SERVER='+server_sql+
            ';PORT=1433;DATABASE='+database_sql+
            ';UID='+username_sql+
            ';PWD='+password_sql)
        cursor = self.conn.cursor()

        # Confirming there is a valid connection to Azure SQL
        try:
            cursor.execute("select @@version as version")
            results = cursor.fetchone()
            # Check if anything at all is returned
            print("\n", results, "\n")              
        except exceptions.ConnectionError as err:
            # print ConnectionError for Azure SQL
            print ("\nAzure SQL ERROR:", err)

    def insert_elements(self, table_name, datalist):
        '''
            Get types list
            :param table_name: table_name
            :type table_name: str
            :param datalist: datalist
            :type datalist: list
            :return: None
        '''
        cursor = self.conn.cursor()
        if table_name == 'TYPEDOC':
            # Insert one by one.
            for item in datalist:
                cursor.execute("INSERT INTO TYPEDOC (nom_type, nbr_docs_type) VALUES (?,?)", item['key'], item['doc_count'])
                cursor.commit()
                print(item)
        if table_name == 'AUTEUR':
            # Insert one by one.
            for item in datalist:
                cursor.execute("INSERT INTO AUTEUR (nom_auteur, nbr_docs_auteur) VALUES (?,?)", item['key'], item['doc_count'])
                cursor.commit()
                print(item)
        if table_name == 'SECTION':
            # Insert one by one.
            for item in datalist:
                cursor.execute("INSERT INTO SECTION (nom_section, nbr_docs_section) VALUES (?,?)", item['key'], item['doc_count'])
                cursor.commit()
                print(item)

    def get_elements_list(self, element_name):

        '''
            get_elements_list
            :param self: self
            :type self: None
            :param element_name: element_name
            :type element_name: str
            :return: elements_list
            :rtype: list of dics (doc_count & key)
        '''
        res = self.es.search(index=self.index_name, body={
                "size": 0,
                "aggs": {
                    "Articles": {
                        "filter": {
                            "range": {
                                "date": {
                                    "gte": "2020-01-01T00:00:00.00"
                                }
                            }
                        },
                        "aggs": {
                            "GroupBy": {
                                "terms": { "field": element_name + ".keyword", "size": 10000 } 
                            }
                        }
                    }
                }
            }
        )
        elements_docs = res['aggregations']['Articles']['GroupBy']['buckets']
        # sorting by doc_count desc
        elements_list  = [item for item in sorted(elements_docs, key = lambda i: i['doc_count'], reverse=True)]
        # list of dics (doc_count & key)
        return elements_list

    def get_documents(self, from_nb, size_docs):

        '''
            select all from index nb to size
            :param from_nb: from_nb
            :type from_nb: int
            :param size_docs: size_docs
            :type size_docs: int
            :return: documents_list
            :rtype: list of dicts
        '''
        res = self.es.search(index=self.index_name, body={
                "from" : from_nb, 
                "size" : size_docs,
                "query": {
                    "match_all": {},
                },
                "_source": {
                    "include": ["author", "date", "doc_token", "link", "section", "title", "type"]
                }      
            }
        )
        return res['hits']['hits']

    def insert_documents(self, docs_list):
        '''
            insert_docs in DOCUMENT table
            :param datalist: datalist
            :type datalist: list
            :return: None
        '''
        cursor = self.conn.cursor()
        
        # Insert one by one.
        for i, doc in enumerate(docs_list):
            # data to insert
            id_doc = doc['_id']
            author = doc['_source']['author']
            date = doc['_source']['date']
            nbr_mots = len(doc['_source']['doc_token'])
            link = doc['_source']['link']
            section = doc['_source']['section']
            title = doc['_source']['title']
            type_doc = doc['_source']['type']

            # auteur
            if author is None:
                id_auteur = 1
            else:
                cursor.execute("select id_auteur, nom_auteur from [dbo].[AUTEUR] where nom_auteur = ?", author)
                Auteur_id_auteur = cursor.fetchone()
                cursor.commit()
                if(Auteur_id_auteur):
                    id_auteur = Auteur_id_auteur[0]
                else:
                    id_auteur = 1
            cursor.commit()

            # section
            if section is None:
                id_section = 1
            else:
                cursor.execute("select id_section, nom_section from [dbo].[SECTION] where nom_section = ?", section)
                Section_id_section  = cursor.fetchone()
                if(Section_id_section):
                    id_section = Section_id_section[0]
                else:
                    id_section = 1
            cursor.commit()

            # type
            if type_doc is None:
                id_type = 1
            else:
                cursor.execute("select id_type, nom_type from [dbo].[TYPEDOC] where nom_type = ?", type_doc)
                Type_id_type = cursor.fetchone()
                if(Type_id_type):
                    id_type = Type_id_type[0]
                else:
                    id_type = 1
            cursor.commit()

            print('\n')
            print(i)
            print(id_doc)
            print(author)
            print(date)
            print(nbr_mots)
            print(link)
            print(section)
            print(title)
            print(type_doc)
            print(id_auteur)
            print(id_section)
            print(id_type)

            # document
            cursor.execute("""INSERT INTO DOCUMENT (id_elastic, 
                                                    date_document, 
                                                    link, 
                                                    titre, 
                                                    nbr_mots, 
                                                    Auteur_id_auteur, 
                                                    Type_id_type, 
                                                    Section_id_section) 
                                                    VALUES (?,?,?,?,?,?,?,?)""", 
                            id_doc, date, link, title, nbr_mots, id_auteur, id_type, id_section)
            cursor.commit()
        
        cursor.close()
        print("Total docs = ", len(docs_list), '\n')

# -------------------------
# main
# -------------------------

# instanciation
transformData = TransformationData('news_analysis')


elements_list = transformData.get_elements_list("author")
#transformData.insert_elements('AUTEUR', elements_list)
#pprint.pprint(elements_list)

elements_list = transformData.get_elements_list("type")
#transformData.insert_elements('TYPEDOC', elements_list)
#pprint.pprint(elements_list)

elements_list = transformData.get_elements_list("section")
#transformData.insert_elements('SECTION', elements_list)
#pprint.pprint(elements_list)


documents_list = transformData.get_documents(0, 9999)
#pprint.pprint(documents_list)
transformData.insert_documents(documents_list)


    