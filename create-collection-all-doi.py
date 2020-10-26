import os 
import yaml 
import json
import pymongo
import logging
import argparse
import requests 
import datetime
import lxml.html

from sys import exit 
from pprint import pprint
from urllib.parse import quote

class CreateCollectionAllDoi():

    def __init__(self,config):

        logging.basicConfig(
            level=logging.INFO,
            filename= 'pipeline.log',
            datefmt='%Y-%m-%d %H:%M:%S',
            format='%(asctime)s %(levelname)-8s %(message)s')

        self.logger = logging.getLogger('pipeline') 

        name_yml = os.path.abspath(config)

        with open(name_yml, 'r') as ymlfile:
            cfg = yaml.load(ymlfile,Loader=yaml.BaseLoader)

        client = pymongo.MongoClient('mongodb://{0}:{1}@{2}/{3}'.format(
            quote(cfg['auth'][cfg['env']]['mongo-user']),
            quote(cfg['auth'][cfg['env']]['mongo-pass']),
            cfg['auth'][cfg['env']]['mongo-host'],
            cfg['auth'][cfg['env']]['mongo-db'])
        )

        self.db = client[cfg['auth'][cfg['env']]['mongo-db']]

        self.dimension_ddf = 'dimension_ddf'
        self.dimension_grid = 'dimension_grid'
        self.dimension_all_flags = 'dimension_all_flags'
        self.ddf = 'ddf'

        self.db[self.dimension_all_flags].create_index([('doi',  pymongo.ASCENDING)], name='unique', default_language='english',unique=True)


    def process(self):

        query = {
            'meta.raw.doi' : {
                "$exists": True
            },
            'meta.raw.dbname' : 'publications'
        }
        projection = {
            'meta.raw.doi' : 1
        }

        grid_doi = self.db[self.dimension_grid].find(query,projection)
        ddf_doi = self.db[self.dimension_ddf].find(query,projection)
        ddf = self.db[self.ddf].find({},{'meta.doi' : 1})

        ddf = [doi['meta']['doi'] for doi in ddf]
        grid_doi = [doi['meta']['raw']['doi'] for doi in grid_doi]
        ddf_doi = [doi['meta']['raw']['doi'] for doi in ddf_doi]

        dois = list(set(grid_doi + ddf_doi + ddf))
        total = len(dois)

        for index,doi in enumerate(dois,1):
            if self.db[self.dimension_all_flags].count_documents({'doi':doi}) > 0:
                self.logger.info(f'ALREADY EXISTS {index}/{total}')
                continue 

            self.db[self.dimension_all_flags].insert_one({'doi': doi})
            self.logger.info(f'INSERTED {index}/{total}')

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-cfg','--config',default='./config.yml')
    args = parser.parse_args()

    config = args.config

    CreateCollectionAllDoi(config).process()



