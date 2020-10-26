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

class CombineDDFGRIDDimension():

    def __init__(self,config):

        logging.basicConfig(
            level=logging.INFO,
            filename= 'pipeline.log',
            datefmt='%Y-%m-%d %H:%M:%S',
            format='%(asctime)s %(levelname)-8s %(message)s')

        self.logger = logging.getLogger(__name__) 

        name_yml = os.path.abspath(config)

        with open(name_yml, 'r') as ymlfile:
            cfg = yaml.load(ymlfile,Loader=yaml.BaseLoader)

        self.logger.info('Connecting Mongo')

        client = pymongo.MongoClient('mongodb://{0}:{1}@{2}/{3}'.format(
            quote(cfg['auth'][cfg['env']]['mongo-user']),
            quote(cfg['auth'][cfg['env']]['mongo-pass']),
            cfg['auth'][cfg['env']]['mongo-host'],
            cfg['auth'][cfg['env']]['mongo-db'])
        )

        self.db = client[cfg['auth'][cfg['env']]['mongo-db']]

        self.dimension_ddf = 'dimension_ddf'
        self.dimension_grid = 'dimension_grid'
        self.dimension_mixed = 'dimension_all'

        self.db[self.dimension_mixed].create_index([('meta.defaultid',  pymongo.ASCENDING)], name='unique', default_language='english',unique=True)
        self.db[self.dimension_mixed].create_index([('meta.raw.doi',  pymongo.ASCENDING)], name='meta.raw.doi', default_language='english',unique=False)

    def process(self):

        dimension_ddf = self.db[self.dimension_ddf].find({})
        dimension_grid = self.db[self.dimension_grid].find({})

        total_ddf = self.db[self.dimension_ddf].count_documents({})
        self.logger.info(f'Total DDF {total_ddf}')

        total_grid = self.db[self.dimension_grid].count_documents({})
        self.logger.info(f'Total GRID {total_grid}')

        for index,row in enumerate(dimension_ddf,1):
            
            if self.db[self.dimension_mixed].count_documents({'meta.defaultid' : row['meta']['defaultid']}) > 0:
                self.logger.info(f'ALREADY INSERTED {index}/{total_ddf} DDF')
                continue 

            self.db[self.dimension_mixed].insert_one(row)
    
            self.logger.info(f'{index}/{total_ddf} DDF')

        for index,row in enumerate(dimension_grid,1):
            
            if self.db[self.dimension_mixed].count_documents({'meta.defaultid' : row['meta']['defaultid']}) > 0:
                self.logger.info(f'ALREADY INSERTED {index}/{total_grid} GRID')
                continue 

            self.db[self.dimension_mixed].insert_one(row)

            self.logger.info(f'{index}/{total_grid} GRID')


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-cfg','--config',default='./config.yml')
    args = parser.parse_args()

    config = args.config

    CombineDDFGRIDDimension(config).process()