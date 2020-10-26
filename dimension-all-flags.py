import os 
import yaml 
import json
import pymongo
import logging
import requests 
import datetime
import argparse

from sys import exit 
from pprint import pprint
from urllib.parse import quote

class DimensionAllFlags():

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

        self.db[self.dimension_ddf].create_index([('meta.raw.doi',  pymongo.ASCENDING)], name='meta.raw.doi', default_language='english',unique=False)
        self.db[self.dimension_grid].create_index([('meta.raw.doi',  pymongo.ASCENDING)], name='meta.raw.doi', default_language='english',unique=False)

    def process(self):
        dois = self.db[self.dimension_all_flags].find({})
        total = self.db[self.dimension_all_flags].count_documents({})

        for index,doi in enumerate(dois,1):

            # Verificamos que exista en los doi obtenidos desde DDF y encontro match en DIM
            if self.db[self.dimension_ddf].count_documents({'meta.raw.doi' : doi['doi']}) > 0:
                dim_ddf = True 
            else:
                dim_ddf = False

            self.db[self.dimension_all_flags].update_one({"_id" : doi['_id'] }, {'$set' : {'dim_ddf' : dim_ddf }  })

            if self.db[self.dimension_grid].count_documents({'meta.raw.doi' : doi['doi']}) > 0:
                dim_grid = True
            else:
                dim_grid = False

            self.db[self.dimension_all_flags].update_one({"_id" : doi['_id'] }, {'$set' : {'dim_grid' : dim_grid }  })

            self.logger.info('{index}/{total} {doi}'.format(
                index=index,
                total=total,
                doi=doi['doi']
            ))

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-cfg','--config',default='./config.yml')
    args = parser.parse_args()

    config = args.config

    DimensionAllFlags(config).process()



