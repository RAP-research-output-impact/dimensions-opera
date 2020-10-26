import os 
import yaml 
import time
import json
import pymongo
import logging
import argparse
import requests 
import datetime

from sys import exit 
from pprint import pprint
from urllib.parse import quote
from urllib.parse import urlparse
from dateutil.parser import parse

class KarenExtraRecordDownloadDimension():

    def __init__(self,config):

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

        logging.basicConfig(
            level=logging.INFO,
            filename= 'pipeline.log',
            datefmt='%Y-%m-%d %H:%M:%S',
            format='%(asctime)s %(levelname)-8s %(message)s')

        self.logger = logging.getLogger('pipeline') 

        self.token = requests.post('https://app.dimensions.ai/api/auth.json', json={
        'username': cfg['auth'][cfg['env']]['dimension-username'],
        'password': cfg['auth'][cfg['env']]['dimension-password']
        }).json()['token']

        self.dimension_all = 'dimension_all'
        self.extra_documents = 'extra_documents'

    def process(self):

        data = self.db[self.extra_documents].find({})
        total = self.db[self.extra_documents].count_documents({})

        for index,row in enumerate(data,1):
            
            doi = row['doi']
            dimension_id = row['dimension_id']

            if doi is not None:
                dsl_query = f'search publications where doi = "{doi}" return publications[all]'
            else:
                dsl_query = f'search publications where id = "{dimension_id}" return publications[all]'

            time.sleep(1)

            response = requests.post(u'https://app.dimensions.ai/api/dsl.json',
                data=dsl_query
                ,headers={
                    'Authorization': "JWT " + self.token
                }).json()['publications']

            response = response[0]

            response['dbname'] = 'publications'
            defaultid = 'dimensions-{id}'.format(id=response['id'])

            if self.db[self.dimension_all].count_documents({'meta.defaultid' : defaultid}) > 0:
                self.logger.info(f'{index}/{total} ALREADY INSERTED')
                continue 

            inserted_id = self.db[self.dimension_all].insert_one({
                'meta' : {
                    'raw' : response,
                    "defaultid" : defaultid,
                },
                'created_at' : datetime.datetime.now()
            }).inserted_id

            self.logger.info(f'{index}/{total} INSERTED')


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-cfg','--config',default='./config.yml')
    args = parser.parse_args()

    config = args.config

    KarenExtraRecordDownloadDimension(config).process()



