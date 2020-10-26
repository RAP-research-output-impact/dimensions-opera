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

class KarenExtraRecordParse():

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


        self.collection = 'KarenExtraRecords'
        self.extra_documents = 'extra_documents'
        self.organisations = 'organisations'

        self.db[self.organisations].create_index([('name',  pymongo.ASCENDING)], name='name', default_language='english',unique=False)

    def process(self):

        data = self.db[self.collection].find({})
        total = self.db[self.collection].count_documents({})

        for index,row in enumerate(data,1):

            doi = None 
            dimension_id = None

            if row['Dim link'].startswith('https'):
                dimension_id = urlparse(row['Dim link']).path.split("/")[-1].strip()
            else:
                doi = row['Dim link'].strip()

            try:
                grid = self.db[self.organisations].find_one({'id_original' : row['Grid']})['id']
            except TypeError:
                try:
                    grid = self.db[self.organisations].find_one({'name' : row['Organisation']})['id']
                except TypeError:
                    # No hace match ni el ID o el nombre de la organizacion.
                    # Comunmente son Jounal 
                    # https://app.dimensions.ai/details/publication/pub.1065208274
                    continue

            if doi is not None:
                dsl_query = f'search publications where doi = "{doi}" return publications'
            else:
                dsl_query = f'search publications where id = "{dimension_id}" return publications'

            time.sleep(1)
            data = requests.post(u'https://app.dimensions.ai/api/dsl.json',
                data=dsl_query
                ,headers={
                    'Authorization': "JWT " + self.token
                }).json()


            self.db[self.extra_documents].insert_one({
                'link' : row['Dim link'],
                'grid' :  grid,
                'dimension_id' : dimension_id,
                'doi' : doi
            })

            self.logger.info(f'{index}/{total} INSERTED')
            
if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-cfg','--config',default='./config.yml')
    args = parser.parse_args()

    config = args.config

    KarenExtraRecordParse(config).process()