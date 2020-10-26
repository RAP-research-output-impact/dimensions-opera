import os
import bson
import yaml
import math
import psutil
import pandas
import logging
import pymongo
import argparse
import requests
import threading
import multiprocessing 

from dateutil.parser import parse as parser_date

from bson.objectid import ObjectId
from urllib.parse import quote
from pprint import pprint 
from sys import exit 

class CreateCollectionOrganisations():

    def __init__(self):

        logging.basicConfig(
            level=logging.INFO,
            filename= os.path.splitext(os.path.basename(__file__))[0] + '.log',
            datefmt='%Y-%m-%d %H:%M:%S',
            format='%(asctime)s %(levelname)-8s %(message)s')

        logging.getLogger('requests').setLevel(logging.CRITICAL)

        self.logger = logging.getLogger('luigi-interface') 

        name_yml = os.path.abspath('./config.yml')

        with open(name_yml, 'r') as ymlfile:
            self.cfg = yaml.load(ymlfile,Loader=yaml.BaseLoader)

        self.client = pymongo.MongoClient('mongodb://{0}:{1}@{2}/{3}'.format(
            quote(self.cfg['auth'][self.cfg['env']]['mongo-user']),
            quote(self.cfg['auth'][self.cfg['env']]['mongo-pass']),
            self.cfg['auth'][self.cfg['env']]['mongo-host'],
            self.cfg['auth'][self.cfg['env']]['mongo-db'])
        )

        self.db = self.client[self.cfg['auth'][self.cfg['env']]['mongo-db']]
        self.grid_json = 'grid_json'
        self.organisations = 'organisations'

        self.db[self.grid_json].create_index([('id',  pymongo.ASCENDING)], name='id', default_language='english',unique=True)

        self.db[self.organisations].create_index([('id',  pymongo.ASCENDING)], name='id', default_language='english',unique=False)
        self.db[self.organisations].create_index([('id_original',  pymongo.ASCENDING)], name='id_original', default_language='english',unique=True)


    def parse_row(self,row):

        data = {}
        if 'addresses' in row:
            addresses = row['addresses'][0]

            data['addresses'] = {
                'city' : addresses['city'],
                'country' : addresses['country'],
                'country_code' : addresses['country_code'],
                'lat' : addresses['lat'],
                'lng' : addresses['lng'],
                'state_code' : addresses['state_code'],
                'state' : addresses['state'],
                'postcode' : addresses['postcode']
            }

            # Lo volvemos a asignar para que se puedan generar las relaciones dentro del nodo Organisations
            data['city'] =  addresses['city']
            data['country'] = addresses['country']

        data['acronyms'] =  '@@@'.join(row['acronyms'])
        data['aliases'] = '@@@'.join(row['aliases'])

        data['wikidata'] = None
        if 'external_ids' in row:
            if 'wikidata' in row['external_ids']:
                data['wikidata'] = '@@@'.join(row['external_ids']['Wikidata']['all'])            

        data['id'] = row['id']
        data['links'] = '@@@'.join(row['links'])
        data['name'] = row['name']
        data['types'] = '@@@'.join(row['types'])
        data['wikipedia_url'] = row['wikipedia_url']
        data['defaultid'] = row['id']
        
        # Reemplazamos los valores vacios con None 
        return {k: None if not v else v for k, v in data.items() }

    # Main process
    def process(self):

        rows = self.db[self.grid_json].find({},{'relationships' : 0})
        total = self.db[self.grid_json].count_documents({})

        for index,row in enumerate(rows,1):
            
            if row['status'] == 'active':
                data = self.parse_row(row)
            elif row['status'] == 'redirected':
                data = self.db[self.grid_json].find_one({'id' : row['redirect']})
                data = self.parse_row(data)

            elif row['status'] == 'obsolete':
                continue 

            data['id_original'] = row['id']

            self.db[self.organisations].insert_one(data)

            self.logger.info(f'{index}/{total}')

        self.client.close()

if __name__ == '__main__':
    CreateCollectionOrganisations().process()
