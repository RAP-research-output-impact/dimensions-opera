import re
import os
import yaml
import hashlib 
import logging
import pymongo
import argparse
import requests

from dateutil.parser import parse as parser_date
from urllib.parse import quote
from pprint import pprint 
from sys import exit 

class FixDefaultIdSlash():

    def __init__(self):

        logging.basicConfig(
            level=logging.INFO,
            filename= 'pipeline.log',
            datefmt='%Y-%m-%d %H:%M:%S',
            format='%(asctime)s %(levelname)-8s %(message)s')

        self.logger = logging.getLogger('pipeline')
         
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

    def process(self):
        rows = self.db['dimension_all'].find({},{'meta.defaultid' : 1})

        for row in rows:

            defaultid = row['meta']['defaultid'].replace("-","_")

            self.db['dimension_all'].update_one({"_id" : row['_id'] }, {'$set' : {'meta.defaultid' : defaultid } })


if __name__ == '__main__':
    FixDefaultIdSlash().process()