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

class GetDdf():

    def __init__(self,config,years):

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
        self.ddf = 'ddf'

        self.token = requests.post('https://app.dimensions.ai/api/auth.json', json={
            'username': cfg['auth'][cfg['env']]['dimension-username'],
            'password': cfg['auth'][cfg['env']]['dimension-password']
        }).json()['token']

        self.routes = [
                {'url' : 'http://ws.dads.dtic.dk/service/doi/source/orbit/year/{y}' ,'grid' : 'grid.5170.3'},
                {'url' : 'http://ws.dads.dtic.dk/service/doi/source/rdb_vbn/year/{y}', 'grid' : 'grid.5117.2' },
                {'url' : 'http://ws.dads.dtic.dk/service/doi/source/rdb_au/year/{y}' ,'grid' : 'grid.7048.b'},
                {'url' : 'http://ws.dads.dtic.dk/service/doi/source/rdb_cbs/year/{y}' ,'grid' : 'grid.4655.2'},
                {'url' : 'http://ws.dads.dtic.dk/service/doi/source/rdb_ruc/year/{y}' ,'grid' : 'grid.11702.35'},
                {'url' : 'http://ws.dads.dtic.dk/service/doi/source/rdb_ku/year/{y}' ,'grid' : 'grid.5254.6'},
                {'url' : 'http://ws.dads.dtic.dk/service/doi/source/rdb_sdu/year/{y}' ,'grid' : 'grid.10825.3e'},
                {'url' : 'http://ws.dads.dtic.dk/service/doi/source/rdb_itu/year/{y}' ,'grid' : 'grid.32190.39'},
            ]

        self.years = list(map(int,  years.split(",")))

        self.db[self.ddf].create_index([('meta.doi',  pymongo.ASCENDING)], name='unique', default_language='english',unique=True)


    def process(self):

        total_routes = len(self.routes)

        for rindex,row in enumerate(self.routes,1):
            for year in self.years:

                url = row['url'].format(y=year)
                grid = row['grid']

                response = requests.get(url, headers={
                    'Connection': 'keep-alive',
                    'DNT': '1',
                    'Upgrade-Insecure-Requests': '1',
                    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
                    'Accept-Encoding': 'gzip, deflate',
                    'Accept-Language': 'en,es-ES;q=0.9,es;q=0.8',
                })

                dois = list(set(response.text.split("\n")))
                dois = list(filter(None, dois)) 

                for index,doi in enumerate(dois,1):

                    doi = doi.strip().lower()

                    if self.db[self.ddf].count_documents({'meta.doi' : doi}) > 0:
                        self.logger.info(f'ALREADY INSERTED {grid}:{year} {rindex}/{total_routes}')
                        continue 

                    inserted_id = self.db[self.ddf].insert_one({
                        'meta' : {
                            "defaultid" : f'ddf-{doi}',
                            'url' : url,
                            'doi' : doi,
                            'grid' : grid,
                            'year' : year
                        },
                        'created_at' : datetime.datetime.now()
                    }).inserted_id

                    self.db[self.ddf].update_one({"_id" : inserted_id }, {'$set' : {'meta.mongoid' : inserted_id }  })

                self.logger.info(f'INSERTED {grid}:{year} {rindex}/{total_routes}')

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-cfg','--config',default='./config.yml')
    parser.add_argument('-y','--years',default='2013,2014,2015,2016,2017,2018,2019')
    args = parser.parse_args()

    config = args.config
    years = args.years

    GetDdf(config,years).process()


