import os 
import time
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

class GetDdfDimension():

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
        self.dimension_abstract = 'dimension_abstract'

        self.token = requests.post('https://app.dimensions.ai/api/auth.json', json={
            'username': cfg['auth'][cfg['env']]['dimension-username'],
            'password': cfg['auth'][cfg['env']]['dimension-password']
        }).json()['token']

        self.db[self.dimension_abstract].create_index([('meta.defaultid',  pymongo.ASCENDING)], name='unique', default_language='english',unique=True)


    def get_grids(self,organisations,search_type='Education',country='Denmark'):

        page = 1
        while True:

            self.logger.info(f'grid.ac {search_type} {country} Page:{page}')

            response = requests.get('https://www.grid.ac/institutes', headers={
                'authority': 'www.grid.ac',
                'pragma': 'no-cache',
                'cache-control': 'no-cache',
                'dnt': '1',
                'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36',
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'sec-fetch-site': 'same-origin',
                'sec-fetch-mode': 'navigate',
                'sec-fetch-user': '?1',
                'sec-fetch-dest': 'document',
                'accept-language': 'en,es-ES;q=0.9,es;q=0.8',
            }, params=(
                ('commit', 'Filter'),
                ('page', page),
                ('search[city_filter]', ''),
                ('search[country_filter]', country),
                ('search[geonames_filter]', ''),
                ('search[per_page]', '25'),
                ('search[query]', ''),
                ('search[type_filter]', search_type),
                ('utf8', '\u2713'),
            ))

            root = lxml.html.fromstring(response.content)

            rows = root.xpath("/html/body/div[1]/div[1]/div/section/div/div/div[2]/div/div/div/div")

            if len(rows) > 0:
                for row in rows:
                    if row.xpath('./@class')[0] == 'search-info':
                        continue 

                    grid = row.xpath('./div/div[2]/text()')[0]
                    organisations.append(grid)

                page += 1
            else:
                break

        self.logger.info('Total Organisations {search_type} {total}'.format(search_type=search_type,total=len(organisations)))
        
        return organisations

    def process(self):

        search_types = ['Education','Healthcare']

        organisations = []

        for search_type in search_types:
            organisations = self.get_grids(organisations,search_type)

        for grid in organisations:

            time.sleep(1)

            dsl_query = f'search publications where year >= "2014" and year <= "2019" and research_orgs.id = "{grid}" \
            return publications[id] sort by id limit 1 skip 0'

            total_count = requests.post(u'https://app.dimensions.ai/api/dsl.json',data=dsl_query,headers={
                'Authorization': "JWT " + self.token
            }).json()['_stats']['total_count']

            skip = 0
            while True:

                time.sleep(1)

                dsl_query = f'search publications where year >= "2014" and year <= "2019" and research_orgs.id = "{grid}" \
                return publications[id + abstract + title + doi] sort by id limit 1000 skip {skip}'

                response = requests.post(u'https://app.dimensions.ai/api/dsl.json',data=dsl_query,headers={
                    'Authorization': "JWT " + self.token
                }).json()['publications']

                if not response:
                    break 

                for index,row in enumerate(response,1):

                    defaultid = 'dimensions@{id}'.format(id=row['id'])

                    if self.db[self.dimension_abstract].count_documents({'meta.defaultid' : defaultid}) > 0:
                        self.logger.info(f'ALREADY INSERTED {skip}:{index}/{total_count} {grid}')
                        continue 

                    self.db[self.dimension_abstract].insert_one({
                        'meta' : {
                            'raw' : row,
                            "defaultid" : defaultid,
                        },
                        'created_at' : datetime.datetime.now()
                    }).inserted_id

                    self.logger.info(f'INSERTED {skip}:{index}/{total_count} {grid}')

                skip += 1000

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-cfg','--config',default='./config.yml')
    args = parser.parse_args()

    config = args.config

    GetDdfDimension(config).process()
