import os 
import yaml 
import time
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
from dateutil.parser import parse

class GetData():

    def __init__(self,config,collection,dbname,begin_date,end_date):

        self.ranges_limits = [
            1000,
            500,
            250,
            100,
            50,
            10,
            5,
            1
        ]

        self.collection = collection
        self.dbname = dbname
        self.begin_date = begin_date
        self.end_date = end_date

        self.begin_date_year = parse(self.begin_date).date().year
        self.end_date_year = parse(self.end_date).date().year 

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
        self.db[self.collection].create_index([('meta.defaultid',  pymongo.ASCENDING)], name='unique', default_language='english',unique=True)

        logging.basicConfig(
            level=logging.INFO,
            filename= 'pipeline.log',
            datefmt='%Y-%m-%d %H:%M:%S',
            format='%(asctime)s %(levelname)-8s %(message)s')

        self.logger = logging.getLogger(__name__) 

        self.token = requests.post('https://app.dimensions.ai/api/auth.json', json={
        'username': cfg['auth'][cfg['env']]['dimension-username'],
        'password': cfg['auth'][cfg['env']]['dimension-password']
        }).json()['token']

    def get_data(self,dsl_query,skip,grid,limit_start = 0):

        limit = self.ranges_limits[limit_start] 

        self.logger.info(f'\tTrying skip:{skip},limit:{limit}')

        try:
            response = requests.post(u'https://app.dimensions.ai/api/dsl.json',
                data=dsl_query.format(
                    grid=grid,
                    limit=limit,
                    skip=skip,
                    begin_year=self.begin_date_year,
                    end_year=self.end_date_year)
                ,headers={
                    'Authorization': "JWT " + self.token
                }).json()

        except json.decoder.JSONDecodeError:
            limit,response = self.get_data(dsl_query,skip,grid,limit_start)

        else:
            if 'errors' in response:
                error = response['errors']['query']
                if 'your query is too large' in error['details'][0]:

                    limit_start += 1
                    limit,response = self.get_data(dsl_query,skip,grid,limit_start)

            return limit,response

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

    def get_dsl(self):

        if self.dbname == 'publications':
            return 'search publications where year >= {begin_year} and year <= {end_year} and research_orgs.id in ["{grid}"] return publications[all] \
                    sort by id limit {limit} skip {skip}'
        elif self.dbname == 'grants':
            return 'search grants where active_year >= {begin_year} and active_year <= {end_year} and research_orgs.id in ["{grid}"] return grants[all] \
                    sort by id limit {limit} skip {skip}'
        elif self.dbname == 'patents':
            return 'search patents where publication_year >= {begin_year} and publication_year <= {end_year} and assignees.id in ["{grid}"] return patents[all] \
                    sort by id limit {limit} skip {skip}'  
        elif self.dbname == 'clinical_trials':
            return 'search clinical_trials where active_years >= {begin_year} and active_years <= {end_year} and organizations.id in ["{grid}"] return clinical_trials[all] \
                    sort by id limit {limit} skip {skip}'
        elif self.dbname == 'datasets':
            return 'search datasets where research_orgs.id in ["{grid}"] return datasets[all] \
                    sort by id limit {limit} skip {skip}' 

    def process(self):

        search_types = ['Education','Healthcare']
        dsl_query = self.get_dsl()

        organisations = []

        for search_type in search_types:
            organisations = self.get_grids(organisations,search_type)

        organisations = list(set(organisations))

        total_organisations = len(organisations)

        for index,grid in enumerate(organisations,1):

            # Evitamos que la API nos muestre error de concurrencia.
            time.sleep(1)

            # Obtenemos el total de documentos
            total_documents = requests.post(u'https://app.dimensions.ai/api/dsl.json',
                data=dsl_query.format(
                    grid=grid,
                    limit=1,
                    skip=0,
                    begin_year=self.begin_date_year,
                    end_year=self.end_date_year)
                ,headers={
                    'Authorization': "JWT " + self.token
                }).json()['_stats']['total_count']

            self.logger.info("#-------------------------------------------------------------------------#")
            self.logger.info(f'Total Documents {total_documents} {grid}')
            self.logger.info(f'{grid}\t{index}/{total_organisations}')

            skip = 0
            while skip < total_documents:

                response = self.get_data(dsl_query,skip,grid)

                # Esto es un fix a un bug relacionado a grid.5254.6
                # Algo sucede que hace que de manera random en distintos skip y limit se caiga.
                
                if response is None:
                    limit,response = self.get_data(dsl_query,skip,grid)
                else:
                    limit,response = response

                rows = response[self.dbname]

                for lindex,row in enumerate(rows,1):

                    row['dbname'] = self.dbname  
                    defaultid = 'dimensions@{id}'.format(id=row['id'])

                    if self.db[self.collection].count_documents({'meta.defaultid' : defaultid}) > 0:
                        self.logger.info(f'ALREADY INSERTED {lindex},{limit} {grid}')
                        continue 

                    inserted_id = self.db[self.collection].insert_one({
                        'meta' : {
                            'raw' : row,
                            "defaultid" : defaultid,
                        },
                        'created_at' : datetime.datetime.now()
                    }).inserted_id

                    self.db[self.collection].update_one({"_id" : inserted_id }, {'$set' : {'meta.mongoid' : inserted_id }  })

                self.logger.info(f'Done {skip},{limit} {grid}')
                skip += limit 

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser()
    parser.add_argument('-db','--dbname',choices=['publications','grants','patents','clinical_trials','datasets'],required=True)
    parser.add_argument('-c','--collection',required=True)
    parser.add_argument('-bd','--begin_date',required=True)
    parser.add_argument('-ed','--end_date',required=True)
    parser.add_argument('-cfg','--config',default='./config.yml')

    args = parser.parse_args()

    dbname = args.dbname
    end_date = args.end_date
    begin_date = args.begin_date
    collection = args.collection
    config = args.config

    GetData(config,collection,dbname,begin_date,end_date).process()
