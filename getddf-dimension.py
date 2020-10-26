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
        self.dimension_ddf = 'dimension_ddf'
        self.ddf = 'ddf'

        self.token = requests.post('https://app.dimensions.ai/api/auth.json', json={
            'username': cfg['auth'][cfg['env']]['dimension-username'],
            'password': cfg['auth'][cfg['env']]['dimension-password']
        }).json()['token']


        self.db[self.ddf].create_index([('meta.doi',  pymongo.ASCENDING)], name='unique', default_language='english',unique=True)

    def divide_chunks(self,l, n):  
        for i in range(0, len(l), n):  
            yield l[i:i + n]

    def process(self):

        ddf_dim = self.db[self.dimension_ddf].distinct('meta.raw.doi')

        query = {}
        query["meta.doi"] = {
            u"$nin": ddf_dim
        }

        # Obtenemos todos los DOI que no se hayan descargado
        download_dois = self.db[self.ddf].find(query).distinct('meta.doi')

        # Descargamos en lotes de 200 DOI por peticion
        download_dois = list(self.divide_chunks(download_dois, 200 ))
        total_chunks = len(download_dois)

        for edoi,doi in enumerate(download_dois,1):

            # Eliminamos los elementos None dentro de la lista
            doi = list(filter(None, doi))

            # Descagamos 100 DOI desde dimension
            dsl_query = u'search publications where doi in [{d}] return publications[all] limit 1000'.format(
                d=', '.join('"{0}"'.format(d.strip()) for d in doi)
            ).encode()

            time.sleep(1)
            response = requests.post(u'https://app.dimensions.ai/api/dsl.json',data=dsl_query,headers={
                'Authorization': "JWT " + self.token
            }).json()

            try:
                # Obtenemos todas las publicaciones
                rows = response['publications']
            except KeyError:

                # En el caso que las respuestas del lote sean mas grandes
                # Dividimos el lote en chunks de 5 DOI por peticion.

                rows = []
                large_download_doi = list(self.divide_chunks(doi, 5 ))

                total_large_doi = len(large_download_doi)
                for dindex,large_doi in enumerate(large_download_doi,1):

                    # Descagamos 100 DOI desde dimension
                    dsl_query = u'search publications where doi in [{d}] return publications[all] limit 1000'.format(
                        d=', '.join('"{0}"'.format(d.strip()) for d in large_doi)
                    ).encode()

                    time.sleep(1)
                    response = requests.post(u'https://app.dimensions.ai/api/dsl.json',data=dsl_query,headers={
                        'Authorization': "JWT " + self.token
                    }).json()

                    rows.extend(response['publications'])

                    self.logger.info(f'FETCHING LARGE DOI {dindex}/{total_large_doi} : {edoi}/{total_chunks}')

            # Total DOI encontrados en Dimension
            total = len(rows)

            # Insertamos cada uno de los DOI
            for lindex,row in enumerate(rows,1):

                # Todos los DOI son publicaciones
                row['dbname'] = 'publications'

                defaultid = 'dimensions-{id}'.format(id=row['id'])

                if self.db[self.dimension_ddf].count_documents({'meta.defaultid' :defaultid }) > 0:
                    self.logger.info(f'ALREADY INSERTED DDF-DIM {lindex}/{total} : {edoi}/{total_chunks}')
                    continue 

                inserted_id = self.db[self.dimension_ddf].insert_one({
                    'meta' : {
                        'raw' : row,
                        "defaultid" : defaultid,
                    },
                    'created_at' : datetime.datetime.now()
                }).inserted_id

                self.logger.info(f'INSERTED DDF-DIM {lindex}/{total} : {edoi}/{total_chunks}')

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-cfg','--config',default='./config.yml')
    args = parser.parse_args()

    config = args.config

    GetDdfDimension(config).process()
