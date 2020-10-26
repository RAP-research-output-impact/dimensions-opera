import os 
import yaml 
import json
import py2neo
import pandas
import pymongo
import logging
import requests 
import datetime
import argparse

from sys import exit 
from pprint import pprint
from urllib.parse import quote

class InsertOnlyDoiDDF():

    def __init__(self,config):

        logging.basicConfig(
            level=logging.INFO,
            filename= 'pipeline.log',
            datefmt='%Y-%m-%d %H:%M:%S',
            format='%(asctime)s %(levelname)-8s %(message)s')

        self.logger = logging.getLogger('pipeline') 

        name_yml = os.path.abspath(config)

        with open(name_yml, 'r') as ymlfile:
            cfg = yaml.load(ymlfile,Loader=yaml.BaseLoader)

        client = pymongo.MongoClient('mongodb://{0}:{1}@{2}/{3}'.format(
            quote(cfg['auth'][cfg['env']]['mongo-user']),
            quote(cfg['auth'][cfg['env']]['mongo-pass']),
            cfg['auth'][cfg['env']]['mongo-host'],
            cfg['auth'][cfg['env']]['mongo-db'])
        )

        self.graph = py2neo.Graph(
            password = cfg['auth'][cfg['env']]['neo4j-pass'], 
            host = cfg['auth'][cfg['env']]['neo4j-host']
        )

        self.db = client[cfg['auth'][cfg['env']]['mongo-db']]

        self.dimension_all_flags = 'dimension_all_flags'
        self.organisations = 'organisations'
        self.ddf = 'ddf'

        self.data = []

    def process(self):

        query = {}
        query["dim_grid"] = False
        query["dim_ddf"] = False

        data = self.db[self.dimension_all_flags].find(query,{'_id': 0})

        for row in data:

            # Obtenemos el grid desde la respuesta descargada de la API DDF
            only_ddf = self.db[self.ddf].find_one({'meta.doi' : row['doi']})
            # Comprobamos que no haya cambiado el grid
            grid = self.db[self.organisations].find_one({'id_original' : only_ddf['meta']['grid']})['id']

            # Ya fue solucionado en el python getddf.py
            if 'url ' in only_ddf['meta']:

                only_ddf['meta']['url'] = only_ddf['meta']['url ']
                del only_ddf['meta']['url ']

            self.data.append({
                'defaultid' : 'ddf-{doi}'.format(doi=only_ddf['meta']['doi']),
                'doi' : only_ddf['meta']['doi'],
                'grid' : grid ,
                'url' : only_ddf['meta']['url'],
                'year' : only_ddf['meta']['year']
            })

        pandas.DataFrame(self.data).to_csv(f'/var/lib/neo4j/import/only-ddf.csv',encoding='utf-8',index=False,sep='|')
                
        cyphers = """
            USING PERIODIC COMMIT
            LOAD CSV WITH HEADERS FROM "file:///only-ddf.csv" AS row FIELDTERMINATOR '|'
            WITH row 
            MERGE (d:Document { defaultid: row.defaultid }) 
            SET 
                d.doi = row.doi,
                d.grid = row.grid,
                d.url = row.url,
                d.year = row.year
        """

        nodes_created = self.graph.run(cyphers).stats().nodes_created

        self.logger.info(f'Only DDF { nodes_created}')

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-cfg','--config',default='./config.yml')
    args = parser.parse_args()

    config = args.config

    InsertOnlyDoiDDF(config).process()



