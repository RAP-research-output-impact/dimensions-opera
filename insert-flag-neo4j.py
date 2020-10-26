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

class InsertFlagNeo4j():

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

    def process(self):

        data = self.db[self.dimension_all_flags].find({},{'_id': 0})
        pandas.DataFrame(data).to_csv(f'/var/lib/neo4j/import/doi-ddf-flags.csv',encoding='utf-8',index=False,sep='|')
                
        # Generamos el Indice base
        self.graph.run(f'CREATE INDEX ON:Document(doi);')
        self.graph.run(f'CREATE INDEX ON:Document(dim_grid);')
        self.graph.run(f'CREATE INDEX ON:Document(dim_ddf);')
                
        cypher = """
            USING PERIODIC COMMIT
            LOAD CSV WITH HEADERS FROM "file:///doi-ddf-flags.csv" AS row FIELDTERMINATOR '|'
            WITH row 
            MATCH (d:Document { doi: row.doi }) SET d.dim_grid = row.dim_grid, d.dim_ddf = row.dim_ddf
        """

        properties_set = self.graph.run(cypher).stats().properties_set
        self.logger.info(f'TOTAL FLAGS SET {properties_set}')

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-cfg','--config',default='./config.yml')
    args = parser.parse_args()

    config = args.config

    InsertFlagNeo4j(config).process()