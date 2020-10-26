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

class InsertFlagNeo4jKaren():

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

        self.extra_documents = 'extra_documents'

        self.id = []
        self.doi = []

    def process(self):

        rows = self.db[self.extra_documents].find({},{'_id': 0})

        for row in rows:
            if row['dimension_id'] is not None:
                self.id.append({
                    'defaultid' : 'dimensions_{id}'.format(id=row['dimension_id'])
                })
            else:
                self.doi.append({
                    'doi' : row['doi']
                })

        pandas.DataFrame(self.id).to_csv(f'/var/lib/neo4j/import/karen-flag-defaultid.csv',encoding='utf-8',index=False,sep='|')
        pandas.DataFrame(self.doi).to_csv(f'/var/lib/neo4j/import/karen-flag-doi.csv',encoding='utf-8',index=False,sep='|')
                     
        cypher = """
            USING PERIODIC COMMIT
            LOAD CSV WITH HEADERS FROM "file:///karen-flag-defaultid.csv" AS row FIELDTERMINATOR '|'
            WITH row 
            MATCH (d:Document { defaultid: row.defaultid }) SET d.extra_document = True
        """

        properties_set = self.graph.run(cypher).stats().properties_set

        self.logger.info(f'KAREN FLAG DEFAULTID properties_set: {properties_set}')

        cypher = """
            USING PERIODIC COMMIT
            LOAD CSV WITH HEADERS FROM "file:///karen-flag-doi.csv" AS row FIELDTERMINATOR '|'
            WITH row 
            MATCH (d:Document { doi: row.doi }) SET d.extra_document = True
        """

        properties_set = self.graph.run(cypher).stats().properties_set

        self.logger.info(f'KAREN FLAG DOI properties_set: {properties_set}')


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-cfg','--config',default='./config.yml')
    args = parser.parse_args()

    config = args.config
    
    InsertFlagNeo4jKaren(config).process()



