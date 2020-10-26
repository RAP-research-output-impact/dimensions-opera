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

class RelationDoiGridKaren():

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

        self.graph.run('CREATE INDEX ON:Document(doi);')
        self.graph.run('CREATE INDEX ON:Organisation(id);')
        
        self.data = []

    """ Relacion entre documento y Organización basado en el DOI y el Grid """
    def process(self):

        rows = self.db[self.extra_documents].find({})

        for row in rows:
            self.data.append({
                'grid' : row['grid'],
                'defaultid' : 'dimensions-{id}'.format(id=row['dimension_id'])
            })

        pandas.DataFrame(self.data).to_csv(f'/var/lib/neo4j/import/relation-doi-grid-karen.csv',encoding='utf-8',index=False,sep='|')
                
        self.logger.info('CSV relation-doi-grid-karen.csv')
        
        cypher = """
            CALL apoc.export.csv.query("
                LOAD CSV WITH HEADERS FROM 'file:///relation-doi-grid-karen.csv' AS row FIELDTERMINATOR '|'
                WITH row 
                MATCH (d:Document { defaultid : row.defaultid})
                MATCH (o:Organisation { id : row.grid})
                RETURN ID(d) AS ida,ID(o) AS idb
            ","relation-doi-grid-id-karen.csv",{});
        """

        self.graph.run(cypher)

        self.logger.info('CSV relation-doi-grid-id-karen.csv')
        
        cypher = """
            USING PERIODIC COMMIT
            LOAD CSV WITH HEADERS FROM 'file:////relation-doi-grid-id-karen.csv' AS row
            MATCH (a) WHERE ID(a) = toInt(row.ida)
            MATCH (b) WHERE ID(b) = toInt(row.idb)
            MERGE (a)-[:KAREN_ORGANISATION]->(b)
        """

        relationships_created = self.graph.run(cypher).stats().relationships_created

        self.logger.info(f'Relation DEFAULTID GRID  { relationships_created} : KAREN ')

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-cfg','--config',default='./config.yml')
    args = parser.parse_args()

    config = args.config

    RelationDoiGridKaren(config).process()



