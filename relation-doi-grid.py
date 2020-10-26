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

class RelationDoiGrid():

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

        self.dimension_all = 'dimension_all'
        self.organisations = 'organisations'
        self.ddf = 'ddf'

        self.db[self.dimension_all].create_index([('meta.raw.dbname',  pymongo.ASCENDING)], name='meta.raw.dbname', default_language='english',unique=False)
        
        self.graph.run('CREATE INDEX ON:Document(doi);')
        self.graph.run('CREATE INDEX ON:Organisation(id);')
        
        self.data = []

    """ Relacion entre documento y Organización basado en el DOI y el Grid """
    
    def process(self):

        rows = self.db[self.ddf].find({})

        for ddf in rows:
            self.data.append({
                'grid' : ddf['meta']['grid'],
                'doi' : ddf['meta']['doi']
            })

        query = {}
        query["meta.raw.dbname"] = u"publications"

        rows = self.db[self.dimension_all].find(query)
        total = self.db[self.dimension_all].count_documents({})

        for index,dim in enumerate(rows,1):

            if 'research_orgs' not in dim['meta']['raw']:
                continue

            for research_org in  dim['meta']['raw']['research_orgs']:

                if 'doi' not in dim['meta']['raw']:
                    continue 

                self.data.append({
                    'grid' : research_org['id'],
                    'doi' :  dim['meta']['raw']['doi']
                })

            self.logger.info(f'{index}/{total}')

        pandas.DataFrame(self.data).to_csv(f'/var/lib/neo4j/import/relation-doi-grid.csv',encoding='utf-8',index=False,sep='|')
                
        self.logger.info('CSV relation-doi-grid.csv')
        
        cypher = """
            CALL apoc.export.csv.query("
                LOAD CSV WITH HEADERS FROM 'file:///relation-doi-grid.csv' AS row FIELDTERMINATOR '|'
                WITH row 
                MATCH (d:Document { doi : row.doi})
                MATCH (o:Organisation { id : row.grid})
                RETURN ID(d) AS ida,ID(o) AS idb
            ","relation-doi-grid-id.csv",{});
        """

        self.graph.run(cypher)

        self.logger.info('CSV relation-doi-grid-id.csv')
        
        cypher = """
            USING PERIODIC COMMIT
            LOAD CSV WITH HEADERS FROM 'file:////relation-doi-grid-id.csv' AS row
            MATCH (a) WHERE ID(a) = toInt(row.ida)
            MATCH (b) WHERE ID(b) = toInt(row.idb)
            MERGE (a)-[:DDF_ORGANISATION]->(b)
        """

        relationships_created = self.graph.run(cypher).stats().relationships_created

        self.logger.info(f'Relation DOI GRID { relationships_created}')

if __name__ == '__main__':


    parser = argparse.ArgumentParser()
    parser.add_argument('-cfg','--config',default='./config.yml')
    args = parser.parse_args()

    config = args.config

    RelationDoiGrid(config).process()



