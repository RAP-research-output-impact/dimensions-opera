import os
import csv
import yaml
import time
import py2neo
import pandas
import pymongo
import requests
import argparse
import datetime
import logging

from collections import defaultdict
from urllib.parse import quote
from pprint import pprint 

class CreateNodesNeo4j():

    def __init__(self,config):

        logging.basicConfig(
            level=logging.INFO,
            filename= 'pipeline.log',
            datefmt='%Y-%m-%d %H:%M:%S',
            format='%(asctime)s %(levelname)-8s %(message)s')

        self.logger = logging.getLogger('pipeline')
        
        name_yml = os.path.abspath(config)

        with open(name_yml, 'r') as ymlfile:
            self.cfg = yaml.load(ymlfile,Loader=yaml.BaseLoader)

        self.client = pymongo.MongoClient('mongodb://{0}:{1}@{2}/{3}'.format(
            quote(self.cfg['auth'][self.cfg['env']]['mongo-user']),
            quote(self.cfg['auth'][self.cfg['env']]['mongo-pass']),
            self.cfg['auth'][self.cfg['env']]['mongo-host'],
            self.cfg['auth'][self.cfg['env']]['mongo-db'])
        )

        self.db = self.client[self.cfg['auth'][self.cfg['env']]['mongo-db']]

        self.graph = py2neo.Graph(
            password = self.cfg['auth'][self.cfg['env']]['neo4j-pass'], 
            host = self.cfg['auth'][self.cfg['env']]['neo4j-host']
        )

        self.parsed = 'parsed'

        self.data = defaultdict(list)

        self.total_docs = self.db[self.parsed].count_documents({})

        self.db[self.parsed].create_index([('nodes_created',  pymongo.ASCENDING)], name='nodes_created', default_language='english',unique=False)

    def save_csv(self,row,label):
        pandas.DataFrame(row).to_csv(f'/var/lib/neo4j/import/{label}.csv',encoding='utf-8',index=False,sep='|',quoting=csv.QUOTE_ALL)

    def parser(self):

        query = {}
        query["$or"] = [
            {
                u"nodes_created": False
            },
            {
                u"nodes_created": {
                    u"$exists": False
                }
            }
        ]

        docs = self.db.parsed.find(query,{'_id' : 0})

        for index,doc in enumerate(docs,1):

            for label,rows in doc['nodes'].items():
                
                # El Label no tiene datos 
                if rows is None or not rows:
                    continue

                self.data[label].extend(rows)

            self.logger.info('{index}/{total_docs}'.format(
                index=index,
                total_docs=self.total_docs
            ))

        
    def process(self):

        self.parser()

        if not self.data:
            self.logger.info(f'NOTHING TO PROCESS')
        else:
            for label,row in self.data.items():

                # Generamos el Indice base
                self.graph.run(f'CREATE CONSTRAINT ON (d:{label}) ASSERT d.defaultid IS UNIQUE;')
                
                self.logger.info(f'{label}')

                # Eliminamos repetidos
                row = pandas.DataFrame(row).drop_duplicates().to_dict('records')
                
                self.logger.info(f'\tGROUPED {label}')
                # Generamos el CSV
                self.save_csv(row,label)
                self.logger.info(f'\tCSV SAVED {label}')

                # Generamos la listra de atributos para Neo4j
                create_set = []
                attribs = list(set().union(*(d.keys() for d in row))) 

                for kda in attribs:
                    create_set.append("n.`{kda}` = row.`{kda}`".format(kda=kda))
                    
                create_set = ',\n'.join(create_set)

                cypher = """
                    USING PERIODIC COMMIT
                    LOAD CSV WITH HEADERS FROM "file:///{filename}" AS row FIELDTERMINATOR '|'
                    WITH row 
                    MERGE (n:{label} {{ defaultid: row.defaultid }}) 
                    ON CREATE SET 
                        {create_set};
                """.format(
                    create_set = create_set,
                    filename = f'{label}.csv',
                    label = label)

                self.logger.info(f'\tEXECUTING {label}')

                nodes_created = self.graph.run(cypher).stats().nodes_created

                self.logger.info(f'{label} {nodes_created}')

            self.db[self.parsed].update_many({},{"$set": { "nodes_created" : True }}) 

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-cfg','--config',default='./config.yml')
    args = parser.parse_args()
    config = args.config

    CreateNodesNeo4j(config).process()

