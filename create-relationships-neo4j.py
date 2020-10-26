import os
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

class CreateRelationshipsNeo4j():

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
            self.cfg['auth'][self.cfg['env']]['mongo-db']), connect=False
        )

        self.db = self.client[self.cfg['auth'][self.cfg['env']]['mongo-db']]

        self.graph = py2neo.Graph(
            password = self.cfg['auth'][self.cfg['env']]['neo4j-pass'], 
            host = self.cfg['auth'][self.cfg['env']]['neo4j-host']
        )

        self.parsed = 'parsed'

        self.db[self.parsed].create_index([
            ('relationships_created',  pymongo.ASCENDING)], 
            name='relationships_created', default_language='english',unique=False)

        # self.data = {}
        self.data = defaultdict(list)

    def process(self):

        query = {}
        query["$or"] = [
            {
                u"relationships_created": False
            },
            {
                u"relationships_created": {
                    u"$exists": False
                }
            }
        ]

        docs = self.db.parsed.find(query,{'_id' : 0,'relations' : 1})
        total_docs = self.db.parsed.count_documents(query)           

        if not total_docs:
            self.logger.info(f'NOTHING TO PROCESS')
        else:
            # Por cada Documento
            for index,doc in enumerate(docs,1):

                self.logger.info(f'GROUPING BY ...{index}/{total_docs}')

                if not doc['relations']:
                    self.logger.info(f'SKIPPED GROUPING BY ...{index}/{total_docs}')
                    continue 

                relations = pandas.DataFrame(doc['relations']).groupby(['name'])

                for relname,relation in relations:
                    self.data[relname].extend(relation.to_dict('records'))


            for relation_name,row in self.data.items():
                origin,destiny = relation_name.split("_")

                filename = f'relations-{origin}-{destiny}.csv'
                self.logger.info(f'CREATING {filename}\n')

                pandas.DataFrame(row).to_csv(f'/var/lib/neo4j/import/{filename}',encoding='utf-8',index=False,sep='|')

                cypher = f"""
                    LOAD CSV WITH HEADERS FROM "file:///relations-{origin}-{destiny}.csv" AS row FIELDTERMINATOR "|"
                    MATCH (a:{origin} {{ defaultid : row.origin_defaultid }})
                    MATCH (b:{destiny} {{ defaultid : row.destiny_defaultid }})
                    RETURN ID(a) AS ida, ID(b) AS idb,row.attribute_name AS attribute_name,row.attribute_value AS attribute_value
                """

                relations_id = self.graph.run(cypher).to_data_frame()

                filename = f'relations-{origin}-{destiny}-id.csv'
                self.logger.info(f'CREAING {filename}\n')

                pandas.DataFrame(relations_id.to_dict('records')).to_csv(f'/var/lib/neo4j/import/{filename}',encoding='utf-8',index=False,sep='|')

                cypher = f"""
                    USING PERIODIC COMMIT
                    LOAD CSV WITH HEADERS FROM 'file:////{filename}' AS row FIELDTERMINATOR "|"
                    WITH row
                    WHERE row.attribute_name IS NOT NULL
                    MATCH (a) WHERE ID(a) = toInt(row.ida)
                    MATCH (b) WHERE ID(b) = toInt(row.idb)
                    MERGE (a)-[:{relation_name} {{ name : row.attribute_name, value: row.attribute_value }}]->(b)
                """

                self.logger.info('CREATING MERGE WITH PROPERTIES\n')
                relationships_created = self.graph.run(cypher).stats().relationships_created

                self.logger.info(f'{relation_name} : {relationships_created} WITH PROPERTIES\n')

                cypher = f"""
                    USING PERIODIC COMMIT
                    LOAD CSV WITH HEADERS FROM 'file:////{filename}' AS row FIELDTERMINATOR "|"
                    WITH row
                    WHERE row.attribute_name IS NULL
                    MATCH (a) WHERE ID(a) = toInt(row.ida)
                    MATCH (b) WHERE ID(b) = toInt(row.idb)
                    MERGE (a)-[:{relation_name}]->(b)
                """

                self.logger.info('CREATING MERGE WITHOUT PROPERTIES\n')
                relationships_created = self.graph.run(cypher).stats().relationships_created

                self.logger.info(f'{relation_name} : {relationships_created} WITHOUT PROPERTIES')
                
                self.logger.info('#------------------------------------------------------------------------------------#')
                
            self.db[self.parsed].update_many({},{"$set": { "relationships_created" : True }}) 

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-cfg','--config',default='./config.yml')
    args = parser.parse_args()
    config = args.config

    CreateRelationshipsNeo4j(config).process()

