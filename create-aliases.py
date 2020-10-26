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
import lxml.html

from sys import exit 
from pprint import pprint
from urllib.parse import quote

class CreateAlias():

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

        self.bfi_and_oai = 'BFI_andOAI'
        self.alias_org_name_hospital = 'alias_org_name_hospital'
        self.alias_org_type = 'alias_org_type'
        self.alias_topic_type = 'alias_topic_type'
        self.alias_uni_org_type = 'alias_uni_org_type'

        self.graph.run(f'CREATE INDEX ON:Document(doi);')
        self.graph.run(f'CREATE INDEX ON:Organisation(id_original);')
        self.graph.run(f'CREATE INDEX ON:Organisation(types);')
        self.graph.run(f'CREATE INDEX ON:Topic(title);')
        self.graph.run(f'CREATE INDEX ON:Organisation(name);')

    def process(self):

        bfi_and_oai = self.db[self.bfi_and_oai].find({},{'_id' : 0})
        alias_org_name_hospital = self.db[self.alias_org_name_hospital].find({},{'_id' : 0})
        alias_org_type = self.db[self.alias_org_type].find({},{'_id' : 0})
        alias_topic_type = self.db[self.alias_topic_type].find({},{'_id' : 0})
        alias_uni_org_type = self.db[self.alias_uni_org_type].find({},{'_id' : 0})

        pandas.DataFrame(bfi_and_oai).to_csv(f'/var/lib/neo4j/import/bfi_and_oai.csv',encoding='utf-8',index=False,sep='|')
        pandas.DataFrame(alias_org_name_hospital).to_csv(f'/var/lib/neo4j/import/alias_org_name_hospital.csv',encoding='utf-8',index=False,sep='|')
        pandas.DataFrame(alias_org_type).to_csv(f'/var/lib/neo4j/import/alias_org_type.csv',encoding='utf-8',index=False,sep='|')
        pandas.DataFrame(alias_topic_type).to_csv(f'/var/lib/neo4j/import/alias_topic_type.csv',encoding='utf-8',index=False,sep='|')
        pandas.DataFrame(alias_uni_org_type).to_csv(f'/var/lib/neo4j/import/alias_uni_org_type.csv',encoding='utf-8',index=False,sep='|')

        cypher = """
            LOAD CSV WITH HEADERS FROM 'file:///bfi_and_oai.csv' AS row FIELDTERMINATOR '|'
            MATCH( d:Document{ doi:row.DOI })
            SET 
                d.bfiandoai_type = row.`Type`,
                d.bfiandoai_oai_pub_year = row.`OAI-pub-year`,
                d.bfiandoai_oai_mra = row.`OAI-MRA`,
                d.bfiandoai_oai_sub_year = row.`OAI-sub-year`,
                d.bfiandoai_bfi_level = row.`BFI-Level`,
                d.bfiandoai_oai_class = row.`OAI-Class`,
                d.bfiandoai_bfi_mra = row.`BFI-MRA`,
                d.bfiandoai_bfi_pub_year = row.`BFI-pub-year`, 
                d.bfiandoai_bfi_sub_year = row.`BFI-sub-year`
        """

        properties_set = self.graph.run(cypher).stats().properties_set

        self.logger.info(f' BFI_andOAI {properties_set}')

        cypher = """
            LOAD CSV WITH HEADERS FROM 'file:///alias_org_name_hospital.csv' AS row FIELDTERMINATOR '|'
            MATCH (o:Organisation { id_original : row.GRID}) 
            SET o.alias_org_name_hospital = row.ALIAS 
        """

        properties_set = self.graph.run(cypher).stats().properties_set

        self.logger.info(f' alias_org_name_hospital  {properties_set}')

        cypher = """
            LOAD CSV WITH HEADERS FROM 'file:///alias_org_type.csv' AS row FIELDTERMINATOR '|'
            MATCH (o:Organisation { types : row.PREVIOUS})
            SET o.alias_org_type = row.NEW
        """

        properties_set = self.graph.run(cypher).stats().properties_set

        self.logger.info(f' alias_org_type  {properties_set}')


        cypher = """
            LOAD CSV WITH HEADERS FROM "file:///alias_topic_type.csv" AS row FIELDTERMINATOR '|'
            MATCH (t:Topic { title : row.PREVIOUS})
            SET t.alias_topic_type = row.NEW
        """

        properties_set = self.graph.run(cypher).stats().properties_set

        self.logger.info(f' alias_topic_type  {properties_set}')

        cypher = """
            LOAD CSV WITH HEADERS FROM "file:///alias_uni_org_type.csv" AS row FIELDTERMINATOR '|'
            MATCH (o:Organisation { name : row.PREVIOUS})
            SET o.alias_uni_org_type = row.NEW
        """

        properties_set = self.graph.run(cypher).stats().properties_set

        self.logger.info(f' alias_uni_org_type  {properties_set}')

if __name__ == '__main__':


    parser = argparse.ArgumentParser()
    parser.add_argument('-cfg','--config',default='./config.yml')
    args = parser.parse_args()

    config = args.config

    CreateAlias(config).process()



