import os
import yaml
import pandas
import py2neo
import pymongo
import logging
import argparse

from collections import defaultdict
from urllib.parse import quote
from pprint import pprint
from sys import exit 

class AppendDataNeo4j():

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

        self.graph = py2neo.Graph(
            password = self.cfg['auth'][self.cfg['env']]['neo4j-pass'], 
            host = self.cfg['auth'][self.cfg['env']]['neo4j-host']
        )

        self.db = self.client[self.cfg['auth'][self.cfg['env']]['mongo-db']]

        self.csv_data = []

        self.dimension_all = 'dimension_all'

    def process(self):

        # Lista de campos permitidos:
        # https://docs.google.com/spreadsheets/d/1Jc4IZ55lm02Rdk8p76qtJa0N-LjuP_f2WE4XTjqHLdA/edit?ts=5f1ff5ea#gid=0
        # https://docs.dimensions.ai/dsl/datasource-publications.html

        allowed_fields = {
            'publications' : [
                'altmetric',
                'altmetric_id',
                'book_doi',
                'book_series_title',
                'book_title',
                'date',
                'date_inserted',
                'doi',
                'field_citation_ratio',
                'id',
                'issue',
                'journal',
                'journal_lists',
                'linkout',
                'open_access_categories',
                'pages',
                'pmcid',
                'pmid',
                'proceedings_title',
                'publisher',
                'recent_citations',
                'relative_citation_ratio',
                'resulting_publication_doi',
                'times_cited',
                'title',
                'type',
                'volume',
                'year',
                'recent_citations_total',
                'rcr_avg',
                'fcr_gavg',
                'citations_total',
                'citations_median',
                'citations_avg',
                'altmetric_median',
                'altmetric_avg'
            ],
            'clinical_trials' : [
                'abstract',
                'acronym',
                'active_years',
                'brief_title',
                'conditions',
                'date',
                'date_inserted',
                'gender',
                'id',
                'linkout',
                'phase',
                'registry',
                'title'
            ],
            'grants' : [
                'abstract'
                'active_year'
                'date_inserted'
                'end_date'
                'foa_number'
                'funding_aud'
                'funding_cad'
                'funding_chf'
                'funding_currency'
                'funding_eur'
                'funding_gbp'
                'funding_jpy'
                'funding_nzd'
                'funding_org_acronym'
                'funding_org_city'
                'funding_org_name'
                'funding_usd'
                'grant_number'
                'id'
                'language'
                'language_title'
                'linkout'
                'original_title'
                'start_date'
                'start_year'
                'title'
                'funding'
                'count'
            ],
            'patents' : [
                'abstract',
                'cpc',
                'date',
                'date_inserted',
                'expiration_date',
                'filing_status',
                'granted_date',
                'granted_year',
                'id',
                'inventor_names',
                'ipcr',
                'jurisdiction',
                'legal_status',
                'priority_date',
                'priority_year',
                'publication_date',
                'publication_year',
                'times_cited',
                'title',
                'year'
            ]
        }       

        for dbname,allowed_field in allowed_fields.items():

            projections = {}
            for projection in allowed_field:
                projections['meta.raw.' + projection] = 1

            projections['meta.defaultid'] = 1 
            query = { 'meta.raw.dbname' : dbname }

            projections = {'meta.raw.journal' : 1 ,'meta.defaultid' : 1}

            rows = self.db[self.dimension_all].find(query,projections)
            total = self.db[self.dimension_all].count_documents(query)

            for index,row in enumerate(rows,1):

                if index % 1000 == 0:
                    self.logger.info(f'{dbname}\t{index}/{total}')

                data = {}
                data['defaultid'] = row['meta']['defaultid']
                data['dbname'] = dbname

                row = row['meta']['raw']              

                for key,value in row.items():

                    if isinstance(value, str) or isinstance(value, int) or isinstance(value, float):
                        data[key] = value 
                    elif isinstance(value, list):
                        # Tomamos el primer registro como referencia
                        if isinstance(value[0],dict):

                            dictvalues = defaultdict(list)

                            for dvalues in value: 
                                for dkey, dval in dvalues.items():
                                    dictvalues[dkey].append(dval)

                            for keydictvalue,dictvalue in dictvalues.items():
                                data[f'{key}_{keydictvalue}'] = '@@@'.join(dictvalue)

                        else:
                            data[key] = '@@@'.join(str(v) for v in value)
                    elif isinstance(value,dict):
                        for dkey,dvalue in value.items():
                            data[f'{key}_{dkey}'] = dvalue

                self.csv_data.append(data)
                
        pandas.DataFrame(self.csv_data).to_csv('/var/lib/neo4j/import/append-data-neo4j.csv',encoding='utf-8',index=False,sep="|")

        # Cypher
        attribs = list(set().union(*(d.keys() for d in self.csv_data)))

        for vremove in ['defaultid','dbname']:
            attribs.remove(vremove)

        create_set = []
        for kda in attribs:
            create_set.append("n.`raw_{kda}` = row.`{kda}`".format(kda=kda))

        create_set = ',\n'.join(create_set)

        cypher = f'USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM "file:///append-data-neo4j.csv" AS row FIELDTERMINATOR "|" \n \
            WITH row \
            MATCH (n:Document {{ defaultid: row.defaultid }}) \n \
            SET {create_set};'

        self.graph.run(cypher)

        properties_set = self.graph.run(cypher).stats().properties_set

        self.logger.info(f'Append Data { properties_set}')


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-cfg','--config',default='./config.yml')
    args = parser.parse_args()

    config = args.config

    AppendDataNeo4j(config).process()
  