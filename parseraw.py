import re
import os
import yaml
import logging
import pymongo
import pandas
import argparse
import requests
import functools

from dateutil.parser import parse as parser_date
from urllib.parse import quote
from pprint import pprint 
from sys import exit 

class ParseRaw():

    def __init__(self,collection,config):

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

        self.raw = collection 

        self.organisations = 'organisations'

        self.db.parsed.create_index([('defaultid',  pymongo.ASCENDING)], name='defaultid', default_language='english',unique=True)

    # Helpers #=======================================================================================

    def _save_relations(self,origin_defaultid,rel_name,destiny_defaultid,field = None,attrib = None):

        rel_attrb_value = None 
        
        if attrib is not None:
            rel_attrb_value = field[attrib]

        self.relations.append({
            'origin_defaultid' : origin_defaultid,
            'destiny_defaultid' : destiny_defaultid,
            'name': rel_name,
            'attribute_value': rel_attrb_value,
            'attribute_name' : attrib
        })

    def _defaultid_name(self,value):

        value = str(value).strip()
        value = value.lower()
        value = value.replace(" ","_")
        value = value.replace("-","_")
        value = value.replace("+","_")

        # Solo un _ en el caso que se repitan seguidamente.
        # Terrariet - Reptile Zoo -> terrariet___reptile_zoo -> terrariet_reptile_zoo

        return re.sub("_+", '_', value)

    def _is_str(self,value):

        if type(value) is str:
            return True

        return False

    def _is_list(self,value):

        if type(value) is list:
            return True
            
        return False

    def _is_dict(self,value):

        if type(value) is dict:
            return True
            
        return False

    def _is_int(self,value):

        if type(value) is int:
            return True
            
        return False

    def _escape_text(self,text):

        # https://neo4j.com/developer/kb/parsing-of-quotes-for-load-csv-and-or-import/
        if text is None:
            return 

        if '"' in text:
            ntext = []
            text = text.split()

            for value in text:

                value = value.replace("'","").strip()
                value = value.replace("\\","").strip()

                if value.startswith('"') and value.endswith('"'):
                    value = '"' + value + '"'
                elif '"' in value:
                    value = value.replace('"','')

                ntext.append(value)

            text = ' '.join(ntext)
            text = '"' + text + '"'

        return text.encode('ascii', 'ignore').decode("utf-8").strip()


    # =================================================================================================

    # Concepts
    def get_concepts(self,row):

        concepts = []
        if 'concepts_scores' in row['meta']['raw']:
            items = row['meta']['raw']['concepts_scores']
            if items is not None:
                source = 'dimension'

                for item in items:
                    defaultid = '{source}@{id}'.format(source=source,id=self._defaultid_name(item['concept']))

                    concepts.append({
                        'defaultid' : defaultid,
                        'title' : item['concept'],
                        'source' : source
                    })

                    self._save_relations(row['meta']['defaultid'],'Document_Concept',defaultid,item,'relevance')


        return concepts

    # Topics
    def get_topics(self,row):

        topics = []

        if 'category_for' in row['meta']['raw']:
            items = row['meta']['raw']['category_for']

            if items is not None:
                source = 'dimension'

                for item in items:
                    defaultid = '{source}@{id}'.format(source=source,id=self._defaultid_name(item['name']))

                    topics.append({
                        'defaultid' : defaultid,
                        'title' : item['name'],
                        'source' : source
                    })

                    self._save_relations(row['meta']['defaultid'],'Document_Topic',defaultid)

        return topics

    # Terms
    def get_terms(self,row):

        terms = []

        if 'terms' in row['meta']['raw']:
            items = row['meta']['raw']['terms']
            if items is not None:
                source = 'dimension'

                for item in items:
                    defaultid = '{source}@{id}'.format(source=source,id=self._defaultid_name(item))

                    terms.append({
                        'defaultid' : defaultid,
                        'title' : item,
                        'source' : source
                    })

                    self._save_relations(row['meta']['defaultid'],'Document_Term',defaultid)

        return terms

    # SDG
    def get_sdg(self,row):

        sdg = []
        if 'category_sdg' in row['meta']['raw']:
            items = row['meta']['raw']['category_sdg']

            if items is not None:
                source = 'dimension'

                for item in items:
                    defaultid = '{source}@{id}'.format(source=source,id=self._defaultid_name(item['name']))

                    sdg.append({
                        'defaultid' : defaultid,
                        'title' : item['name'],
                        'source' : source
                    })

                    self._save_relations(row['meta']['defaultid'],'Document_Sdg',defaultid)

        return sdg



    def get_documents(self,row):

        # Obtenemos las organizaciones 
        document_orgs = []

        pivots = {
            'publications' : 'research_orgs',
            'patents' : 'assignees', 
            'grants' : 'research_orgs',
            'clinical_trials' : 'research_orgs'
        }

        pivot = pivots[row['meta']['raw']['dbname']]

        if pivot in row['meta']['raw']:

            document_orgs = row['meta']['raw'][pivot]
            document_orgs = [research_org['id'] for research_org in document_orgs]

        source = 'dimension'

        # Relaciones entre Documento y organizacion
        # Organizaciones es el unico Label que el defaultid es simplemente el ID del grid,no es 'dimension_<algo>'
        for grid in document_orgs: 
            self._save_relations(row['meta']['defaultid'],'Document_Organisation',f"{grid}")

        sheet = {}

        # Aplanamos de una manera diferente
        # Solamente aceptamos, str, int o listas que dentro tengan str o int.
        for key,value in row['meta']['raw'].items():

            # En el caso que el valor sea una lista
            if self._is_list(value):

                # En el caso que la lista este vacia
                if not value:
                    continue

                # Si el primer valor de la lista es un str o un int
                if self._is_str(value[0]) or self._is_int(value[0]):
                    sheet[key] = value
                else:
                    # Si el primer valor de la lista contiene una lista continuamos
                    if self._is_list(value[0]):
                        continue 
                    elif self._is_dict(value[0]):
                        # Si el primer valor de la lista contiene un dict continuamos
                        continue 
                    else:
                        # En el caso que el primer valor de la lista no sea ni una lista o un dict
                        sheet[key] = value
            else:   
                # En el caso que el valor NO sea una lista, verificamos que NO sea un dict.
                if self._is_dict(value):
                    continue 

                # Como el valor NO es una lista o un dict o agregamos.
                sheet[key] = value
            
        document = {}
        # Concatenamos valores de las listas
        for key,value in sheet.items():
            if self._is_list(value):
                document[key] = self._escape_text('@@@'.join(map(str, value)))
            else:
                if self._is_str(value):
                    document[key] = self._escape_text(value) 
                else:
                    document[key] = value 

        # Valores por default
        document['defaultid'] = row['meta']['defaultid']
        document['title'] = row['meta']['raw']['title']
        document['source'] = 'dimension'

        # Fechas 
        pivot_dates = {
            'publications' : 'date',
            'grants' : 'start_date',
            'patents' : 'publication_date',
            'clinical_trials' : 'date'
        }

        if pivot_dates[row['meta']['raw']['dbname']] in row['meta']['raw']:

            date = parser_date(row['meta']['raw'][pivot_dates[row['meta']['raw']['dbname']]]).date()

            document['fulldate'] = str(date)
            document['year'] = date.year 

        return ([document], document_orgs)
            
    def get_organisations(self,document_orgs):

        # Unimos y eliminamos repetidos
        query = {}
        query["id_original"] = {
            u"$in": list(set(document_orgs))
        }

        # Obtenemos todas las organizaciones
        organisations = list(self.db[self.organisations].find(query,{'_id' : 0})) 

        # Asociamos todos los lugares a la variable addresses
        addresses = [organisation['addresses'] for organisation in organisations]

        for organisation in organisations:

            self._save_relations(organisation['id'],'Organisation_City',self._defaultid_city(organisation['addresses']))
            self._save_relations(organisation['id'],'Organisation_Country',self._defaultid_name(organisation['addresses']['country']))

        # Eliminamos addresses desde organisations para que no existan dict dentro del registro.
        [organisation.pop('addresses') for organisation in organisations]

        return (organisations,addresses)

    def _defaultid_city(self,item):

        defaultid = '{country}_{city}'.format(country=item['country'],city=item['city'])
        return self._defaultid_name(defaultid)

    def get_city_country(self,addresses):

        cities = []
        countries = []

        for item in addresses:


            cities.append({
                'defaultid' : self._defaultid_city(item),
                'title' : item['city'],
                'country' : self._defaultid_name(item['country'])
            })

            countries.append({
                'defaultid' : self._defaultid_name(item['country']),
                'title' : item['country']
            })

        cities = pandas.DataFrame(cities).drop_duplicates().to_dict('records')
        countries = pandas.DataFrame(countries).drop_duplicates().to_dict('records')

        return (cities,countries)

    def process(self):

        query = {}
        query["meta.raw.dbname"] = {
            u"$ne": u"datasets"
        }

        rows = self.db[self.raw].find(query)
        total = self.db[self.raw].count_documents(query)

        for index,row in enumerate(rows,1):

            # Verificamos si ya existe en la base de datos
            if self.db.parsed.count_documents({'defaultid' : row['meta']['defaultid']}) > 0:
                self.logger.info(f'ALREADY INSERTED {index}/{total}')
                continue 

            self.relations = []

            concepts = self.get_concepts(row)
            topics = self.get_topics(row)
            terms = self.get_terms(row)
            sdg = self.get_sdg(row)

            # Desde documentos obtengo las organizaciones asociadas al documento.
            document,document_orgs = self.get_documents(row)

            # Desde organizaciones obtenemos Paises y ciudadades
            organisations,addresses = self.get_organisations(document_orgs)

            # Obtenemos ciudades y paises
            cities,countries = self.get_city_country(addresses)

            data = {
                'nodes' : {
                    'Concept' : concepts,
                    'Topic': topics,
                    'Terms' : terms,
                    'Sdg': sdg,
                    'Document' : document,
                    'Organisation' : organisations,
                    'City' : cities,
                    'Country': countries
                },
                'relations' : self.relations,
                'source' : 'dimension',
                'defaultid' : row['meta']['defaultid']
            }

            self.db.parsed.insert_one(data)

            self.logger.info(f'{index}/{total} INSERTED')

        self.client.close()

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-c','--collection',default='raw')
    parser.add_argument('-cfg','--config',default='./config.yml')
    args = parser.parse_args()

    config = args.config
    collection = args.collection

    ParseRaw(collection,config).process()
