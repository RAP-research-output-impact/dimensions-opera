
## Collections that load on startup

* grid_json
* BFI_andOAI
* alias_org_name_hospital
* alias_org_type
* alias_topic_type
* alias_uni_org_type
* KarenExtraRecords

Note: there must be a collection called grid_json, which is the json file downloaded from https://www.grid.ac/downloads

## DB 
1. dimension_grid: Contains all the documents downloaded from dimension based on the GRIDs obtained in the grid.ac search engine, filtering by Education, Healthcare and country Denmark.
2. ddf: contains all the DOIs downloaded from the DDF API.
3. dimension_ddf: DOI DDF based documents found in dimension.
4. dimension_all: contains the combination of the dimension_grid and dimension_ddf collections, this collection does not contain the documents that were NOT found in dimension (DOI obtained from DDF API)
5. organizations: contains the information of the grid_json collection but in an already standardized format to be consumed directly by the opera parser.
6. parsed: contains all the information of dimension_all parsed to be inserted in Neo4j.
7. dimension_all_flags: contains all the doi of the dimension_all collection and the related flags dim_ddf and dim_grid
8. extra_documents: contains all the extra documents that must be entered into the system, it is the homogable of ddf.


## Main Process

#### We download all the information from Dimension (collection: dimension_grid)

```
time python3.6  getraw.py  -c dimension_grid -bd "2014-01-01" -ed "2019-12-31'" --dbname publications
real    320m51.697s

time python3.6  getraw.py  -c dimension_grid -bd "2014-01-01" -ed "2019-12-31'" --dbname patents
real    2m39.879s

time python3.6  getraw.py  -c dimension_grid -bd "2014-01-01" -ed "2019-12-31'" --dbname grants
real    3m50.482s

time python3.6  getraw.py  -c dimension_grid -bd "2014-01-01" -ed "2019-12-31'" --dbname clinical_trials
real    3m29.265s

time python3.6  getraw.py  -c dimension_grid -bd "2014-01-01" -ed "2019-12-31'" --dbname datasets
real    5m2.404s
```

#### We download all DOIs from the DDF API (collection: ddf)

```
time python3.6  getddf.py -cfg /home/ubuntu/dataverz/projects/opera/config.yml -y "2013,2014,2015,2016,2017,2018,2019"
real    3m39.410s
```

#### We download all the documents from dimension based on the DOIs downloaded from DDF (collection: dimension_ddf)

```
time python3.6  getddf-dimension.py
real    192m32.858s
```

#### We combine both collections containing documents downloaded from Dimension (dimension ddf + dimension_grid) (collection: dimension_all)

```
time python3.6 combine-ddf-grid-dimension.py 
real    5m50.531s
```

#### Organization collection creation (based on grid json)

```
time python3.6 create-collection-organisations.py 
real    0m33.713
```

#### We generate the parsed collection related to dimension_all collections and organizations (collection: parsed)

```
time python3.6 parseraw.py 
real    6m57.671s
```

#### We generate collection dimension_all_flags with all the DOIs of all the collections (dimension_grid, dimension_ddf, ddf)

```
time python3.6 create-collection-all-doi.py 
real    1m9.880s
```

#### We update the dimension_all_flags collection with the flags corresponding to dim_ddf and dim_grid

```
time python3.6 dimension-all-flags.py 
real    2m31.858s

```

## Load

#### We generate all the Nodes from the parsed collection

```
time python3.6 create-nodes-neo4j.py 
real    6m52.106s

time python3.6 create-nodes-neo4j.py

```

#### We generate all the relationships of the nodes

```
time python3.6 create-relationships-neo4j.py
real    72m50.981s

time python3.6 create-relationships-neo4j.py
```

## Afterload 

#### Add extra fields from collections delivered by Opera

	* BFI_andOAI
	* alias_org_name_hospital
	* alias_org_type 
	* alias_topic_type
	* alias_uni_org_type

```
time python3.6 create-aliases.py 
real	0m6.013s
```

#### Insert all DOIs that were not found in Dimension into Neo4j.

```
time python3.6 insert-only-doi-ddf.py 
real	0m3.019s
```

#### Associate all DDF DOIs with their corresponding GRIDs within Neo4j.
```
time python3.6 relation-doi-grid.py  
real	2m47.016s

```

#### Insert the DDF DOI flags into Neo4j.
```
time python3.6 insert-flag-neo4j.py   
real	0m13.599s

```

#### Insert all raw attributes into Neo4j.

```
time python3.6 append-data-neo4j.py 
real	1m0.569s
```

## New Documents Update Process (KarenExtraRecords) - Manual DOI process.


#### We sort the information in KarenExtraRecords in a standard collection eliminating the docs that your organization could not find based on the records provided (example: https://app.dimensions.ai/details/publication/pub.1065208274). (collection: extra_documents)


```
time python3.6 karen-extra-records-parse.py
real	2m8.802s
```

#### We download the information from Dimension in dimension_all

```
time python3.6 karen-extra-records-download-dimension.py  
real	2m12.416s
```


#### We parse the new records, they will be inserted into the parsed collection

```
time python3.6 parseraw.py 
real	3m57.495s
```

#### We create the new Nodes

```
time python3.6 create-nodes-neo4j.py  
real	0m4.853s
```

#### We create new relationships

```
time python3.6 create-relationships-neo4j.py 
real	0m5.382s
```

#### We add aliases to new documents
```
time python3.6 create-aliases.py  
real	0m5.404s
```

#### Insert all raw attributes into Neo4j.

```
time python3.6 append-data-neo4j.py 
real	1m7.722s
```

#### Generate the DOCUMENT_ORGANIZATION Relationship between karen's documents and the organizations.

```
time python3.6 relation-doi-grid-karen.py 
real	0m1.396s
```

#### Add the extra_documents field to all the documents that are in karen's collection.
```
time python3.6 insert-flag-neo4j-karen.py 
real	0m1.384s
```






