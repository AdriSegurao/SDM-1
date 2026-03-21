# Property Graphs

Property Graphs Lab derivable for SDM course in MDS.

# Project Execution Instructions

1. To convert the DBLP XML file into CSV files, first run:
```python ./XMLToCSV.py data/xml_dblp_data/dblp.xml data/xml_dblp_data/dblp.dtd data/csv_dblp_data/output.csv --annotate```.

2. To generate the CSV files used by our graph database, run:
```python FormatCSV.py --article data/csv_dblp_data/output_article.csv --article-header data/csv_dblp_data/output_article_header.csv --inproceedings data/csv_dblp_data/output_inproceedings.csv --inproceedings-header data/csv_dblp_data/output_inproceedings_header.csv --out data/csv_graphmodel_data --limit 10000```.
This will use data from the <i>articles</i> and <i>inproceedings</i> CSV files to generate the node and relationship CSVs of our model. The <i>limit</i> parameter sets the maximum number of lines read from each CSV file.

3. Finally, to create the database, first copy the node and relationship CSV files (by default located in ```data/csv_graphmodel_data```) into the instance's ```import``` folder. To open this folder, go to Local Instances in Neo4j Desktop, open the three-dot menu of the desired instance, and select Open Instance Folder. After copying the CSV files, run:
```python UploadCSV.py --user "neo4j" --password "prueba123" --database "prueba"```.
The ```--user``` and ```--database``` parameters are optional. By default, both are set to ```"neo4j"```.
