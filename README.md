# Comando XML to CSV
python ./XMLToCSV.py data/xml_dblp_data/dblp.xml data/xml_dblp_data/dblp.dtd data/csv_dblp_data/output.csv --annotate

# Comando FormatCSV
python FormatCSV.py --article data/csv_dblp_data/output_article.csv --article-header data/csv_dblp_data/output_article_header.csv --inproceedings data/csv_dblp_data/output_inproceedings.csv --inproceedings-header data/csv_dblp_data/output_inproceedings_header.csv --out data/csv_graphmodel_data --limit 10000

# Comando UploadCSV
python UploadCSV.py --csv-dir data/csv_graphmodel_data/ --user "neo4j" --password "pruebaprueba" --database "neo4j" --neo4j-uri bolt://127.0.0.1:7687

