# Property Graphs

Property Graphs lab deliverable for the SDM course in MDS.

## Project Execution Instructions

Run the commands below from the project root.
If you are using PowerShell, commands are written in copy/paste one-line format. 

The scripts are assumed to be executed from ```A/A.2/``` folder.

## 1) Convert DBLP XML to CSV

This script comes from ThomHurks' repository:
https://github.com/ThomHurks/dblp-to-csv

General command:

```powershell
python XMLToCSV.py PATH_TO_XML_FILE PATH_TO_DTD_FILE OUTPUT_FOLDER --annotate
```

Recommended command:

```powershell
python XMLToCSV.py data/xml_dblp_data/dblp.xml data/xml_dblp_data/dblp.dtd data/csv_dblp_data/output.csv --annotate
```

## 2) Generate model CSV files

This step uses the `articles`, `inproceedings`, and `proceedings` CSVs to generate node and relationship CSV files for the graph model.

`--limit` sets the maximum number of rows read from each input CSV.

General command:

```powershell
python FormatCSV.py --article PATH_TO_ARTICLE_CSV --article-header PATH_TO_ARTICLE_HEADER_CSV --inproceedings PATH_TO_INPROCEEDINGS_CSV --inproceedings-header PATH_TO_INPROCEEDINGS_HEADER_CSV --out OUTPUT_FOLDER --limit MAX_NUMBER
```

Optional arguments:

```powershell
--proceedings PATH_TO_PROCEEDINGS_CSV --proceedings-header PATH_TO_PROCEEDINGS_HEADER_CSV
```

Example (if step 1 was executed as recommended):

```powershell
python FormatCSV.py --article data/csv_dblp_data/output_article.csv --article-header data/csv_dblp_data/output_article_header.csv --inproceedings data/csv_dblp_data/output_inproceedings.csv --inproceedings-header data/csv_dblp_data/output_inproceedings_header.csv --proceedings data/csv_dblp_data/output_proceedings.csv --proceedings-header data/csv_dblp_data/output_proceedings_header.csv --out data/csv_graphmodel_data --limit 10000
```

## 3) Upload CSV files to the graph database

This script copies the CSV files generated in step 2 and loads them into the target graph database.

General command:

```powershell
python UploadCSV.py --password PASSWORD --uri URI --csv-dir MODEL_CSV_FOLDER --user USER --database DATABASE
```

Default values:

- `--uri`: `neo4j://127.0.0.1:7687`
- `--user`: `neo4j`
- `--database`: `neo4j`

Example (using default `uri`, `user`, and `database`):

```powershell
python UploadCSV.py --password "prueba123" --csv-dir data/csv_graphmodel_data
```
