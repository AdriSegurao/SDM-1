# Property Graphs

Property Graphs lab deliverable for the SDM course in MDS.

## Project Execution Instructions

Run the commands below from the folder where the corresponding script is located.  
If you are using PowerShell, commands are written in copy/paste one-line format.

The scripts are assumed to be executed from `A/A.2/` or `A/A.3/`.

The project uses these data folders at project root:

- `data/xml_dblp_data/`
- `data/csv_dblp_data/`
- `data/csv_graphmodel_A2_data/`
- `data/csv_graphmodel_A3_data/`

## 1) Convert DBLP XML to CSV

This script comes from ThomHurks' repository:  
https://github.com/ThomHurks/dblp-to-csv

The adapted version reads the XML and DTD from `data/xml_dblp_data/` and generates only:

- `output_article.csv`
- `output_article_header.csv`
- `output_inproceedings.csv`
- `output_inproceedings_header.csv`
- `output_proceedings.csv`
- `output_proceedings_header.csv`

in `data/csv_dblp_data/`.

General command:
```
python XMLtoCSV.py --outputfile OUTPUT_NAME.csv --annotate
```

Recommended command:
```
python XMLtoCSV.py --outputfile output.csv --annotate
```

## 2) Generate model CSV files
This step uses the `article`, `inproceedings`, and `proceedings` CSVs generated in step 1 to create the node and relationship CSV files for the graph model.

`FormatCSV.py` automatically reads from `data/csv_dblp_data/` and writes the result directly to `data/csv_graphmodel_A2_data/` or `data/csv_graphmodel_A3_data/`, depending on the task.

General command for A.2:
```
python FormatCSV.py --target-articles N1 --target-inproceedings N2 --min-internal-cites N3 --max-internal-cites N4 --reviewers-per-paper N5 --seed N6
```

General command for A.3:
```
python FormatUpdateCSV.py --target-articles N1 --target-inproceedings N2 --min-internal-cites N3 --max-internal-cites N4 --reviewers-per-paper N5 --seed N6
```

Recommended command for A.2:
```
python FormatCSV.py
```

Recommended command for A.3:
```
python FormatUpdateCSV.py
```

## 3) Upload CSV files to the graph database

This script reads the CSV files generated in step 2 from `data/csv_graphmodel_A2_data/` or `data/csv_graphmodel_A3_data/` and loads them into Neo4j.

General command A.2:
```
python UploadCSV.py --password PASSWORD --uri URI --user USER --database DATABASE --batch-size BATCH_SIZE
```

General command A.3:
```
python UploadUpdateCSV.py --password PASSWORD --uri URI --user USER --database DATABASE --batch-size BATCH_SIZE
```

Recommended command A.2:
```
python UploadCSV.py --password TU_PASSWORD
```

Recommended command A.3:
```
python UploadUpdateCSV.py --password TU_PASSWORD
```

## 3) Execute queries from python scripts

General command:
X is the letter of the section and Y is the script number
```
python XY.py --password TU_PASSWORD
```

Example:
```
python B1.py --password TU_PASSWORD
```




