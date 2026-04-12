# Property Graphs

Property Graphs lab deliverable for the SDM course in MDS.

## Project Execution Instructions

Run the commands below from the folder where the corresponding script is located.  
If you are using PowerShell, commands are written in copy/paste one-line format.

Before running the scripts, install the required Python packages from `requirements.txt`:
```
pip install -r requirements.txt
```

This installs the packages used by the project, including `lxml` and `neo4j`.

The scripts are assumed to be executed from `A/A.2/` or `A/A.3/`.

The project uses these data folders at project root:

- `data/xml_dblp_data/`
- `data/csv_dblp_data/`
- `data/csv_graphmodel_A2_data/`
- `data/csv_graphmodel_A3_data/`

Before running the project, the DBLP source files must be downloaded from:

https://dblp.org/xml/

Required files:

- `dblp.xml.gz`
- `dblp.dtd`

After downloading:

1. Extract the XML file contained inside `dblp.xml.gz`.
2. Place the extracted XML file and `dblp.dtd` inside `data/xml_dblp_data/`.

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

Parameter summary for `XMLToCSV.py`:

- `--xml-filename`: XML input file name inside `data/xml_dblp_data/` (`dblp.xml` by default).
- `--dtd-filename`: DTD file name inside `data/xml_dblp_data/` (`dblp.dtd` by default).
- `--outputfile`: base name used for the generated CSV files in `data/csv_dblp_data/`.
- `--annotate`: generates extra header files with type information.
- `--neo4j`: generates Neo4j-oriented headers and implies `--annotate`.
- `--relations`: optional list of `attribute:relation` mappings to export some attributes as explicit relations.

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

Parameter summary for `FormatCSV.py` and `FormatUpdateCSV.py`:

- `--article`, `--article-header`, `--inproceedings`, `--inproceedings-header`, `--proceedings`, `--proceedings-header`: input file names read from `data/csv_dblp_data/`.
- `--target-articles`: number of article rows selected for the curated dataset.
- `--target-inproceedings`: number of inproceedings rows selected for the curated dataset.
- `--scan-articles`, `--scan-inproceedings`, `--scan-proceedings`: optional limits on how many rows are scanned from each input file.
- `--reviewers-per-paper`: number of synthetic reviewers assigned to each paper.
- `--min-internal-cites`: minimum number of internal citations each paper should have after reinforcement.
- `--max-internal-cites`: maximum number of internal citations each paper may have after reinforcement.
- `--seed`: random seed used to make the generated dataset reproducible.

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

Parameter summary for `UploadCSV.py` and `UploadUpdateCSV.py`:

- `--user`: Neo4j username (`neo4j` by default).
- `--password`: Neo4j password. This is the only required parameter.
- `--database`: Neo4j database name (`neo4j` by default).
- `--batch-size`: number of CSV rows sent to Neo4j per batch (`1000` by default).
- `--uri`: Neo4j connection URI (`neo4j://127.0.0.1:7687` by default).

## 4) Execute queries from python scripts

For section `D`, the Neo4j instance must have the `GDS` (`Graph Data Science`) plugin installed and enabled before running the scripts.
After installing the plugin, the instance must be restarted. 
Without this plugin, the graph algorithms used in `D1.py` and `D2.py` will not be available.

General command:
X is the letter of the section and Y is the script number
```
python XY.py --password TU_PASSWORD
```

Example:
```
python B1.py --password TU_PASSWORD
```

Parameter summary for query scripts:

- `B1.py`, `B2.py`, `B3.py`, `B4.py`: use `--uri`, `--user`, `--password` to connect to Neo4j.
- `C1.py`, `C2.py`, `C3.py`, `C4.py`: use `--uri`, `--user`, `--password` to connect to Neo4j.
- `D1.py`: uses `--uri`, `--user`, `--password`, `--database`, plus `--limit` for the number of ranked papers, `--max-iterations` for PageRank iterations, and `--damping-factor` for the PageRank damping value.
- `D2.py`: uses `--uri`, `--user`, `--password`, `--database`, plus `--limit` for the number of communities shown, `--sample-size` for the number of sample paper titles per community, and `--keyword-sample` for the number of sample keywords per community.




