set schema HPC;

IMPORT FROM CSV FILE '/home/jzhou/dblp/dblp.csv' INTO DBLP
WITH
RECORD DELIMITED BY '\n'
FIELD DELIMITED BY ','
BATCH 10000
ERROR LOG '/home/jzhou/dblp/hana_import_errors.log';
