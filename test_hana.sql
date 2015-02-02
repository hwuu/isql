
--
-- create data table
--

CREATE TABLE DBLP (
      "ID" INTEGER PRIMARY KEY,
      "KEY" VARCHAR(200),
      "MDATE" VARCHAR(20),
      "TYPE" VARCHAR(20),
      "TITLE" NVARCHAR(1000),
      "AUTHORS" NVARCHAR(2000),
      "BOOKTITLE" VARCHAR(200),
      "YEAR" INTEGER,
      "VOLUME" VARCHAR(50),
      "NUMBER" VARCHAR(50),
      "PAGES" VARCHAR(50),
      "EE" VARCHAR(1000),
      "URL" VARCHAR(200)
);

--
-- load data into the data table
--

IMPORT FROM CSV FILE '/home/jzhou/dblp/dblp.csv' INTO DBLP
WITH
RECORD DELIMITED BY '\n'
FIELD DELIMITED BY ','
BATCH 10000
ERROR LOG '/home/jzhou/dblp/hana_import_errors.log';

--
-- create index tables
--

create table _DBLP_K_ (kid integer, str varchar(128));
--
create table _DBLP_P_ (v varchar(128), h varchar(128), lkid integer, ukid integer);
--
create table _DBLP_I_ (kid integer, rid integer);

--
-- load data into index tables
--

IMPORT FROM CSV FILE '/home/jzhou/isql/isql_DBLP_I.csv' INTO _DBLP_I_
WITH
RECORD DELIMITED BY '\n'
FIELD DELIMITED BY ','
BATCH 10000
ERROR LOG '/home/jzhou/isql/hana_import_errors.log';
--
IMPORT FROM CSV FILE '/home/jzhou/isql/isql_DBLP_K.csv' INTO _DBLP_K_
WITH
RECORD DELIMITED BY '\n'
FIELD DELIMITED BY ','
BATCH 10000
ERROR LOG '/home/jzhou/isql/hana_import_errors.log';
--
IMPORT FROM CSV FILE '/home/jzhou/isql/isql_DBLP_P.csv' INTO _DBLP_P_
WITH
RECORD DELIMITED BY '\n'
FIELD DELIMITED BY ','
BATCH 10000
ERROR LOG '/home/jzhou/isql/hana_import_errors.log';

--
-- create indexes for index tables
--

create index _IDX_DBLP_K_kid_ on _DBLP_K_ (kid);
create index _IDX_DBLP_K_str_ on _DBLP_K_ (str);
--
create index _IDX_DBLP_P_v_ on _DBLP_P_ (v);
create index _IDX_DBLP_P_h_ on _DBLP_P_ (h);
create index _IDX_DBLP_P_lkid_ on _DBLP_P_ (lkid);
create index _IDX_DBLP_P_ukid_ on _DBLP_P_ (ukid);
--
create index _IDX_DBLP_I_kid_ on _DBLP_I_ (kid);
create index _IDX_DBLP_I_rid_ on _DBLP_I_ (rid);

--
-- query
--

select T.* from _DBLP_P_ as P, _DBLP_I_ as I, DBLP as T
where  P.v = 'se' and
P.lkid <= I.kid  and
I.kid  <= P.ukid and
I.rid = T.id;


select * from _DBLP_P_ as P
where P.v = 'decentralized'

select count(*) from _DBLP_I_ as I
--where I.kid = 63342
where I.kid >= 63342 and I.kid <= 63342


select I.rid from _DBLP_P_ as P, _DBLP_I_ as I
where P.v = 'decr' and I.kid >= P.lkid and I.kid <= P.ukid

