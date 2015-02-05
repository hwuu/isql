
#
# create data table
#

drop table if exists DBLP;
CREATE TABLE DBLP (
      `id` INTEGER PRIMARY KEY,
      `key` VARCHAR(200),
      `mdate` VARCHAR(20),
      `type` VARCHAR(20),
      `title` NVARCHAR(1000),
      `authors` NVARCHAR(2000),
      `booktitle` VARCHAR(200),
      `year` INTEGER,
      `volume` VARCHAR(50),
      `number` VARCHAR(50),
      `pages` VARCHAR(50),
      `ee` VARCHAR(1000),
      `url` VARCHAR(200)
);

#
# load data into the data table
#

load data infile 'dblp.csv' into table DBLP fields terminated by ',' enclosed by '"' lines terminated by '\r\n';

#
# create indexes for the data table
#

create index IDX_T_year on DBLP(`year`);

#
# create index tables
#

drop table if exists P;
create table P (v varchar(128), h varchar(128), lkid integer, ukid integer);
#
drop table if exists I;
create table I (kid integer, rid integer);

#
# load data into index tables
#

load data infile 'isql_DBLP_P.csv' into table P fields terminated by ',' enclosed by '"' lines terminated by '\r\n';
load data infile 'isql_DBLP_I.csv' into table I fields terminated by ',' enclosed by '"' lines terminated by '\r\n';

#
# create indexes for index tables
#

create index IDX_P_v on P(v);
create index IDX_P_h on P(h);
create index IDX_P_lkid on P(lkid);
create index IDX_P_ukid on P(ukid);
#
create index IDX_I_kid on I (kid);
create index IDX_I_rid on I (rid);

#
# query
#

select T.* from P, I, DBLP as T
where  P.v = 's' and
P.lkid <= I.kid  and
I.kid  <= P.ukid and
I.rid = T.id and
`year`=2012;

select count(*) from I;

select count(*) from I
where I.kid >= 63342 and I.kid <= 64342;

