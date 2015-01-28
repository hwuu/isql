set schema HPC;

--
-- Note (01/28/2015):
--
-- TITLE and AUTHORS must be NVARCHAR instead of VARCHAR,
-- otherwise, when issuing:
--     select id, title from dblp
-- there will be the following error raised:
--     pyodbc.Error: ('HY000', '[HY000] [SAP AG][LIBODBCHDB SO][HDB]
--     General error;-1onversion of parameter/column (2) from data type
--     VARCHAR1 to ASCII failed (-10SQLGetData)')
-- The solution is found at:
--     http://scn.sap.com/thread/3673963
-- And the diff between NVARCHAR and VARCHAR is described at:
--     http://goo.gl/Sv6u9
--

CREATE COLUMN TABLE DBLP (
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
