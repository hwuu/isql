#
# Hao, created: 01/29/2015, modified: 02/04/2015
#
# There are 5 index tables:
#
# 1. K (keywords):
#
#    kid  - keyword id
#    str  - keyword string
#
# 2. P (prefixes):
#
#    v    - prefix
#    h    - head of the prefix, i.e. v[:-1]
#    lkid - lower bound of the kid range
#    ukid - upper bound of the kid range
#
# 3. I (inverted index):
#
#    kid: keyword id
#    rid: record id
#
# 4. S (similar prefixes):
#
#    w - input prefix
#    m - max. edit distance
#    v - similar prefix
#    d - ed(w, v)
#    p - the editing operation that converts w to v
#        1: deletion, 2: match, 3: insertion, 4: substitution
#    i - # of insertions
#
# 5. R (records of similar prefixes)
#
#    w - input prefix
#    m - max. edit distance
#    n - number of similar prefixes
#

import csv
import os
import re
import time
import pyodbc
from subprocess import Popen, PIPE

#
##
#

def pull_data_file_from_ds(conn, env):
    #
    tbl_T = env["db_table"]
    col_T_id = env["db_id_column"]
    col_T_content = env["db_content_column"]
    tbl_D = "_" + tbl_T + "_D_"
    #
    cursor = conn.cursor()
    #
    # Create data table 'D' in DB, and load data into it.
    #
    try:
        sql = "drop table %s" % tbl_D
        cursor.execute(sql)
    except Exception, e:
        print e
    sql = "create table %s (rid integer, content nvarchar(1000))" % tbl_D
    cursor.execute(sql)
    sql = "insert into %s select %s, %s from %s order by %s" \
          % (tbl_D, col_T_id, col_T_content, tbl_T, col_T_id)
    cursor.execute(sql)
    #
    # Export data table 'D' to the remote folder.
    #
    sql = "export %s as csv into '%s' with replace" \
          % (tbl_D, env["ds_folder"])
    cursor.execute(sql)
    #
    # Compress the data file in the remote folder.
    #
    cmd = "sudo ssh -i %s %s@%s "\
          "\"cd %s; tar -czvf %s.tar.gz ./export/%s/_D/%s/data\"" \
          % (env["ds_key_file"], env["ds_username"], env["ds_ip_addr"],
             env["ds_folder"], tbl_D, env["db_schema"], tbl_D)
    run(cmd)
    #
    # Pull the compressed file to the local folder.
    #
    cmd = "sudo scp -i %s %s@%s:%s/%s.tar.gz ." \
          % (env["ds_key_file"], env["ds_username"], env["ds_ip_addr"],
             env["ds_folder"], tbl_D)
    run(cmd)
    #
    # Uncompress and rename the file.
    #
    cmd = "tar -zxvf %s.tar.gz" % tbl_D
    run(cmd)
    cmd = "mv ./export/%s/_D/%s/data %s.csv" \
          % (env["db_schema"], tbl_D, tbl_D)
    run(cmd)

#
##
#

def create_index_files(conn, env):
    #
    tbl_T = env["db_table"]
    tbl_D = "_" + tbl_T + "_D_"
    tbl_K = "_" + tbl_T + "_K_"
    tbl_P = "_" + tbl_T + "_P_"
    tbl_I = "_" + tbl_T + "_I_"
    #
    cursor = conn.cursor()
    #
    # Build the map of keyword-kid and output 'K'.
    #
    m_keyword_kid = dict()
    #
    csv_file_K = open(tbl_K + ".csv", "w")
    csv_writer_K = csv.writer(csv_file_K, delimiter=',',
        quotechar='\"', quoting=csv.QUOTE_ALL)
    #
    cursor.execute("""
            select count(*) from %s
            """ % env["db_table"])
    n_row = cursor.fetchone()[0]
    s_keyword = set()
    n = 0
    csv_file_D = open(tbl_D + ".csv", "r")
    csv_reader_D = csv.reader(csv_file_D)
    for row in csv_reader_D:
        n += 1
        #if n % 10000 == 0:
        #    print "T: " + str(n) + " / " + str(n_row)
        v_word = split(row[1])
        for word in v_word:
            word = clean(word)
            if word != "" and word not in s_keyword:
                s_keyword.add(word)
    v_keyword = sorted(s_keyword)
    n = 0
    for kid in range(0, len(v_keyword)):
        n += 1
        #if n % 10000 == 0:
        #    print "K: " + str(n) + " / " + str(len(v_keyword))
        m_keyword_kid[v_keyword[kid]] = kid
        csv_writer_K.writerow([kid, v_keyword[kid]])
    #
    csv_file_D.close()
    csv_file_K.close()
    #
    # Build the map of prefix-lkid-ukid and output 'P'.
    #
    m_prefix_lkid_ukid = dict()
    #
    csv_file_P = open(tbl_P + ".csv", "w")
    csv_writer_P = csv.writer(csv_file_P, delimiter=',',
        quotechar='\"', quoting=csv.QUOTE_ALL)
    #
    for kid in range(0, len(v_keyword)):
        keyword = v_keyword[kid]
        for j in range(0, len(keyword) + 1):
            prefix = keyword[0:j]
            if prefix in m_prefix_lkid_ukid:
                m_prefix_lkid_ukid[prefix][1] = kid
            else:
                m_prefix_lkid_ukid[prefix] = [kid, kid]
    n = 0
    for prefix in sorted(m_prefix_lkid_ukid):
        n += 1
        #if n % 10000 == 0:
        #    print "P: " + str(n) + " / " + str(len(m_prefix_lkid_ukid))
        [lkid, ukid] = m_prefix_lkid_ukid[prefix]
        csv_writer_P.writerow([prefix, prefix[:-1], lkid, ukid])
    #
    csv_file_P.close()
    #
    # Output 'I'.
    #
    csv_file_I = open(tbl_I + ".csv", "w")
    csv_writer_I = csv.writer(csv_file_I, delimiter=',',
        quotechar='\"', quoting=csv.QUOTE_ALL)
    #
    n = 0
    csv_file_D = open(tbl_D + ".csv", "r")
    csv_reader_D = csv.reader(csv_file_D)
    for row in csv_reader_D:
        n += 1
        #if n % 10000 == 0:
        #    print "I: " + str(n) + " / " + str(n_row)
        rid = int(row[0])
        v_word = split(row[1])
        for word in v_word:
            word = clean(word)
            if word == "":
                continue
            kid = m_keyword_kid[word]
            csv_writer_I.writerow([kid, rid])
    #
    csv_file_D.close()
    csv_file_I.close()

#
##
#

def push_index_files_to_ds(conn, env):
    #
    tbl_T = env["db_table"]
    tbl_K = "_" + tbl_T + "_K_"
    tbl_P = "_" + tbl_T + "_P_"
    tbl_I = "_" + tbl_T + "_I_"
    tbl_S = "_" + tbl_T + "_S_"
    tbl_R = "_" + tbl_T + "_R_"
    idx_file_name = "_" + tbl_T + "_idx_.tar.gz"
    #
    cursor = conn.cursor()
    #
    # Compress all csv files to a single index file.
    #
    cmd = "tar -czvf %s %s.csv %s.csv %s.csv" \
          % (idx_file_name, tbl_K, tbl_P, tbl_I)
    run(cmd)
    #
    # Transfer the index file to the remote data server.
    #
    cmd = "scp -i %s %s %s@%s:%s" \
          % (env["ds_key_file"], idx_file_name,
             env["ds_username"], env["ds_ip_addr"],
             env["ds_folder"])
    run(cmd)
    #
    # Uncompress the index file from the remote data server.
    #
    cmd = "sudo ssh -i %s %s@%s \"cd %s; tar -zxvf %s\"" \
          % (env["ds_key_file"], env["ds_username"], env["ds_ip_addr"],
             env["ds_folder"], idx_file_name)
    run(cmd)
    #
    # Create index tables in DB.
    #
    m_tbl_sql = {
            tbl_K: "create table %s (kid integer, str varchar(128))" % tbl_K,
            tbl_P: "create table %s (v varchar(128), h varchar(128), " \
                   "lkid integer, ukid integer)" % tbl_P,
            tbl_I: "create table %s (kid integer, rid integer)" % tbl_I,
            tbl_S: "create table %s (id integer, " \
                   "w varchar(128), m integer, v varchar(128), d integer, " \
                   "p integer, i integer)" % tbl_S,
            tbl_R: "create table %s (w varchar(128), " \
                   "m integer, n integer)" % tbl_R}
    for t in m_tbl_sql:
        try:
            sql = "drop table %s" % t
            cursor.execute(sql)
        except Exception, e:
            print e
        cursor.execute(m_tbl_sql[t])
    #
    # Import index files into the index tables.
    #
    v_tbl = [tbl_K, tbl_P, tbl_I]
    for t in v_tbl:
        sql = "import from csv file '%s/%s.csv' into %s " \
              "with record delimited by '\n' field delimited by ',' " \
              "batch 10000" % (env["ds_folder"], t, t)
        cursor.execute(sql)
    #
    # Create indexes for index tables.
    #
    m_tbl_v_col = {
            tbl_K: ["kid", "str"],
            tbl_P: ["v", "h", "lkid", "ukid"],
            tbl_I: ["kid", "rid"],
            tbl_S: ["id", "w", "m", "v"],
            tbl_R: ["w", "m"]}
    for t in m_tbl_v_col:
        for c in m_tbl_v_col[t]:
            sql = "create index _IDX_%s_%s_ on %s (%s)" % (t, c, t, c)
            cursor.execute(sql)
    #
    # Create id sequence for table 'S'.
    #
    try:
        sql = "create sequence _SEQ_%s_id_ " \
              "minvalue 0 start with 0" % tbl_S
        cursor.execute(sql)
    except Exception , e:
        print e

#
##
#

def run(cmd):
    #
    #os.system(cmd)
    p = Popen(cmd , shell=True, stdout=PIPE, stderr=PIPE)
    out, err = p.communicate()
    #print "Return code: ", p.returncode
    #print out.rstrip(), err.rstrip()

#

def split(s):
    #
    return re.split(r"[^\w]+", s)

#

def clean(s):
    #
    s = s.lower().strip().encode("punycode")
    return re.sub(r"[^\w]+", "", s)

#
##
#

if __name__ == "__main__":
    #
    env = {
        "name":         "DBLP",
        "db_dsn":       "hana2",
        "db_username":  "HPC",
        "db_password":  "Initial1234",
        "db_schema":    "HPC",
        "db_table":     "DBLP",
        "db_id_column": "ID",
        "db_content_column": "TITLE",
        "ds_username":  "jzhou",
        "ds_ip_addr":   "10.128.84.28",
        "ds_key_file":  "jzhou_10_128_84_28",
        "ds_folder":    "/home/jzhou/isql/tmp/"}
    #
    conn = pyodbc.connect("DSN=%s;UID=%s;PWD=%s" \
           % (env["db_dsn"], env["db_username"], env["db_password"]),
              autocommit=False)
    #
    start_time = time.time()
    print "Pulling data file ...",
    #pull_data_file_from_ds(conn, env)
    print "Done. " + str(time.time() - start_time)
    #
    start_time = time.time()
    print "Creating index files ...",
    #create_index_files(conn, env)
    print "Done. " + str(time.time() - start_time)
    #
    start_time = time.time()
    print "Pushing index files ...",
    push_index_files_to_ds(conn, env)
    print "Done. " + str(time.time() - start_time)
    #
    conn.close()

#
##
###
##
#
#
##
###
##
#
#
##
###
##
#
#
##
###
##
#
#
##
###
##
#

