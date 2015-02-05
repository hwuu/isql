#
# Hao, created: 02/04/2015, modified: 02/05/2015
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
import mysql.connector
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
    sql = "insert into %s select %s, " \
          "replace(%s, '\\n', '') " \
          "from %s order by %s" \
          % (tbl_D, col_T_id, col_T_content, tbl_T, col_T_id)
    cursor.execute(sql)
    conn.commit()
    #
    # Export data table 'D' to the tmp folder.
    # Note 1: Existing csv file must be deleted in advance.
    # Note 2: AppArmor should be configured by following the instructions
    #         at http://goo.gl/yAEyx2 to avoid permission issues.
    #
    cmd = "rm %s/%s.csv" % (env["ds_folder"], tbl_D)
    run(cmd)
    sql = "select * from %s into outfile '%s/%s.csv' " \
          "fields enclosed by '\"' terminated by ',' " \
          "lines terminated by '\\n'" \
          % (tbl_D, env["ds_folder"], tbl_D)
    cursor.execute(sql)
    #
    # Move the data file to the local folder.
    #
    cmd = "mv %s/%s.csv ." % (env["ds_folder"], tbl_D)
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
    tbl_D = "_" + tbl_T + "_D_"
    tbl_K = "_" + tbl_T + "_K_"
    tbl_P = "_" + tbl_T + "_P_"
    tbl_I = "_" + tbl_T + "_I_"
    tbl_S = "_" + tbl_T + "_S_"
    tbl_R = "_" + tbl_T + "_R_"
    #
    cursor = conn.cursor()
    #
    # Move index files to the tmp folder.
    #
    run("mv %s.csv %s" % (tbl_D, env["ds_folder"]))
    run("mv %s.csv %s" % (tbl_K, env["ds_folder"]))
    run("mv %s.csv %s" % (tbl_P, env["ds_folder"]))
    run("mv %s.csv %s" % (tbl_I, env["ds_folder"]))
    #
    # Create index tables in DB.
    #
    m_tbl_sql = {
            tbl_K: "create table %s (kid integer, str varchar(128)) " % tbl_K,
            tbl_P: "create table %s (v varchar(128), h varchar(128), " \
                   "lkid integer, ukid integer) " % tbl_P,
            tbl_I: "create table %s (kid integer, rid integer) " % tbl_I,
            tbl_S: "create table %s (id integer primary key auto_increment, " \
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
        sql = "load data infile '%s/%s.csv' into table %s " \
              "fields terminated by ',' enclosed by '\"' " \
              "lines terminated by '\\r\\n'" % (env["ds_folder"], t, t)
        cursor.execute(sql)
    #
    # Create indexes for index tables.
    #
    m_tbl_v_col = {
            tbl_K: ["kid", "str"],
            tbl_P: ["v", "h", "lkid", "ukid"],
            tbl_I: ["kid", "rid"],
            tbl_S: ["w", "m", "v"],
            tbl_R: ["w", "m"]}
    for t in m_tbl_v_col:
        for c in m_tbl_v_col[t]:
            sql = "create index _IDX_%s_%s_ on %s (%s)" % (t, c, t, c)
            cursor.execute(sql)

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
        "db_host":      "10.128.80.204",
        "db_username":  "hpc",
        "db_password":  "q1w2e3r4",
        "db_schema":    "test",
        "db_table":     "DBLP",
        #"db_table":     "Book",
        "db_id_column": "id",
        "db_content_column": "title",
        "ds_username":  "mwu",
        "ds_ip_addr":   "10.128.80.204",
        "ds_key_file":  "mwu_10_128_80_204",
        "ds_folder":    "/home/mwu/isql/mysql/tmp/"}
    #
    conn = mysql.connector.connect(
           user=env["db_username"], password=env["db_password"],
           host=env["db_host"], database=env["db_schema"])
    #
    start_time = time.time()
    print "Pulling data file ...",
    pull_data_file_from_ds(conn, env)
    print "Done. " + str(time.time() - start_time)
    #
    start_time = time.time()
    print "Creating index files ...",
    create_index_files(conn, env)
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

