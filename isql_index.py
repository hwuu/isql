#
# Hao, created: 01/29/2015, modified: 01/29/2015
#

import csv
import os
import re
import time
import pyodbc

#

def db_open(en):
    #
    conn = pyodbc.connect("DSN=%s;UID=%s;PWD=%s" \
           % (env["db_dsn"], env["db_username"], env["db_password"]),
              autocommit=False)
    return conn

#

def db_close(conn):
    #
    conn.close()

#

def generate_index_csv_files(conn, env):
    #
    cursor = conn.cursor()
    #
    # Build the map of keyword-kid,
    # and output the csv table of keywords.
    #
    m_keyword_kid = dict()
    #
    csv_file_K = open("isql_%s_K.csv" % env["name"], "w")
    csv_writer_K = csv.writer(csv_file_K, delimiter=',',
        quotechar='\"', quoting=csv.QUOTE_ALL)
    #
    cursor.execute("""
        select count(*) from %s
        """ % env["db_table"])
    n_row = cursor.fetchone()[0]
    cursor.execute("""
        select %s, %s from %s
        """ % (env["db_id_column"],
               env["db_content_column"],
               env["db_table"]))
    s_keyword = set()
    n = 0
    for row in cursor:
        n += 1
        if n % 10000 == 0:
            print "T: " + str(n) + " / " + str(n_row)
        v_word = split(row[1])
        for word in v_word:
            word = clean(word)
            if word != "" and word not in s_keyword:
                s_keyword.add(word)
    v_keyword = sorted(s_keyword)
    n = 0
    for kid in range(0, len(v_keyword)):
        n += 1
        if n % 10000 == 0:
            print "K: " + str(n) + " / " + str(len(v_keyword))
        m_keyword_kid[v_keyword[kid]] = kid
        csv_writer_K.writerow([kid, v_keyword[kid]])
    #
    csv_file_K.close()
    #
    # Build the map of prefix-lkid-ukid, and
    # output the csv table of prefixes.
    # Columns:
    #     v    - prefix
    #     h    - head of the prefix, i.e. prefix[:-1]
    #     lkid - lower bound of the kid range
    #     ukid - upper bound of the kid range
    #
    m_prefix_lkid_ukid = dict()
    #
    csv_file_P = open("isql_%s_P.csv" % env["name"], "w")
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
        if n % 10000 == 0:
            print "P: " + str(n) + " / " + str(len(m_prefix_lkid_ukid))
        [lkid, ukid] = m_prefix_lkid_ukid[prefix]
        csv_writer_P.writerow([prefix, prefix[:-1], lkid, ukid])
    #
    csv_file_P.close()
    #
    # Output the csv table of the inverted index
    # Columns:
    #     kid: keyword id
    #     rid: record id
    #
    csv_file_I = open("isql_%s_I.csv" % env["name"], "w")
    csv_writer_I = csv.writer(csv_file_I, delimiter=',',
        quotechar='\"', quoting=csv.QUOTE_ALL)
    #
    cursor.execute("""
        select %s, %s from %s
        """ % (env["db_id_column"],
               env["db_content_column"],
               env["db_table"]))
    n = 0
    for row in cursor:
        n += 1
        if n % 10000 == 0:
            print "I: " + str(n) + " / " + str(n_row)
        rid = int(row[0])
        v_word = split(row[1])
        for word in v_word:
            word = clean(word)
            if word == "":
                continue
            kid = m_keyword_kid[word]
            csv_writer_I.writerow([kid, rid])
    #
    csv_file_I.close()

#

def transfer_index_csv_files(env):
    #
    # Compress all csv files
    #
    os.system("tar czvf isql_%s.tar.gz isql_%s_*.csv" \
              % (env["name"], env["name"]))
    #
    # Transfer the .tar.gz file to the data server
    #
    os.system("scp -i %s isql_%s.tar.gz %s@%s:%s" \
              % (env["ds_key_file"], env["name"],
                 env["ds_username"], env["ds_ip_addr"],
                 env["ds_folder"]))

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
        "ds_folder":    "/home/jzhou/isql/"}
    #
    conn = db_open(env)
    #
    # Build index
    #
    start_time = time.time()
    print "Building index ...",
    #generate_index_csv_files(conn, env)
    print "Done. " + str(time.time() - start_time)
    #
    # Transfer
    #
    start_time = time.time()
    print "Transferring ...",
    transfer_index_csv_files(env)
    print "Done. " + str(time.time() - start_time)
    #
    db_close(conn)

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

