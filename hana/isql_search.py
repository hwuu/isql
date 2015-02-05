#
# Hao, created: 01/29/2015, modified: 02/05/2015
#

import re
import time
import pyodbc

#

def exact_search(conn, env, query):
    #
    tbl_T = env["db_table"]
    col_T_id = env["db_id_column"]
    tbl_P = "_" + tbl_T + "_P_"
    tbl_I = "_" + tbl_T + "_I_"
    #
    sql = ""
    v_prefix = split(query)
    for prefix in v_prefix:
        prefix = clean(prefix)
        if prefix == "":
            continue
        if sql != "":
            sql += "intersect"
        sql +=  """
                select T.* from %s as P, %s as I, %s as T
                where  P.v = '%s' and
                       P.lkid <= I.kid  and
                       I.kid  <= P.ukid and
                       I.rid = T.%s
                """ % (tbl_P, tbl_I, tbl_T, prefix, col_T_id)
    if sql != "":
        cursor = conn.cursor()
        cursor.execute(sql)
        n = 0
        for row in cursor:
            n += 1
            #print row
            if n == 10:
                break

#

def fuzzy_search(conn, env, query, t):
    #
    tbl_T = env["db_table"]
    col_T_id = env["db_id_column"]
    tbl_P = "_" + tbl_T + "_P_"
    tbl_I = "_" + tbl_T + "_I_"
    tbl_S = "_" + tbl_T + "_S_"
    #
    sql = ""
    v_wp = split(query)
    for wp in v_wp:
        m = int(len(wp) * t)
        if len(wp) <= m:
            continue
        if sql != "":
            sql += "intersect"
        insert_similar_prefixes(conn, env, wp, m)
        sql +=  """
                select T.* from %s as S, %s as P, %s as I, %s as T
                where  S.w = '%s' and S.m = %d and S.v = P.v and
                       P.lkid <= I.kid and I.kid <= P.ukid and
                       I.rid = T.%s
                """ % (tbl_S, tbl_P, tbl_I, tbl_T, wp, m, col_T_id)
    if sql != "":
        cursor = conn.cursor()
        cursor.execute(sql)
        n = 0
        for row in cursor:
            n += 1
            #print "  ", row
            if n == 10:
                break

#

def insert_similar_prefixes(conn, env, wp, m):
    #
    tbl_T = env["db_table"]
    col_T_id = env["db_id_column"]
    tbl_P = "_" + tbl_T + "_P_"
    tbl_I = "_" + tbl_T + "_I_"
    tbl_S = "_" + tbl_T + "_S_"
    tbl_R = "_" + tbl_T + "_R_"
    seq = "_SEQ_" + tbl_S + "_ID_"
    #
    cursor = conn.cursor()
    cursor.execute("""
            select n from %s as R where w = '%s' and m = %d
            """ % (tbl_R, wp, m))
    row = cursor.fetchone()
    if row != None:
        return
    if wp == "":
        cursor.execute("""
                insert into %s (id, w, m, v, d, p, i)
                select %s.nextval, '', %d, v, length(v), 3, 1
                from %s where length(v) <= %d
                """ % (tbl_S, seq, m, tbl_P, m))
        cursor.execute("""
                insert into %s
                select '', %d, count(*) from %s
                """ % (tbl_R, m, tbl_S))
        return
    #
    w = wp[:-1]
    c = wp[-1]
    insert_similar_prefixes(conn, env, w, m)
    #
    # Deletion.
    #
    cursor.execute("""
            insert into %s (id, w, m, v, d, p, i)
            select %s.nextval, '%s', %d, v, d + 1, 1, 0 from %s
            where  w = '%s' and d < %d and m = %d
            """ % (tbl_S, seq, wp, m, tbl_S, w, m, m))
    #
    # Match.
    #
    cursor.execute("""
            insert into %s (id, w, m, v, d, p, i)
            select %s.nextval, '%s', %d, P.v, d, 2, 0
            from %s as S, %s as P
            where  S.w = '%s' and S.m = %d and
                   P.v = concat(S.v, '%s')
            """ % (tbl_S, seq, wp, m, tbl_S, tbl_P, w, m, c))
    #
    # Insertion.
    #
    for i in range(0, m):
        p = 2 if i == 0 else 3
        cursor.execute("""
                insert into %s (id, w, m, v, d, p, i)
                select %s.nextval, S.w, S.m, P.v, S.d + 1, 3, S.i + 1
                from   %s as S, %s as P
                where  S.w = '%s' and S.m = %d and
                       S.v = P.h and S.d < S.m and
                       S.p = %d and S.i = %d
                """ % (tbl_S, seq, tbl_S, tbl_P, wp, m, p, i))
    #
    # Substitution.
    #
    cursor.execute("""
            insert into %s (id, w, m, v, d, p, i)
            select %s.nextval, '%s', %d, P.v, d + 1, 4, 0
            from   %s as S, %s as P
            where  S.w = '%s' and S.m = %d and
                   P.v <> concat(S.v, '%s') and
                   P.h = S.v and S.d < %d
            """ % (tbl_S, seq, wp, m, tbl_S, tbl_P, w, m, c, m))
    #
    # Remove incorrect entries in 'S'.
    #
    cursor.execute("""
            delete from %s
            where id in (
                select S1.id from %s as S1
                inner join %s as S2
                on S1.w = S2.w and S1.v = S2.v and
                   S1.m = S2.m and S1.d > S2.d)
            """ % (tbl_S, tbl_S, tbl_S))
    #
    # Insert count of entries of <wp, m> in S into R.
    #
    cursor.execute("""
            insert into %s
            select '%s', %d, count(*) from %s
            where w = '%s' and m = %d
            """ % (tbl_R, wp, m, tbl_S, wp, m))
    #
    # Commit.
    #
    conn.commit()

#
##
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

def test_exact_search(conn, env, query):
    #
    query = query.strip()
    for i in range(0, len(query)):
        q = query[:(i + 1)]
        start_time = time.time()
        print ">>>> '%s'" % q
        exact_search(conn, env, q)
        print "     " + str(time.time() - start_time)

#

def test_fuzzy_search(conn, env, query, t):
    #
    query = query.strip()
    for i in range(0, len(query)):
        q = query[:(i + 1)]
        start_time = time.time()
        print ">>>> '%s'" % q
        fuzzy_search(conn, env, q, t)
        print "     " + str(time.time() - start_time)

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
        "db_content_column": "TITLE"}
    #
    conn = pyodbc.connect("DSN=%s;UID=%s;PWD=%s" \
           % (env["db_dsn"], env["db_username"], env["db_password"]),
              autocommit=False)
    #
    # Test exact search
    #
    #test_exact_search(conn, env, "dat mi")
    #test_exact_search(conn, env, "mi dat")
    test_exact_search(conn, env, "determin")
    #
    # Test fuzzy search
    #
    #test_fuzzy_search(conn, env, "date", 0.3)
    test_fuzzy_search(conn, env, "determin", 0.3)
    #test_fuzzy_search(conn, env, "date mi", 0.3)
    #test_fuzzy_search(conn, env, "mi dat", 0.3)
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

