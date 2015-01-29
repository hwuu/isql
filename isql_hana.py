#
# Hao, created: 01/29/2015, modified: 01/29/2015
#

import re
import time
import pyodbc

#

def db_open(en):
    #
    conn = pyodbc.connect("DSN=%s;UID=%s;PWD=%s" \
           % (env["dsn"], env["username"], env["password"]),
              autocommit=False)
    return conn

#

def db_close(conn):
    #
    conn.close()

#

def dump_table(conn, table_name, title, limit):
    #
    cursor = conn.cursor()
    cursor.execute("select * from %s limit %d" \
                   % (table_name, limit))
    print title + ":"
    for row in cursor:
        print "  ", row

#

def build_index(conn, env):
    #
    cursor = conn.cursor()
    #
    # Dump the data table
    #
    dump_table(conn, env["table_name"],
        "0. The data table ('%s')" % env["table_name"], 100)
    #
    # Build the table of keywords and the map of keyword-kid
    #
    cursor.execute("""
        select count(*) from %s
        """ % env["table_name"])
    n_row = cursor.fetchone()[0]
    cursor.execute("""
        select %s, %s from %s
        """ % (env["column_name_id"],
               env["column_name_index"],
               env["table_name"]))
    s_keyword = set()
    n = 0
    for row in cursor:
        n += 1
        if n % 10000 == 0:
            print "_T_: " + str(n) + " / " + str(n_row)
        v_word = split(row[1])
        for word in v_word:
            word = clean(word)
            if word != "" and word not in s_keyword:
                s_keyword.add(word)
    v_keyword = sorted(s_keyword)
    m_keyword_kid = dict()
    try:
        cursor.execute("drop table _K_")
    except Exception, e:
        print e
    cursor.execute("""
        create column table _K_ (
            kid integer primary key, str varchar(128))
        """)
    n = 0
    for kid in range(0, len(v_keyword)):
        n += 1
        if n % 10000 == 0:
            print "_K_: " + str(n) + " / " + str(len(v_keyword))
        m_keyword_kid[v_keyword[kid]] = kid
        cursor.execute("""
            insert into _K_ values (%s, '%s')
            """ % (str(kid), v_keyword[kid]))
    #
    dump_table(conn, "_K_", "1. The table of keywords (_K_)", 10)
    #
    # Build the table of prefixes and the map of prefix-lkid-rkid
    # Columns:
    #     v    - prefix
    #     h    - head of the prefix, i.e. prefix[:-1]
    #     lkid - lower bound of the kid range
    #     ukid - upper bound of the kid range
    #
    m_prefix_lkid_ukid = dict()
    for kid in range(0, len(v_keyword)):
        keyword = v_keyword[kid]
        for j in range(0, len(keyword) + 1):
            prefix = keyword[0:j]
            if prefix in m_prefix_lkid_ukid:
                m_prefix_lkid_ukid[prefix][1] = kid
            else:
                m_prefix_lkid_ukid[prefix] = [kid, kid]
    try:
        cursor.execute("drop table _P_")
    except Exception, e:
        print e
    cursor.execute("""
        create column table _P_ (
            v varchar(128), h varchar(128),
            lkid integer, ukid integer)
        """)
    n = 0
    for prefix in sorted(m_prefix_lkid_ukid):
        n += 1
        if n % 10000 == 0:
            print "_P_: " + str(n) + " / " + str(len(m_prefix_lkid_ukid))
        [lkid, ukid] = m_prefix_lkid_ukid[prefix]
        cursor.execute("""
            insert into _P_ values ('%s', '%s', %d, %d)
            """ % (prefix, prefix[:-1], lkid, ukid))
    #
    dump_table(conn, "_P_", "2. The table of prefixes (_P_)", 10)
    #
    # Build the table of the inverted index
    # Columns:
    #     kid: keyword id
    #     rid: record id
    #
    try:
        cursor.execute("drop table _I_")
    except Exception, e:
        print e
    cursor.execute("""
        create column table _I_ (
        kid integer, rid integer)
        """)
    cursor.execute("""
        select %s, %s from %s
        """ % (env["column_name_id"],
               env["column_name_index"],
               env["table_name"]))
    cursor2 = conn.cursor()
    n = 0
    for row in cursor:
        n += 1
        if n % 10000 == 0:
            print "_I_: " + str(n) + " / " + str(n_row)
        rid = int(row[0])
        v_word = split(row[1])
        for word in v_word:
            word = clean(word)
            if word == "":
                continue
            kid = m_keyword_kid[word]
            cursor2.execute("""
                insert into _I_ values (%d, %d)
                """ % (kid, rid))
    #
    dump_table(conn, "_I_", "3. The table of the inverted index (_I_)", 10)
    #
    # Create the table of similar prefixes
    # Columns:
    #     w - input prefix
    #     m - max. edit distance
    #     v - similar prefix
    #     d - ed(w, v)
    #     p - the editing operation that converts w to v
    #         1: deletion, 2: match, 3: insertion, 4: substitution
    #     i - # of insertions
    #
    try:
        cursor.execute("drop table _S_")
    except Exception, e:
        print e
    cursor.execute("""
        create column table _S_ (
            id integer primary key,
            w varchar(128), m integer,
            v varchar(128), d integer,
            p integer, i integer)
        """)
    #
    # Create the table of records of similar prefixes
    # Columns:
    #     w - input prefix
    #     m - max. edit distance
    #     n - number of similar prefixes
    #
    try:
        cursor.execute("drop table _R_")
    except Exception, e:
        print e
    cursor.execute("""
        create column table _R_ (
            w varchar(128), m integer, n integer)
        """)
    #
    # Create indexes
    #
    #cursor.execute("create index idx_T on %s (id)" % env["table_name"])
    cursor.execute("create index idx_P_1 on _P_ (v)")
    cursor.execute("create index idx_P_2 on _P_ (h)")
    cursor.execute("create index idx_I_1 on _I_ (kid)")
    cursor.execute("create index idx_I_2 on _I_ (rid)")
    #cursor.execute("create index idx_S_1 on _S_ (id)")
    cursor.execute("create index idx_S_2 on _S_ (w)")
    cursor.execute("create index idx_S_3 on _S_ (m)")
    cursor.execute("create index idx_S_4 on _S_ (v)")
    cursor.execute("create index idx_R_1 on _R_ (w)")
    cursor.execute("create index idx_R_2 on _R_ (m)")
    #
    conn.commit()

#

def exact_search(conn, env, query):
    #
    sql = ""
    v_prefix = split(query)
    for prefix in v_prefix:
        prefix = clean(prefix)
        if prefix == "":
            continue
        if sql != "":
            sql += "intersect"
        sql += """
            select T.* from _P_, _I_, %s as T
            where  _P_.v = '%s' and
                   _P_.lkid <= _I_.kid  and
                   _I_.kid  <= _P_.ukid and
                   _I_.rid = T.id
            """ % (env["table_name"], prefix)
    if sql != "":
        cursor = conn.cursor()
        cursor.execute(sql)
        for row in cursor:
            print row

#

def fuzzy_search(conn, env, query, t):
    #
    print "\n\"" + query + "\" " + str(t) + ":"
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
        #dump_table(conn, "_S_", "_S_")
        sql += """
            select distinct(_I_.rid) from _S_, _P_, _I_
            where  _S_.w = '%s' and
                   _S_.m = %d and
                   _S_.v = _P_.v and
                   _P_.lkid <= _I_.kid and
                   _I_.kid  <= _P_.ukid
            """ % (wp, m)
    if sql != "":
        cursor = conn.cursor()
        cursor.execute(sql)
        for row in cursor:
            print "  ", row

#

def insert_similar_prefixes(conn, env, wp, m):
    #
    OPT_INDEX = True
    #
    cursor = conn.cursor()
    cursor.execute("""
        select n from _R_ where w = '%s' and m = %d
        """ % (wp, m))
    row = cursor.fetchone()
    if row != None:
        return
    if wp == "":
        cursor.execute("""
            insert into _S_ (w, m, v, d, p, i)
            select "", %d, v, length(v), 3, 1
            from _P_ where length(v) <= %d
            """ % (m, m))
        cursor.execute("""
            insert into _R_
            select '', %d, count(*) from _S_
            """ % m)
        return
    #
    w = wp[:-1]
    c = wp[-1]
    insert_similar_prefixes(conn, env, w, m)
    #
    # Deletion
    #
    conn.execute("""
        insert into _S_ (w, m, v, d, p, i)
        select '%s', %d, v, d + 1, 1, 0 from _S_
        where  w = '%s' and d < %d and m = %d
        """ % (wp, m, w, m, m))
    #
    # Match
    #
    conn.execute("""
        insert into _S_ (w, m, v, d, p, i)
        select '%s', %d, _P_.v, d, 2, 0 from _S_, _P_
        where  _S_.w = '%s' and _S_.m = %d and
               _P_.v = (_S_.v || '%s')
        """ % (wp, m, w, m, c))
    #
    # Insertion
    #
    if OPT_INDEX == True:
        for i in range(0, m):
            p = 2 if i == 0 else 3
            cursor.execute("""
                insert into _S_ (w, m, v, d, p, i)
                select _S_.w, _S_.m, _P_.v, _S_.d + 1, 3, _S_.i + 1
                from   _S_, _P_
                where  _S_.w = '%s' and _S_.m = %d and
                       _S_.v = _P_.h and _S_.d < _S_.m and
                       _S_.p = %d and _S_.i = %d
            """ % (wp, m, p, i))
    else:
        cursor.execute("""
            insert into _S_ (w, m, v, d, p, i)
            select '%s', %d, _P_.v,
                   length(_P_.v) - length(_S_.v) - 1 + d, 3, 0
            from   _S_, _P_
            where  _S_.w = '%s' and _S_.m = %d and
                   length(_P_.v) > length(_S_.v) + 1 and
                   substr(_P_.v, 1, length(_S_.v) + 1) = (_S_.v || '%s') and
                   _S_.d + length(_P_.v) - length(_S_.v) - 1 <= %d
            """ % (wp, m, w, m, c, m))
    #
    # Substitution
    #
    if OPT_INDEX == True:
        cursor.execute("""
            insert into _S_ (w, m, v, d, p, i)
            select '%s', %d, _P_.v, d + 1, 4, 0 from _S_, _P_
            where  _S_.w = '%s' and _S_.m = %d and
                   _P_.v <> (_S_.v || '%s') and
                   _P_.h = _S_.v and
                   _S_.d < %d
            """ % (wp, m, w, m, c, m))
    else:
        cursor.execute("""
            insert into _S_ (w, m, v, d, p, i)
            select '%s', %d, _P_.v, d + 1, 4, 0 from _S_, _P_
            where  _S_.w = '%s' and _S_.m = %d and
                   _P_.v <> (_S_.v || '%s') and
                   substr(_P_.v, 1, length(_P_.v) - 1) = _S_.v and
                   _S_.d < %d
            """ % (wp, m, w, m, c, m))
    #
    # Remove duplicates
    #
    cursor.execute("""
        delete from _S_
        where id in (
            select S1.id from _S_ as S1
            inner join _S_ as S2
            on S1.w = S2.w and S1.v = S2.v and
               S1.m = S2.m and S1.d > S2.d)
        """)
    #
    # Insert count of entries of <wp, m> in S into R
    #
    cursor.execute("""
        insert into _R_
        select w, m, count(*) from _S_
        where w = '%s' and m = %d
        """ % (wp, m))
    #
    conn.commit()

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
        "dsn":               "hana2",
        "username":          "HPC",
        "password":          "Initial1234",
        "database":          "HPC",
        "table_name":        "DBLP",
        "column_name_id":    "ID",
        "column_name_index": "TITLE"}
    conn = db_open(env)
    #
    # Build index
    #
    start_time = time.time()
    print "Building index ...",
    build_index(conn, env)
    print "Done. " + str(time.time() - start_time)
    #
    # Test exact search
    #
    start_time = time.time()
    print ">>>> 'd'"
    exact_search(conn, env, "d")
    print ">>>> 'da'"
    exact_search(conn, env, "da")
    print ">>>> 'dat '"
    exact_search(conn, env, "dat")
    print ">>>> 'dat m'"
    exact_search(conn, env, "dat m")
    print ">>>> 'dat mi'"
    exact_search(conn, env, "dat mi")
    print "Done. " + str(time.time() - start_time)
    #
    # Test fuzzy search
    #
    '''
    fuzzy_search(conn, env, "a", 0.4)
    fuzzy_search(conn, env, "ap", 0.4)
    fuzzy_search(conn, env, "apu", 0.4)
    fuzzy_search(conn, env, "apul", 0.4)
    fuzzy_search(conn, env, "apul c", 0.4)
    fuzzy_search(conn, env, "apul cr", 0.4)
    fuzzy_search(conn, env, "apul cre", 0.4)
    '''
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

