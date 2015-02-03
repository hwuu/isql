#
# Hao, created: 01/29/2015, modified: 02/03/2015
#

import re
import time
import pyodbc

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
    #
    conn = pyodbc.connect("DSN=%s;UID=%s;PWD=%s" \
           % (env["dsn"], env["username"], env["password"]),
              autocommit=False)
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

