#
# Hao, created: 01/27/2015, modified: 01/28/2015
#
# dblp.xml and dblp.dtd are available at: http://dblp.uni-trier.de/xml/
# A description of dblp.xml: http://goo.gl/V9ZtYI
# Usage of the class 'Element': http://goo.gl/dxcjzH
# Usage of the 'csv' module: http://goo.gl/8v0XmB
#

import csv
from lxml import etree

#

if __name__ == "__main__":
    #
    n = 0
    s_tag = set(["inproceedings", "incollection", "article"])
    v_column_name = [
            "id",
            "key", "mdate", "type",
            "title", "authors",
            "booktitle", "year", "volume",
            "number", "pages",
            "ee", "url"]
    s_column_name = set(v_column_name)
    xml_file = open("dblp.xml", "r")
    csv_file = open("dblp.csv", "w")
    csv_writer = csv.writer(csv_file, delimiter=',',
            quotechar='\"', quoting=csv.QUOTE_ALL)
    csv_writer.writerow(v_column_name)
    d_max_len = {}
    #
    # To prevent any 'XMLSyntaxError',
    # attribute 'load_dtd' must be True.
    # Ref: http://goo.gl/jzzIV9
    #
    for event, element in etree.iterparse(
            xml_file, load_dtd = True):
        level = 0
        e = element
        while e is not None:
            e = e.getparent()
            level += 1
        if level == 2 and element.tag in s_tag:
            n += 1
            p = {}
            p["id"] = str(n)
            p["type"] = element.tag
            for [tag, text] in element.items():
                if tag in s_column_name:
                    p[tag] = text
            for child in element.getchildren():
                tag = child.tag
                text = child.text
                if text is None:
                    text = ""
                text = text.encode('utf-8')
                if tag == "author":
                    if "authors" not in p:
                        p["authors"] = text
                    else:
                        p["authors"] += ("," + text)
                if tag in s_column_name:
                    p[tag] = text
            row = []
            for column_name in v_column_name:
                text = ""
                if column_name in p:
                    text = p[column_name]
                row.append(text)
                if column_name not in d_max_len or \
                        d_max_len[column_name] < len(text):
                    d_max_len[column_name] = len(text)
            csv_writer.writerow(row)
            if n % 10000 == 0:
                print n
        #
        # Add this to prevent the 'out of memory' error.
        # Ref: http://goo.gl/eKnUiY
        #
        if level <= 2:
            element.clear()
        #
        # Add this to prevent 'XMLSyntaxError: None'.
        # Ref: http://goo.gl/UdE5Pe
        #
        if level == 1:
            break
        #
    xml_file.close()
    csv_file.close()
    csv_stat_file = open("dblp.csv.stat", "w")
    for column_name in v_column_name:
        csv_stat_file.write(column_name + ": " + \
                str(d_max_len[column_name]) + "\n")
    csv_stat_file.close()
    print "Done. # of papers:", str(n)

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

