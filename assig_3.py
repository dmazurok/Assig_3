from whoosh.index import create_in
from whoosh.fields import *
import csv
from whoosh import scoring
import math
from whoosh.qparser import QueryParser

schema = Schema(title=TEXT(stored=True), path=ID(stored=True), content=TEXT)
ix = create_in("indexdir", schema)
writer = ix.writer()

def read_file(file_path, delimiter='\t'):
    with open(file_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile, delimiter=delimiter, quotechar='|', quoting=csv.QUOTE_MINIMAL)
        doc_list = []
        for row in reader:
            try:
                doc_list.append((row[0],row[1], row[2].replace('\n',' ')))
                #print((row[0],row[1], row[2].replace('\n',' ')))
            except Exception as e:
                print(e);

    return doc_list
try:
    doc_list = read_file("collection.tsv")
except Exception as e:
    print(e)
schema = Schema(id=ID(stored=True), content=TEXT)
ix = create_in("cw_index", schema)
try:
    writer = ix.writer()
except Exception as e:
    print("An exception occured: " + str(e))

for doc in doc_list:
    writer.add_document(id=doc[0],content=doc[2])
writer.commit()

# here is the tf-idf
def tf (word, doc):
    ret = 0
    leng = 0
    doc = str(doc)
    for x in doc.split():
        if word == x:
            ret = ret + 1
        leng = leng + 1
    return ret/leng

idf_known = {}  # avoid additional calculations

def idf(word, doc_list):
    if word in idf_known:
        return idf_known[word]
    d = 0
    for doc in doc_list:
        if (word in str(doc)):
            d = d + 1
    res = math.log10(doc_list.__len__()/(1+d)) # 1 + d to avoid dev by 0
    idf_known[word]=res
    return res

def tf_idf_my (searcher, fieldname, text, matcher):
    poses = matcher.id()
    text = str(text).split("'")[1]
    tf_ = tf(text, doc_list[poses])
    idf_ = idf(text, doc_list)
    return tf_*idf_

my_idf = scoring.FunctionWeighting(tf_idf_my)

with ix.searcher(weighting =my_idf) as searcher:
    query = QueryParser("content", ix.schema).parse("famous")
    results = searcher.search(query)
    print(results[0], results[0].score) # returns the best result