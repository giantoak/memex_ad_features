import requests
import datetime
import pandas
import ipdb
from pandas.io import sql
from sqlalchemy import create_engine

#### 
# MITIE ner.py
#!/usr/bin/python
#
# This example shows how to use the MITIE Python API to perform named entity
# recognition and also how to run a binary relation detector on top of the
# named entity recognition outputs.
#
import sys, os
# Make sure you put the mitielib folder into the python search path.  There are
# a lot of ways to do this, here we do it programmatically with the following
# two statements:
MITIE_root = '/home/ubuntu/mitie/'
MITIE_root = '/mnt/mount/mitie/'
parent = os.path.dirname(os.path.realpath(__file__))
sys.path.append(MITIE_root + 'mitielib')
#sys.path.append(parent + '/../../mitielib')

from mitie import *
from collections import defaultdict



class sl_extractor():
    entity_context = 5 # Number of words before and after entity to report in verbose mode
    verbose=True
    def __init__(self):
        self.extracted_count = 0
        print "loading NER model..."
        self.ner = named_entity_extractor(MITIE_root + 'MITIE-models/english/ner_model.dat')
        print "\nTags output by this NER model:", self.ner.get_possible_ner_tags()
        self.tika_url = 'http://localhost:9998/tika'
        data = '<!DOCTYPE html><html><body>Body!</body></html>'
        r = requests.put(
                self.tika_url,
                data=data
                )
        r.raise_for_status()
        # When starting hte extractor, try to run a query through the tika
        # server - if it doesn't work, we know something's up and we'll error

    def _tika_extract(self, document_data):
        """
        Take document data and run through tika to get main results
        """
        r = requests.put(
                self.tika_url,
                data=document_data
                )
        r.raise_for_status()
        return r.content

    def extract(self, document_data):
        """
        This method takes the raw document data to send to Tika, and returns locations
        """
        self.extracted_count += 1
        doc_text = self._tika_extract(document_data)
        tokens = tokenize(doc_text)
        entities = self.ner.extract_entities(tokens)
        locations = [i for i in entities if i[1] == 'LOCATION']
        print("Number of entities detected: %s" % len(entities))
        print("Number of locations detected: %s" % len(locations))
        print("Extractions performed: %s" % self.extracted_count)
        result = []
        for entity in entities:
            myrange = entity[0]
            tag = entity[1]
            entity_text = " ".join(tokens[i] for i in myrange)
            out = {'entity':entity_text, 'type':tag}
            if self.verbose:
                context_start = max(myrange[0]-self.entity_context,0)
                context_stop = min(myrange[-1] + self.entity_context, len(tokens) - 1)
                entity_context = " ".join(tokens[i] for i in xrange(context_start, context_stop))
                out['context'] = entity_context
            result.append(out)
        return result

def state_lookup(data):
    return 'NY'

def get_people(data):
    """
    Take a tuple from calling extract() and return a person specific dataframe with firstname, middlename, and last name columns
    """
    names = [i for i in data if i['type'] == 'PERSON']
    d=set()
    for i in names:
        d.add(i['entity'].lower())
    e=list(d)
    df = pandas.DataFrame({'Name':e}, index=range(len(e)))
    df['lengths'] = df.Name.apply(lambda x: len(x.split(' ')))
    df = df[df.lengths > 1]
    df = df[df.lengths < 4]
    df['firstname'] = ''
    df['middlename'] = ''
    df['lastname'] = ''
    df.firstname[df.lengths == 2], df.lastname[df.lengths ==2] = zip(*df.Name[df.lengths==2].map(lambda x: x.split(' ')))
    df.firstname[df.lengths == 3], df.middlename[df.lengths ==3], df.lastname[df.lengths ==3] = zip(*df.Name[df.lengths==3].map(lambda x: x.split(' ')))
    return df

def get_locations(data):
    """
    Take a tuple from calling extract() and return a data frame with looked up location info
    """
    locations = [i for i in data if i['type'] == 'LOCATION']
    d=set()
    for i in locations:
        d.add(i['entity'].lower())
    e=list(d)
    df = pandas.DataFrame({'locstring':e}, index=range(len(e)))
    df['lengths'] = df.locstring.map(len)
    df = df[df.lengths > 1]
    df['State'] = df[df.lengths == 2].locstring.map(state_lookup)
    # Repeat this state code for Country and City off gazeteer files
    return df

a= sl_extractor()
#b=a.extract(open('/home/ubuntu/htmls/xULdni.html','r').read())
#names = [i for i in b if i['type'] == 'PERSON']
#d=set()
#for i in names:
    #d.add(i['entity'])
#e=list(d)
engine = create_engine('mysql+pymysql://acorn:acornacornacorn@acornht.cfalas7jic14.us-east-1.rds.amazonaws.com/memex_ht', echo=True)
#xx = sql.read_sql("SHOW TABLES;", engine)
xx = sql.read_sql("select * from backpage_incoming limit 30000;", engine)
print(xx)
import json
xx['extractions'] = xx.body.map(lambda x: a.extract(x))
