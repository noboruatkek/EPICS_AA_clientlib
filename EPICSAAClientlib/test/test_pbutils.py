#!python3
# -*- coding:utf-8 -*-
"""
for test pbulits.py 
"""
import time
import datetime
import pytz
from  matplotlib import pyplot

from ..pbutils import *

req={ "pv":'MRMON:DCCT_073_1:VAL:MRPWR',
      # "from" : datetime.datetime(2020,1,20,0,0,0,0,tzinfo=JST),
      # "to" :   datetime.datetime(2020,1,21,0,0,0,0,tzinfo=JST),
      "from" : datetime.datetime(2024,2,1, 0, 0, 0, 0,tzinfo=JST),
      "to" :   datetime.datetime(2024,3,9,10, 0, 0, 0,tzinfo=JST),
      "fetchLatestMetadata" : 'true',
      "donotchunk" : 'false'
     }

def pb_test():
    uri=builduri(req, "raw")
    st=time.monotonic()
    dev=request.urlopen(uri)
    raw=dev.read() # should set time out?
    print ("pb transmission  took:",time.monotonic()-st, len(raw))
    c=convert_pb(raw)
    #c=[convert_pb(chunk) for chunk in raw.split(b'\n\n')]
    print ("pb conversion done:",time.monotonic()-st, "number fo chunks:", len(c))
    # c may includes several chunks.
    T=[]
    X=[]
    
    for chunk in c:
        #print(chunk.keys(),chunk["info"]) :chunk.keys=["info","data"]
        # pvname=chunk['info'].pvname
        # dtype=chunk["info"].type
        year=chunk['info'].year
        # elementCount=chunk["info"].elementCount
        yeart0=time.mktime((year,1,1,0,0,0,0,0,0))
        data=chunk["data"]
        severities=[o.severity == 0 for o in data]
        print( "any:", any(severities), "all:", all(severities),
               severities.count(True), severities.count(False), 
               len(data))
        # time.mktime(d.utctimetuple())
        T.extend([ (yeart0+o.secondsintoyear+ o.nano*1e-9)/SECSADAY  for o in data if o.severity == 0 ])
        X.extend([  o.val for o in data if o.severity == 0 ])
    print ("pb conversion  took :",time.monotonic()-st, "raw data size:",len(raw))
    print( "data size", len(X))
    pyplot.clf()
    pyplot.plot_date(T,X)
    #pyplot.plot(T,X)
    pyplot.draw()
    pyplot.savefig( f"{req['pv']}_python_pb.png".format())
    pyplot.clf()
    print ("pb done:",time.monotonic()-st)
