#!python3
#-*- coding:utf-8 -*-
from EPICSEvent_pb2 import *
import google
import  google.protobuf as gpb
from typing import Union

def findChunkBoundaries(pbraw:Union[bytes, str])->Union[bytes,str]:
    n=0
    s=[0]
    while 1:
        try:
            i=pbraw.index(b'\n',n)
            s.append((n,i+1))
            n= i+1
            continue
        except ValueError:
            break
    if s[-1][-1] != len(pbraw):
        s.append((n,len(pbraw)))
    return s   #s:[(0, 2),(2, 4), (4,62162)] ->  chunk: pbraw[0:2], pbraw[2:4],pbraw[4:62162]

def convert_pb_chunk(chunk):
    _type_dict={
        SCALAR_BYTE: ScalarByte,
        SCALAR_DOUBLE:ScalarDouble,
        SCALAR_ENUM:ScalarEnum,
        SCALAR_FLOAT:ScalarFloat,
        SCALAR_INT:ScalarInt,
        SCALAR_SHORT:ScalarShort,
        SCALAR_STRING:ScalarString,
        V4_GENERIC_BYTES:V4GenericBytes,
        WAVEFORM_BYTE:VectorChar, 
        WAVEFORM_DOUBLE: VectorDouble,
        WAVEFORM_ENUM:VectorEnum,
        WAVEFORM_FLOAT:VectorFloat,
        WAVEFORM_INT:VectorInt,
        WAVEFORM_SHORT:VectorShort,
        WAVEFORM_STRING:VectorString
        }
    header=None
    data=[]
    for l in chunk.split(b'\n'):
        if not l:
            continue
        if not header:
            header=PayloadInfo.FromString(unescape(l))
            if header.type in _type_dict:
                T=_type_dict[header.type]
            else: # we should use dictionary 
                T=None
                data=[]
                continue
        if l == b'\n':
            break
        try:
            data.append(T.FromString(unescape(l)))
        except gpb.message.DecodeError as m:
            #print(m, l, "@", raw.index(l))
            print(m, l, "@", )
            # data.append(T.FromString((l)))
    return {"info" : header, "data" : data}

def convert_pb(raw:Union[bytes,bytearray]):
    c=[convert_pb_chunk(chunk) for chunk in raw.split(b'\n\n')]
    return c

# for test,
import datetime
import pytz

req={"pv":'MRMON:DCCT_073_1:VAL:MRPWR',
     # "from" : datetime.datetime(2020,1,20,0,0,0,0,tzinfo=JST),
     # "to" :   datetime.datetime(2020,1,21,0,0,0,0,tzinfo=JST),
     "from" : datetime.datetime(2024,2,1, 0, 0, 0, 0,tzinfo=JST),
     "to" :   datetime.datetime(2024,3,9,10, 0, 0, 0,tzinfo=JST),
     "fetchLatestMetadata" : 'true',
     "donotchunk" : 'false'}

import time

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
        pvname=chunk['info'].pvname
        dtype=chunk["info"].type
        year=chunk['info'].year
        elementCount=chunk["info"].elementCount
        yeart0=time.mktime((year,1,1,0,0,0,0,0,0))
        data=chunk["data"]
        severities=[o.severity == 0 for o in data]
        print( "any:", any(severities), "all:", all(severities),
               severities.count(True), severities.count(False), 
               len(data))
        # time.mktime(d.utctimetuple())
        T.extend([ (yeart0+o.secondsintoyear+ o.nano*1e-9)/SECSADAY  for o in data if (o.severity == 0)])
        X.extend([  o.val for o in data if (o.severity == 0)])
    print ("pb conversion  took :",time.monotonic()-st, "raw data size:",len(raw))
    print( "data size", len(X))
    pyplot.clf()
    pyplot.plot_date(T,X)
    #pyplot.plot(T,X)
    pyplot.draw()
    pyplot.savefig("{}_python_pb.png".format(req["pv"]))
    pyplot.clf()
    print ("pb done:",time.monotonic()-st)
