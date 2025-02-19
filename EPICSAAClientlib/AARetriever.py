#!python3
#-*- coding:utf-8 -*-
import logging

#
import json
import datetime,time
import urllib.request as request ,urllib.parse as parse
#import pytz
import matplotlib.pyplot as pyplot
pyplot.clf()

from .EPICSEvent_pb2 import *
import google
#

class  Request:
    def __init__(self, action, pv, from_ , to, fmt,):
        pass

    def build_query(self):
        query_str=parse.urlencode(query)
        uri="?{}".format(format,query_str)
        pass

class AAServer:

    def __init__(self, url="<put url for your server>"):
        self.url=url
        pass

    def send_req(self, req, data):
        self.dev=request.urlopen(url)
        pass

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
        except google.protobuf.message.DecodeError as m:
            #print(m, l, "@", raw.index(l))
            loggin.warning(m, l, "@", )
            # data.append(T.FromString((l)))
    return {"info" : header, "data" : data}
