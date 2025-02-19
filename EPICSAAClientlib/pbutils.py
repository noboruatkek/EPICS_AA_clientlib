#!python3
#-*- coding:utf-8 -*-

from . import EPICSEvent_pb2
from .EPICSEvent_pb2 import *

import google
from google.protobuf.json_format import MessageToJson, Parse, MessageToDict
import  google.protobuf as gpb

from typing import Union

import logging

# export objects
__all__=[
    'convert_pbchunk',
    'convert_pb',
    'PayloadInfoToDict'
]

# for raw i.e. http/pb
def unescape(epb):
    pb=epb.replace(b'\x1b\x03',b'\x0d').replace(b'\x1b\x02',b'\n').replace(b'\x1b\x01',b'\x1b').strip() # b'\x1b\x01' should be last.
    return pb

#def findChunkBoundaries(pbraw:Union[bytes, str])->Union[bytes,str]:
def findChunkBoundaries(pbraw):
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

def convert_pbchunk(chunkbytes):
    message_type_dict=EPICSEvent_pb2.DESCRIPTOR.message_types_by_name
    _type_dict={
        SCALAR_BYTE: ScalarByte,
        SCALAR_DOUBLE: ScalarDouble,
        SCALAR_ENUM: ScalarEnum,
        SCALAR_FLOAT: ScalarFloat,
        SCALAR_INT: ScalarInt,
        SCALAR_SHORT: ScalarShort,
        SCALAR_STRING: ScalarString,
        V4_GENERIC_BYTES: V4GenericBytes,
        WAVEFORM_BYTE: VectorChar, 
        WAVEFORM_DOUBLE:  VectorDouble,
        WAVEFORM_ENUM: VectorEnum,
        WAVEFORM_FLOAT: VectorFloat,
        WAVEFORM_INT: VectorInt,
        WAVEFORM_SHORT: VectorShort,
        WAVEFORM_STRING: VectorString
        }
    plinfo=None
    data=[]
    for l in chunkbytes.split(b'\n'):
        if not l:
            continue
        if not plinfo:
            plinfo=PayloadInfo.FromString(unescape(l))
            if plinfo.type in _type_dict:
                T=_type_dict[plinfo.type]
            else: # we should use dictionary
                logging.debug(f"{plinfo.type=},{plinfo=}")
                T=None
                data=[]
                continue
        if l == b'\n':
            break
        try:
            data.append(T.FromString(unescape(l)))
        except google.protobuf.message.DecodeError as m:
            logging.info(m, l, "@")
            # data.append(T.FromString((l)))
        except AttributeError as m:
            logging.info(m, l, "@")
    return {"info" : PayloadInfoToDict(plinfo), "data" : [MessageToDict(dobj) for dobj in data]}

def convert_pb(raw):
    chunks=[convert_pbchunk(chunkbytes) for chunkbytes in raw.split(b'\n\n')]
    return chunks

def PayloadInfoToDict(plinfo:PayloadInfo):
    d=MessageToDict(plinfo)
    logging.info(f"{[(desc.name, obj) for desc,obj in plinfo.ListFields()]}")
    # logging.debug(f"{d=},{plinfo.headers}")
    logging.debug(f"d=%s,%s", d, plinfo.headers)
    if hasattr(plinfo,"headers"):
        d["headers"]={h.name:h.val for h in plinfo.headers}
    elif "headers" in d:
        d["headers"]={ h["name"]:h["val"] for h in d["headers"]}
    logging.debug("d=%s",d)
    return d

