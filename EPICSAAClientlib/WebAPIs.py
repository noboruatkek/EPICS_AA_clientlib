#!python3.12
#-*- coding:utf-8 -*-
"""
:Author: noboru.yamamoto@kek.jp
:date:  2025/1/22 - 
"""
import typing
from typing import List, Union, Generator, Iterator, Iterable

import urllib.request as request ,urllib.parse as parse
import os
import os.path
import asyncio
import sys
import io
import importlib
from importlib import reload

import json,datetime,time
import logging
#logging.getLogger().setLevel(logging.WARN)

__all__=[
    # bpl/
    'filterArchivedPVs',
    # 'getApplianceMetrics', # bpl/reports
    # 'getApplianceMetricsForAppliance' , # bpl/reports
    # 'getInstanceMetricsForAppliance', # bpl/reports
    # 'getPVDetails', #bpl/reports
    'searchForPVsRegex',
    'getMatchingPVs',
    'getProcessMetrics',
    'getVersion',
    # 'getClientConfig'
    'getMetadata',
    'areWeArchiving',
    #'filterArchivedPVs', # post
    ## DataRetrievalServlet:doGet
    'getData',
    ## DataRetrievalServlet:doGet/doPost
    'getDataForPVs',
    ## DataRetrievalServlet:doPost
    'getDataAtTime',
    'getDataAtTimeForAppliance',
    ## pingServelet
    "ping",
    #
    'JST','UTC','datetime','toDatetime',
    "toDatetime",
    #
    "DataIterator"
]
# setup Timezone
# import pytz
# jst=pytz.timezone("Asia/Tokyo")
# import zoneinfo
# JST=zoneinfo.ZoneInfo("Asia/Tokyo")
# UTC=zoneinfo.ZoneInfo("UTC")
JST=datetime.timezone(datetime.timedelta(hours=9))
#UTC=datetime.timezone(datetime.timedelta(hours=0))
UTC=datetime.timezone.utc
SECSADAY=24*3600

# retrieval/DataRetrievalServlet.java
_getData_formats=(
        "raw",  # "application/x-protobuf"
        "svg",  # "image/svg+xml"
        "json", # "application/json"
        "qw",   # "application/json"
        "jplot",# "application/json"
        "csv",  # "text/csv"
        "flx",  # "text/xml"
        "txt",  # "text/plain"
        "mat",  # "application/matlab"
)

if sys.version_info > (3,11):
    from enum import StrEnum,auto
    class DataFormat(StrEnum):
        RAW=auto()
        SVG="svg"
        JSON="json"
        QW="qw"
        JPLOT="jplot"
        CSV="csv"
        FLX="flx"
        TXT="txt"
        MAT="mat"
    
baseuri="http://www-cont.j-parc.jp"
data_retrieval_url=os.path.join(baseuri,"retrieval")

def buildReqURI(api, query):
    query_str=parse.urlencode(query)
    uri=f"{os.path.join(data_retrieval_url,api)}?{query_str}"
    uri=uri.replace("True","true").replace("False","false") # EAA does not accept "True" but "true".
    uri=uri.replace("None","null")
    logging.info(uri)
    return uri

def sendRequest(api, query,data=None,cont_type="application/json"):
    url=buildReqURI(api, query)
    if data:
        req=request.Request(url, data=data,headers={"Content-Type":cont_type, "Content_Length":len(str(data))})
        resp=request.urlopen(req)
    else:
        resp=request.urlopen(url)
    return resp.read()

#Web APIs of EAA/retrieval

def ping():
    api="bpl/ping"
    query={"pv":""}
    resp=sendRequest(api, query)
    return resp.decode('utf-8').strip()
    
def areWeArchiving(pv):
    api="bpl/areWeArchivingPV"
    query={"pv":pv}
    resp=sendRequest(api, query)
    resp=json.loads(resp)
    return resp["status"]

def searchForPVsRegex(regex:str=".*")->list[str]:
    api="bpl/searchForPVsRegex"
    query={"regex":regex}
    resp=sendRequest(api, query)
    return resp.decode('ascii').split()

def getMatchingPVs(pv="*", regex=".*", limit=None)->list[str]:
    """
    default limit is 500 in server side.
    """
    api="bpl/getMatchingPVs"
    query={"pv":pv, "regex":regex}
    if limit:
        query["limit"]=limit
    resp=sendRequest(api, query)
    return json.loads(resp)

def getVersion():
    api="bpl/getVersion"
    query={}
    resp=sendRequest(api, query)
    if resp:
        return json.loads(resp)
    return None

def getMetadata(pv:str):
    api="bpl/getMetadata"
    query=dict(pv=pv)
    resp=sendRequest(api, query)
    if resp:
        return json.loads(resp)
    return None
        

def getPVDetails(pv:str):
    """
    No BPLAction for getPVDetails(??)
    """
    api="bpl/getPVDetails"
    query=dict(pv=pv)
    resp=sendRequest(api, query)
    if resp:
        return json.loads(resp)
    return None

def GetClientConfig(configFile=""):
    api="bpl/getClientConfig"
    query=dict(configFile=configFile)
    resp=sendRequest(api, query)
    return resp
    
def getProcessMetrics():
    api="bpl/getProcessMetrics"
    query={}
    resp=sendRequest(api, query)
    if resp:
        return json.loads(resp)
    return None

def getApplianceMetrics():
    """
    No BPLAction class was found(???).
    getApplianceMetricsForAppliance//getInstanceMetricsForAppliance/getPVDetails
    """
    pass

def getData(
        pv:str,
        from_:datetime.datetime=None,
        to_:datetime.datetime=None,
        *,
        timeranges=None, # list of timeranges [start1,end1, start2,end2,...]
        fetchLatestMetadata:bool=True,
        donotchunk:bool=True, #  converted to  useChunkedEncoding
        usereduced=False,
        pp=None, # postProcessUserArg, chnameで "(<chnnel name>, <postProcessUser Args>)"の形式で与えることも可。
        retiredPVTemplate= None,
        fmt="json",   #:["json"|"csv"|"mat"|"raw"|"txt"|"svg"]
):
    api=f"data/getData.{fmt}"
    query={
        "pv":pv,  # pvname string 'MRMON:DCCT_073_1:VAL:MRPWR', or with postprcessing eg. pv=mean(test%3Apv%3A123).
        # postprocess functions:firstSampe/lastSample/firstFill/lastFill/mean/min/max/count/ncount/nth/median/std
        #  /jitter/ignoreflyers/flylyers/variance/popvariance/kurtosis/skewness/linear/loess/optimized/optimLastSample
        #  /caplotbinning/deadBand/errorbar
        # loess_intervalSecs_binNum
        #
        # "from" : from_,    #  datetime.datetime(2020, 11,  1, 0, 0, 0, 0, tzinfo=JST),
        # "to" :   to_,      #  datetime.datetime(2024, 12, 31, 0, 0, 0, 0, tzinfo=JST),
        ### optional parameters see "https://slacmshankar.github.io/epicsarchiver_docs/userguide.html" for details
        # "timeranges": timeranges,
        "fetchLatestMetadata": fetchLatestMetadata,
        "donotchunk": 'true' if donotchunk else 'false',
        #"usereduced": usereduced,
        # following args are meningful only for ChannalArchiver
        # ca_count=10000,
        # ca_how=0, #detault raw
    }
    if from_:
        if isinstance(from_, datetime.datetime):
            query["from"]=from_.astimezone(JST).isoformat(timespec="milliseconds")
        else:
            query["from"]=from_
    if to_:
        if isinstance(to_, datetime.datetime):
            query["to"]=to_.astimezone(JST).isoformat(timespec="milliseconds")
        else:
            query["to"]=to_
    if timeranges:
        if isinstance(timeranges,str):
            query["timeranges"]=timeranges
        else:
            query["timeranges"]="{}".format(
                ",".join([t.astimezone(JST).isoformat(timespec="milliseconds") for t in timeranges])
            )
    if usereduced:
        query["usereduced"]=usereduced
    if pp:
        query["pp"]=pp
    if retiredPVTemplate:
        query["retiredPVTemplate"]=retiredPVTemplate
        
    resp=sendRequest(api, query)
    
    if fmt in ("json","qw","jplot"):
        resp=json.loads(resp)
        return resp[0]
    elif fmt in ("raw",):
        chunks=convert_pb(resp)
        return chunks
    return resp

def getDataForPVs( # GET/POST
        pvs:str, # list of pvs
        from_:datetime.datetime=None,
        to_:datetime.datetime=None,
        *,
        timeranges=None,
        fetchLatestMetadata:bool=True,
        donotchunk:bool=True,
        usereduced=False,
        retiredPVTemplate=None,
        pp=None,
        fmt="json",   #:["json"|"jplot"|"qw"|"raw"] only
):
    api=f"data/getDataForPVs.{fmt}"
    query={
        #"pv":pvs,           #  'MRMON:DCCT_073_1:VAL:MRPWR',
        # "from" : from_,    #  datetime.datetime(2020, 11,  1, 0, 0, 0, 0, tzinfo=JST),
        # "to" :   to_,      #  datetime.datetime(2024, 12, 31, 0, 0, 0, 0, tzinfo=JST),
        #optional parameters see "https://slacmshankar.github.io/epicsarchiver_docs/userguide.html" for details
        "fetchLatestMetadata": fetchLatestMetadata,
        "donotchunk": 'true' if donotchunk else 'false',
        # "timeranges": timeranges,
        # "usereduced": usereduced,
        # "retiredPVTemplate":retiredPVTemplate,
        # "pp":pp,   # postProcessorUserArg
        # "ca_count": 100_000,
        # "ca_how": 0=raw,
    }
    data=None
    if isinstance(pvs,str):
        pvs=[s.strip() for s in pvs.split(",")]
    data=json.dumps(pvs).encode('utf-8')
    #query["pv"]=pvs
    if from_:
        query["from"]=from_.astimezone(JST).isoformat(timespec="milliseconds")
    if to_ :
        query["to"]= to_.astimezone(JST).isoformat(timespec="milliseconds")
    if timeranges:
        query["timeranges"]="{}".format(
            ",".join([t.astimezone(JST).isoformat(timespec="milliseconds") for t in timeranges])
            )
    if usereduced:
        query["usereduced"]=usereduced
    if retiredPVTemplate:
        query["retiredPVTemplate"]= retiredPVTemplate
    if pp:
        query["pp"]=pp
    resp=sendRequest(api, query, data)
    if fmt in ("json","qw","jplot"):
        resp=json.loads(resp) # return list of dict("meta","data")
    elif fmt in ("raw"):
        chunks=convert_pb(resp) #return list of chunks
        return chunks
    else:
        raise RuntimeError(f"{fmt} is not supported")
    return resp

def getDataAtTime(pvs:List[str],
                  at:Union[str, datetime.datetime]="",
                  searchPeriod="",
                  includeProxies=False):
    """
    use Post with query
    """
    api="data/getDataAtTime"
    if isinstance(pvs, str):
        pvs=[s.strip() for s in pvs.split(",")]
    data=json.dumps(pvs).encode('utf-8')
    query={}
    if at:
        if isinstance(at, datetime.datetime):
            query["at"]=at.astimezone(JST).isoformat(timespec="milliseconds")
        else:
            query["at"]=at
    if searchPeriod:
        query["searchPeriod"]=searchPeriod
    if includeProxies:
        query["includeProxies"]=includeProxies
    logging.info(f"{data}, {query}, {at}, {type(at)}")
    print(f"{data}, {query}, {at}, {type(at)}")
    resp=sendRequest(api, query, data=data)
    return json.loads(resp)

def getDataAtTimeForAppliance(pvNames:List[str], at:Union[str,datetime.datetime]="", searchPeriod=""):
    """
    POST
    searchPeriod:ISO-8601の期間フォーマットPnYnMnDおよびPnWに基づいている.
    先頭のプラス/マイナス記号と他の単位の負の値は、ISO-8601標準の一部ではありません。
    また、ISO-8601ではPnYnMnDフォーマットとPnWフォーマットの混在を許可していません。
    週ベースの入力は、7を掛けることで日数として扱われます。

    たとえば、次の入力は有効です。
    "P2Y"             -- Period.ofYears(2)
    "P3M"             -- Period.ofMonths(3)
    "P4W"             -- Period.ofWeeks(4)
    "P5D"             -- Period.ofDays(5)
    "P1Y2M3D"         -- Period.of(1, 2, 3)
    "P1Y2M3W4D"       -- Period.of(1, 2, 25)
    "P-1Y2M"          -- Period.of(-1, 2, 0)
    "-P1Y2M"          -- Period.of(-1, -2, 0)
    """
    api="data/getDataAtTimeForAppliance"
    query=dict()
    if at:
        if isinstance(at, datetime.datetime):
            query["at"]=at.astimezone(JST).isoformat(timespec="milliseconds")
        else:
            query["at"]=at
    if searchPeriod:
        query["searchPeriod"]=searchPeriod
    if isinstance(pvNames, str):
        pvNames=[s.strip() for s in pvNames.split(",")]
    data=json.dumps(pvNames).encode('utf-8')
    resp=sendRequest(api, query, data=data)
    return json.loads(resp)

def filterArchivedPVs(pvs:list[str]):
    """
    POST only 
    """
    api=f"bpl/filterArchivedPVs"
    query={}
    data=json.dumps(pvs).encode('utf-8')
    resp=sendRequest(api, query, data=data)
    return json.loads(resp)
    
# protol buffer conversion functions

from . import EPICSEvent_pb2
from .EPICSEvent_pb2 import *
from google.protobuf.json_format import MessageToJson, Parse, MessageToDict
import google

# for raw i.e. http/pb
def unescape(epb):
    pb=epb.replace(b'\x1b\x03',b'\x0d').replace(b'\x1b\x02',b'\n').replace(b'\x1b\x01',b'\x1b').strip() # b'\x1b\x01' should be last.
    return pb

def toDatetime(year, siy, nano, tzinfo=datetime.timezone.utc): # siy:secondsintoyear
    return datetime.datetime(year,1,1,0,0,0,0,tzinfo=tzinfo) + datetime.timedelta(seconds=siy, microseconds=nano/1000)

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

def chunk_to_json(chunks):
        for chunk in chunks:
            data=chunk["data"]
            desc=chunk["info"]
            year=chunk["info"]["year"]
            data=[e for e in data
                  if (('val' in e)  and (e['val'] != "NaN"))]
            plt.plot(
                [ toDatetime( year, e["secondsintoyear"], e["nano"] if "nano" in e else 0,    tzinfo=UTC  ).astimezone(JST)
                  for e in data],
                [ d["val"] for d in data],
            label= f"{pp}_{nsamples}" if pp else "raw",
            )
            av=numpy.average([ d["val"] for d in data])
            if numpy.isnan(av):
                continue
            xmin,xmax=[toDatetime( year, e["secondsintoyear"], e["nano"] if "nano" in e else 0, tzinfo=UTC).astimezone(JST)
                        for e in (data[0],data[-1])]
            plt.hlines(av, xmin, xmax, linestyle=":")
            plt.text(
                0.9, av,
                f"{av:5.3f}",
                transform=plt.get_yaxis_transform(),
            )
    
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
    logging.debug(f"{d=},{plinfo.headers}")
    if hasattr(plinfo,"headers"):
        d["headers"]=dict([(h.name,h.val) for h in plinfo.headers])
    elif "headers" in d:
        d["headers"]=dict([(h["name"],h["val"]) for h in d["headers"]])
    logging.debug(f"{d=}")
    return d

def DataIterator(chunks:List[dict]) -> Iterable[dict]:
    ometa=None
    for chunk in chunks:
        year=chunk["info"]["year"]
        meta=chunk["info"]
        for e in chunk["data"]:
            if 'val' in e:
                date=toDatetime( year,
                                 e.pop("secondsintoyear"),
                                 e.pop("nano") if "nano" in e else 0 ,
                                 tzinfo=UTC,
                                ).astimezone(JST)
                if meta != ometa:
                    ometa=meta
                    e.update(meta)
                    logging.info(f"{meta=}")
                yield {"date":date,
                       "data":e
                       }
            else:
                continue

if __name__ == "__main__":
    from test_EAA_WebAPIs import test,test_postprocess_all
    logging.getLogger().setLevel(logging.INFO)
    # test()
    test_postprocess_all()
