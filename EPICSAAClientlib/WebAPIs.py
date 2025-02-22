#!python3.12
#-*- coding:utf-8 -*-
# pylint: disable=C0103, W0401, R0903, C0116, R1735, R0912, W0611, W0614, R0913, W0311
"""
:Author: noboru.yamamoto@kek.jp
:date:  2025/1/22 - 
"""

import typing
from typing import List, Union, Generator, Iterator, Iterable

import os
import os.path
import asyncio
import sys
import io
import importlib
from importlib import reload
import datetime
import time
import logging
#logging.getLogger().setLevel(logging.WARN)

from urllib import request, parse
import json

#
from google.protobuf.json_format import MessageToJson, Parse, MessageToDict
import google

from . import EPICSEvent_pb2
from .EPICSEvent_pb2 import *
from .pbutils import *

__all__=[
    # bpl/
    'filterArchivedPVs', # post
    # 'getApplianceMetrics', # bpl/reports
    # 'getApplianceMetricsForAppliance' , # bpl/reports, not implemented?
    # 'getInstanceMetricsForAppliance', # bpl/reports, not implemented?
    # 'getPVDetails', #bpl/reports ,not implemented?
    'searchForPVsRegex',
    'getMatchingPVs',
    'getProcessMetrics',
    'getVersion',
    # 'getClientConfig' #not implemented?
    'getMetadata',
    'areWeArchiving',
    'getData',
    'getDataForPVs',
    'getDataAtTime',
    'getDataAtTimeForAppliance',
    #
    "ping",
    #
    'JST','UTC','datetime',
    "SIYtoDatetime", "DatetimeFromJSONarchiveddata",
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
    from enum import StrEnum, auto
    class DataFormat(StrEnum):
        """
        introduce constants for dataformat.
        """
        RAW=auto() #"raw"
        SVG="svg"
        JSON="json"
        QW="qw"
        JPLOT="jplot"
        CSV="csv"
        FLX="flx"
        TXT="txt"
        MAT="mat"

baseuri=os.environ.get("EPICS_AA_URL" ,"http://www-cont.j-parc.jp")
data_retrieval_url=os.path.join(baseuri,"retrieval")

def buildReqURI(api, query):
    """
    building request uri includes query string.
    """
    query_str=parse.urlencode(query)
    uri=f"{os.path.join(data_retrieval_url, api)}?{query_str}"
    uri=uri.replace("True","true").replace("False","false") # EAA does not accept "True" but "true".
    uri=uri.replace("None","null")
    logging.info(uri)
    return uri

def sendRequest(api, query,data=None,cont_type="application/json"):
    """
    send request to server and get response.
    """
    url=buildReqURI(api, query)
    if data:
        req=request.Request(url,
                            data=data,
                            headers={"Content-Type":cont_type, "Content_Length":len(str(data))}
                            )
        with request.urlopen(req) as resp:
            return resp.read()
    else:
        with request.urlopen(url) as resp:
            return resp.read()

#Web APIs of EAA/retrieval

def ping():
    """ chekc status of Archive Appliance server
    Paremeters
    ----------
    None
    
    Returns
    -------
    str : "pong"
    """
    api="bpl/ping"
    query={"pv":""}
    resp=sendRequest(api, query)
    return resp.decode('utf-8').strip()

def areWeArchiving(pv:str) -> bool:
    api="bpl/areWeArchivingPV"
    query={"pv":pv}
    resp=sendRequest(api, query)
    resp=json.loads(resp)
    return resp["status"]

def searchForPVsRegex(regex:str=".*") -> list[str]:
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
    logging.warning("getApplianceMetrics is not supported")

def getData(
        pv:str, from_:datetime.datetime=None, to_:datetime.datetime=None,  *,
        timeranges=None, # list of timeranges [start1,end1, start2,end2,...]
        fetchLatestMetadata:bool=True,
        donotchunk:bool=True, #  converted to  useChunkedEncoding
        usereduced=False,
        pp=None, # postProcessUserArg, chnameで "(<chnnel name>, <postProcessUser Args>)"の形式で与えることも可。
        retiredPVTemplate= None,
        fmt="json",   #:["json"|"csv"|"mat"|"raw"|"txt"|"svg"]
):
    """ Retrieve data of given PV.
    Parameters
    -----------
    pv:
      str. name PV.
    
    from_,to_:
      datetime objects for data retirieval time range.

    timeranges:
      list of datetime objects, represent multi timeranges for retrieval.

    fmt:
      data format for retrieval

    Returns
    -------

    """
    api=f"data/getData.{fmt}"
    query={
        "pv":pv,
        # pvname string 'MRMON:DCCT_073_1:VAL:MRPWR',
        # or with postprcessing eg. pv=mean(test%3Apv%3A123).
        # postprocess functions:
        #   firstSample/lastSample/firstFill/lastFill/mean/min/max/count/ncount/nth/median/std/
        #   jitter/ignoreflyers/flylyers/variance/popvariance/kurtosis/skewness/linear/
        #   loess/optimized/optimLastSample/
        #   caplotbinning/deadBand/errorbar
        #   loess_intervalSecs_binNum
        #
        # "from" : from_,    #  datetime.datetime(2020, 11,  1, 0, 0, 0, 0, tzinfo=JST),
        # "to" :   to_,      #  datetime.datetime(2024, 12, 31, 0, 0, 0, 0, tzinfo=JST),
        ### optional parameters
        #   see "https://slacmshankar.github.io/epicsarchiver_docs/userguide.html" for details
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
            query["timeranges"]=",".join(
                [t.astimezone(JST).isoformat(timespec="milliseconds") for t in timeranges]
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
    if fmt in ("raw",):
        chunks=convert_pb(resp)
        return chunks
    return resp

def getDataForPVs( # GET/POST
        pvs:str, # list of pvs
        from_:datetime.datetime=None, to_:datetime.datetime=None,   *,
        timeranges=None,
        fetchLatestMetadata:bool=True,  donotchunk:bool=True,  usereduced=False,
        retiredPVTemplate=None,   pp=None,
        fmt="json",   #:["json"|"jplot"|"qw"|"raw"] only
):
    """
    get data for pvs in arguments.
    """
    api=f"data/getDataForPVs.{fmt}"
    query={
        #"pv":pvs,           #  'MRMON:DCCT_073_1:VAL:MRPWR',
        # "from" : from_,    #  datetime.datetime(2020, 11,  1, 0, 0, 0, 0, tzinfo=JST),
        # "to" :   to_,      #  datetime.datetime(2024, 12, 31, 0, 0, 0, 0, tzinfo=JST),
        # optional parameters
        # see "https://slacmshankar.github.io/epicsarchiver_docs/userguide.html" for details
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
        query["timeranges"]=",".join(
            [t.astimezone(JST).isoformat(timespec="milliseconds") for t in timeranges]
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
    logging.info("%s, %s, %s, %s",
                 data, query, at, type(at),
                 )
    resp=sendRequest(api, query, data=data)
    return json.loads(resp)

def getDataAtTimeForAppliance(pvNames:List[str],
                              at:Union[str,datetime.datetime]="",
                              searchPeriod=""):
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

def filterArchivedPVs(pvs:List[str])->List[str]:
    """ remove unarchived channels from given list of PVs.

    parameters
    ----------
    pvs:
      list of PV names to check if it is archived.

    Returns
    --------
    list:
      list of archived PV names.

    comments
    --------
    POST only

    """
    api="bpl/filterArchivedPVs"
    query={}
    data=json.dumps(pvs).encode('utf-8')
    resp=sendRequest(api, query, data=data)
    return json.loads(resp)

# protol buffer conversion functions

def SIYtoDatetime(year:int, siy:int, nano:int, tzinfo=datetime.timezone.utc): # siy:secondsintoyear
    """ convert data in PB data into python datetime object.
    Parameters
    ----------
    year:
      year as int

    siy:
     secnods in year as int

    nano:
      nanosecond portion of time as int

    tsinfo:
      optional timezone info. default to UTC

    Returns
    -------
    datetime.datime:
      datetime object 
    """
    return (datetime.datetime(year, 1,1, 0,0, 0,0, tzinfo=tzinfo)
            + datetime.timedelta(seconds=siy, microseconds=nano/1000))

def DatetimeFromJSONarchiveddata(data, tzinfo=datetime.timezone.utc):
    """ extract datetime from archived data in json format. ie. data includes "secs" and "nanos"
    Parameters

    data:
       archived data as dict objcect, which inlude secs/nanos

    tsinfo:
      optional timezone info. default to UTC

    Returns
    -------
    datetime.datime:
      datetime object 
    """
    return datetime.datetime.fromtimestamp(data["secs"]+data["nanos"]*1e-9).astimezone(tzinfo)


def chunk_to_dict(chunks):
    data=[]
    for chunk in chunks:
        desc=chunk["info"]
        year=chunk["info"]["year"]
        data+=[e for e in chunk["data"]
              if (('val' in e)  and (e['val'] != "NaN"))]
    return {"desc":desc, "year":year,"data":data}

def DataIterator(chunks:List[dict]) -> Iterable[dict]:
    pmeta=None # previous meta data
    for chunk in chunks:
        meta=chunk["info"]
        year=chunk["info"]["year"]
        for e in chunk["data"]:
            if 'val' in e:
                date=SIYtoDatetime( year,
                                 e.pop("secondsintoyear"),
                                 e.pop("nano") if "nano" in e else 0 ,
                                 tzinfo=UTC,
                                ).astimezone(JST)
                if meta != pmeta:
                    pmeta=meta
                    e.update(meta)
                    logging.info("%s",meta)
                yield {"date":date,
                       "data":e
                       }
            else:
                continue

if __name__ == "__main__":
    #logging.getLogger().setLevel(logging.INFO)
    # test()
    #test_postprocess_all()
    pass
